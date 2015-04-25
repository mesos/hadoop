package org.apache.hadoop.mapred;

import com.codahale.metrics.Meter;
import org.apache.commons.httpclient.HttpHost;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Pool;
import org.apache.hadoop.mapred.PoolManager;
import org.apache.hadoop.mapred.FairScheduler;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.*;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.hadoop.Metrics;

import java.lang.reflect.Field;
import java.lang.ReflectiveOperationException;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MesosScheduler extends TaskScheduler implements Scheduler {
  public static final Log LOG = LogFactory.getLog(MesosScheduler.class);

  // This is the memory overhead for a jvm process. This needs to be added
  // to a jvm process's resource requirement, in addition to its heap size.
  public static final double JVM_MEM_OVERHEAD_PERCENT_DEFAULT = 0.25; // 25%.

  // NOTE: It appears that there's no real resource requirements for a
  // map / reduce slot. We therefore define a default slot as:
  // 1 cores.
  // 1024 MB memory.
  // 1 GB of disk space.
  public static final double SLOT_CPUS_DEFAULT = 1; // 1 cores.
  public static final int SLOT_DISK_DEFAULT = 1024; // 1 GB.
  public static final int SLOT_JVM_HEAP_DEFAULT = 1024; // 1024MB.
  public static final double TASKTRACKER_CPUS_DEFAULT = 1.0; // 1 core.
  public static final int TASKTRACKER_MEM_DEFAULT = 1024; // 1 GB.
  public static final int TASKTRACKER_DISK_DEFAULT = 1024; // 1 GB.
  // The default behavior in Hadoop is to use 4 slots per TaskTracker:
  public static final int MAP_SLOTS_DEFAULT = 2;
  public static final int REDUCE_SLOTS_DEFAULT = 2;
  // The amount of time to wait for task trackers to launch before
  // giving up.
  public static final long LAUNCH_TIMEOUT_MS = 300000; // 5 minutes
  public static final long PERIODIC_MS = 300000; // 5 minutes
  public static final long DEFAULT_IDLE_CHECK_INTERVAL = 5; // 5 seconds
  // Destroy task trackers after being idle for N idle checks
  public static final long DEFAULT_IDLE_REVOCATION_CHECKS = 5;
  private SchedulerDriver driver;

  protected TaskScheduler taskScheduler;
  protected JobTracker jobTracker;
  protected Configuration conf;
  protected File stateFile;
  // Count of the launched trackers for TaskID generation.
  protected long launchedTrackers = 0;
  // Use a fixed slot allocation policy?
  protected boolean policyIsFixed = false;
  protected ResourcePolicy policy;

  protected boolean enableMetrics = false;
  public Metrics metrics;

  // Maintains a mapping from {tracker host:port -> MesosTracker}.
  // Used for tracking the slots of each TaskTracker and the corresponding
  // Mesos TaskID.
  protected Map<HttpHost, MesosTracker> mesosTrackers =
    new ConcurrentHashMap<HttpHost, MesosTracker>();

  protected final ScheduledExecutorService timerScheduler =
       Executors.newScheduledThreadPool(1);

  protected JobInProgressListener jobListener = new JobInProgressListener() {
    @Override
    public void jobAdded(JobInProgress job) throws IOException {
      LOG.info("Added job " + job.getJobID());
      if (metrics != null) {
        metrics.jobTimerContexts.put(job.getJobID(), metrics.jobTimer.time());
      }
    }

    @Override
    public void jobRemoved(JobInProgress job) {
      LOG.info("Removed job " + job.getJobID());
    }

    @Override
    public void jobUpdated(JobChangeEvent event) {
      synchronized (MesosScheduler.this) {
        JobInProgress job = event.getJobInProgress();

        if (metrics != null) {
          Meter meter = metrics.jobStateMeter.get(job.getStatus().getRunState());
          if (meter != null) {
            meter.mark();
          }
        }

        // If we have flaky tasktrackers, kill them.
        final List<String> flakyTrackers = job.getBlackListedTrackers();
        // Remove the task from the map.  This is O(n^2), but there's no better
        // way to do it, AFAIK.  The flakyTrackers list should usually be
        // small, so this is probably not bad.
        for (String hostname : flakyTrackers) {
          for (MesosTracker mesosTracker : mesosTrackers.values()) {
            if (mesosTracker.host.getHostName().startsWith(hostname)) {
              LOG.info("Killing Mesos task: " + mesosTracker.taskId + " on host "
                  + mesosTracker.host + " because it has been marked as flaky");
              if (metrics != null) {
                metrics.flakyTrackerKilledMeter.mark();
              }
              killTracker(mesosTracker);
            }
          }
        }

        // If the job is complete, kill all the corresponding idle TaskTrackers.
        if (!job.isComplete()) {
          return;
        }

        if (metrics != null) {
          com.codahale.metrics.Timer.Context context = metrics.jobTimerContexts.get(job.getJobID());
          context.stop();
          metrics.jobTimerContexts.remove(job.getJobID());
        }

        LOG.info("Completed job : " + job.getJobID());

        // Remove the task from the map.
        final Set<HttpHost> trackers = new HashSet<HttpHost>(mesosTrackers.keySet());
        for (HttpHost tracker : trackers) {
          MesosTracker mesosTracker = mesosTrackers.get(tracker);
          mesosTracker.jobs.remove(job.getJobID());

          // If the TaskTracker doesn't have any running job tasks assigned,
          // kill it.
          if (mesosTracker.jobs.isEmpty() && mesosTracker.active) {
            LOG.info("Killing Mesos task: " + mesosTracker.taskId + " on host "
                + mesosTracker.host + " because it is no longer needed");

            killTracker(mesosTracker);
          }
        }
      }
    }
  };

  // TaskScheduler methods.
  @Override
  public synchronized void start() throws IOException {
    conf = getConf();
    String taskTrackerClass = conf.get("mapred.mesos.taskScheduler",
        "org.apache.hadoop.mapred.JobQueueTaskScheduler");

    try {
      taskScheduler =
        (TaskScheduler) Class.forName(taskTrackerClass).newInstance();
      taskScheduler.setConf(conf);
      taskScheduler.setTaskTrackerManager(taskTrackerManager);
    } catch (ClassNotFoundException e) {
      LOG.fatal("Failed to initialize the TaskScheduler", e);
      System.exit(1);
    } catch (InstantiationException e) {
      LOG.fatal("Failed to initialize the TaskScheduler", e);
      System.exit(1);
    } catch (IllegalAccessException e) {
      LOG.fatal("Failed to initialize the TaskScheduler", e);
      System.exit(1);
    }

    // Add the job listener to get job related updates.
    taskTrackerManager.addJobInProgressListener(jobListener);

    LOG.info("Starting MesosScheduler");
    jobTracker = (JobTracker) super.taskTrackerManager;

    String master = conf.get("mapred.mesos.master", "local");

    try {
      FrameworkInfo frameworkInfo = FrameworkInfo
        .newBuilder()
        .setUser("") // Let Mesos fill in the user.
        .setCheckpoint(conf.getBoolean("mapred.mesos.checkpoint", false))
        .setRole(conf.get("mapred.mesos.role", "*"))
        .setName("Hadoop: (RPC port: " + jobTracker.port + ","
                 + " WebUI port: " + jobTracker.infoPort + ")").build();

      driver = new MesosSchedulerDriver(this, frameworkInfo, master);
      driver.start();
    } catch (Exception e) {
      // If the MesosScheduler can't be loaded, the JobTracker won't be useful
      // at all, so crash it now so that the user notices.
      LOG.fatal("Failed to start MesosScheduler", e);
      System.exit(1);
    }

    String file = conf.get("mapred.mesos.state.file", "");
    if (!file.equals("")) {
      this.stateFile = new File(file);
    }

    policyIsFixed = conf.getBoolean("mapred.mesos.scheduler.policy.fixed",
        policyIsFixed);

    if (policyIsFixed) {
      policy = new ResourcePolicyFixed(this);
    } else {
      policy = new ResourcePolicyVariable(this);
    }

    enableMetrics = conf.getBoolean("mapred.mesos.metrics.enabled",
        enableMetrics);

    if (enableMetrics) {
      metrics = new Metrics(conf);
    }

    taskScheduler.start();
  }

  @Override
  public synchronized void terminate() throws IOException {
    try {
      LOG.info("Stopping MesosScheduler");
      driver.stop();
    } catch (Exception e) {
      LOG.error("Failed to stop Mesos scheduler", e);
    }

    taskScheduler.terminate();
  }

  @Override
  public void checkJobSubmission(JobInProgress job) throws IOException {
    taskScheduler.checkJobSubmission(job);
  }

  @Override
  public List<Task> assignTasks(TaskTracker taskTracker)
      throws IOException {
    HttpHost tracker = new HttpHost(taskTracker.getStatus().getHost(),
        taskTracker.getStatus().getHttpPort());

    if (!mesosTrackers.containsKey(tracker)) {
      LOG.info("Unknown/exited TaskTracker: " + tracker + ". ");
      return null;
    }

    MesosTracker mesosTracker = mesosTrackers.get(tracker);

    // Make sure we're not asked to assign tasks to any task trackers that have
    // been stopped. This could happen while the task tracker has not been
    // removed from the cluster e.g still in the heartbeat timeout period.
    synchronized (this) {
      if (mesosTracker.stopped) {
        LOG.info("Asked to assign tasks to stopped tracker " + tracker + ".");
        return null;
      }
    }

    // Let the underlying task scheduler do the actual task scheduling.
    List<Task> tasks = taskScheduler.assignTasks(taskTracker);

    // The Hadoop Fair Scheduler is known to return null.
    if (tasks == null) {
      return null;
    }

    // Keep track of which TaskTracker contains which tasks.
    for (Task task : tasks) {
      mesosTracker.jobs.add(task.getJobID());
    }

    return tasks;
  }

  @Override
  public synchronized Collection<JobInProgress> getJobs(String queueName) {
    return taskScheduler.getJobs(queueName);
  }

  @Override
  public synchronized void refresh() throws IOException {
    taskScheduler.refresh();
  }

  // Mesos Scheduler methods.
  // These are synchronized, where possible. Some of these methods need to access the
  // JobTracker, which can lead to a deadlock:
  // See: https://issues.apache.org/jira/browse/MESOS-429
  // The workaround employed here is to unsynchronize those methods needing access to
  // the JobTracker state and use explicit synchronization instead as appropriate.
  // TODO(bmahler): Provide a cleaner solution to this issue. One solution is to
  // split up the Scheduler and TaskScheduler implementations in order to break the
  // locking cycle. This would require a synchronized class to store the shared
  // state across our Scheduler and TaskScheduler implementations, and provide
  // atomic operations as needed.
  @Override
  public synchronized void registered(SchedulerDriver schedulerDriver,
                                      FrameworkID frameworkID, MasterInfo masterInfo) {
    LOG.info("Registered as " + frameworkID.getValue()
        + " with master " + masterInfo);
  }

  @Override
  public synchronized void reregistered(SchedulerDriver schedulerDriver,
                                        MasterInfo masterInfo) {
    LOG.info("Re-registered with master " + masterInfo);
  }

  public void killTracker(MesosTracker tracker) {
    if (metrics != null) {
      metrics.killMeter.mark();
    }
    synchronized (this) {
      driver.killTask(tracker.taskId);
    }
    tracker.stop();
    if (mesosTrackers.get(tracker.host) == tracker) {
      mesosTrackers.remove(tracker.host);
    }
  }

  public synchronized void scheduleTimer(Runnable command,
                                         long delay,
                                         TimeUnit unit) {
    timerScheduler.schedule(command, delay, unit);
  }

  public int getPendingTasks(TaskInProgress[] tasks, TaskType taskType) {

    // Pull out the pool manager from the FairScheduler, if we are configured
    // to use the fair scheduler. Not idea to do this using reflection but as
    // far as I can tell, there's no easier way.
    PoolManager poolMgr = null;
    if (taskScheduler instanceof FairScheduler) {
        Field field = FairScheduler.getDeclaredField("poolMgr");
        field.setAccessible(true);

        poolMgr = (PoolMgr) field.get(taskSchdeduler);
    }

    Map<String, MutablePair<Pool, Integer>> pools =
            new HashMap<String, MutablePair<Pool, Integer>>();

    for (int i = 0; i < totalTasks; ++i) {
      TaskInProgress task = tasks[i];
      if (task == null) {
        continue;
      }

      String poolName;
      if (poolMgr != null) {
          Pool pool = poolMgr.getPool(task.getJob());
          if (pool != null) {
              poolName = pool.getName();
          }
      }

      if (poolName == null) {
          poolName = Pool.DEFAULT_POOL_NAME;
      }

      MutablePair<Pool, Integer> pair = pools.get(poolName);
      Integer pendingTasks = pair.getRight();
      if (pendingTasks == null) {
          pendingTasks = 0;
      }

      pendingTasks++;

      if (task.isComplete() || task.isRunning() || task.wasKilled()) {
        pendingTasks -= 1;
      }

      pair.setRight(pendingTasks);
      pools.put(poolName, pair);
    }

    // Calculate the total number of pending tasks, however, capping each pool
    // at its' configured maximum.
    int totalPendingTasks = 0;
    for (Pair<Pool, Integer> pair : pools.values()) {
        if ((taskType == TaskType.MAP || taskType == TaskType.REDUCE) && pair.getLeft()) {
            int maxTasks = poolMgr.getMaxSlots(pair.getLeft().getName(), taskType);
            totalPendingTasks += min(pair.getRight(), maxTasks);
        } else {
            totalPendingTasks += pair.getRight();
        }
    }

    return totalPendingTasks;
  }

  // This method uses explicit synchronization in order to avoid deadlocks when
  // accessing the JobTracker.
  @Override
  public void resourceOffers(SchedulerDriver schedulerDriver,
                             List<Offer> offers) {
    policy.resourceOffers(schedulerDriver, offers);
  }

  @Override
  public synchronized void offerRescinded(SchedulerDriver schedulerDriver,
                                          OfferID offerID) {
    LOG.warn("Rescinded offer: " + offerID.getValue());
  }

  @Override
  public synchronized void statusUpdate(SchedulerDriver schedulerDriver,
                                        Protos.TaskStatus taskStatus) {
    LOG.info("Status update of " + taskStatus.getTaskId().getValue()
        + " to " + taskStatus.getState().name()
        + " with message " + taskStatus.getMessage());

    // Remove the TaskTracker if the corresponding Mesos task has reached a
    // terminal state.
    switch (taskStatus.getState()) {
      case TASK_FINISHED:
      case TASK_FAILED:
      case TASK_KILLED:
      case TASK_LOST:
        // Make a copy to iterate over keys and delete values.
        Set<HttpHost> trackers = new HashSet<HttpHost>(mesosTrackers.keySet());

        // Remove the task from the map.
        for (HttpHost tracker : trackers) {
          if (mesosTrackers.get(tracker).taskId.equals(taskStatus.getTaskId())) {
            LOG.info("Removing terminated TaskTracker: " + tracker);
            mesosTrackers.get(tracker).stop();
            mesosTrackers.remove(tracker);
          }
        }
        break;
      case TASK_STAGING:
      case TASK_STARTING:
      case TASK_RUNNING:
        break;
      default:
        LOG.error("Unexpected TaskStatus: " + taskStatus.getState().name());
        break;
    }

    if (metrics != null) {
      Meter meter = metrics.taskStateMeter.get(taskStatus.getState());
      if (meter != null) {
        meter.mark();
      }
    }
  }

  @Override
  public synchronized void frameworkMessage(SchedulerDriver schedulerDriver,
                                            ExecutorID executorID, SlaveID slaveID, byte[] bytes) {
    LOG.info("Framework Message of " + bytes.length + " bytes"
        + " from executor " + executorID.getValue()
        + " on slave " + slaveID.getValue());
  }

  @Override
  public synchronized void disconnected(SchedulerDriver schedulerDriver) {
    LOG.warn("Disconnected from Mesos master.");
  }

  @Override
  public synchronized void slaveLost(SchedulerDriver schedulerDriver,
                                     SlaveID slaveID) {
    LOG.warn("Slave lost: " + slaveID.getValue());
  }

  @Override
  public synchronized void executorLost(SchedulerDriver schedulerDriver,
                                        ExecutorID executorID, SlaveID slaveID, int status) {
    LOG.warn("Executor " + executorID.getValue()
        + " lost with status " + status + " on slave " + slaveID);
  }

  @Override
  public synchronized void error(SchedulerDriver schedulerDriver, String s) {
    LOG.error("Error from scheduler driver: " + s);
  }
}
