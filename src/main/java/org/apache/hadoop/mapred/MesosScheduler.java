package org.apache.hadoop.mapred;

import com.codahale.metrics.Meter;
import com.google.protobuf.ByteString;
import org.apache.commons.httpclient.HttpHost;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.*;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.hadoop.Metrics;

import java.io.File;
import java.io.FileInputStream;
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

  // The amount of time to wait for task trackers to launch before giving up.
  public static final long LAUNCH_TIMEOUT_MS = 300000; // 5 minutes
  public static final long PERIODIC_MS = 300000; // 5 minutes
  public static final long DEFAULT_IDLE_CHECK_INTERVAL = 5; // 5 seconds

  // Destroy task trackers after being idle for N idle checks
  public static final long DEFAULT_IDLE_REVOCATION_CHECKS = 5;
  public Metrics metrics;

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

  // Maintains a mapping from {tracker host:port -> MesosTracker}.
  // Used for tracking the slots of each TaskTracker and the corresponding
  // Mesos TaskID.
  protected Map<HttpHost, MesosTracker> mesosTrackers = new ConcurrentHashMap<>();

  protected final ScheduledExecutorService timerScheduler = Executors.newScheduledThreadPool(1);

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
              LOG.info("Killing tracker on host " + mesosTracker.host + " because it has been marked as flaky");
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
        final Set<HttpHost> trackers = new HashSet<>(mesosTrackers.keySet());
        for (HttpHost tracker : trackers) {
          MesosTracker mesosTracker = mesosTrackers.get(tracker);
          mesosTracker.jobs.remove(job.getJobID());

          // If the TaskTracker doesn't have any running job tasks assigned, kill it.
          if (mesosTracker.jobs.isEmpty() && mesosTracker.active) {
            LOG.info("Killing tracker on host " + mesosTracker.host + " because it is no longer needed");

            killTracker(mesosTracker);
          }
        }
      }
    }
  };
  private SchedulerDriver driver;

  // --------------------- Interface Scheduler ---------------------

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
  public synchronized void registered(SchedulerDriver schedulerDriver, FrameworkID frameworkID, MasterInfo masterInfo) {
    LOG.info("Registered as " + frameworkID.getValue() + " with master " + masterInfo);
  }

  @Override
  public synchronized void reregistered(SchedulerDriver schedulerDriver, MasterInfo masterInfo) {
    LOG.info("Re-registered with master " + masterInfo);
  }

  // This method uses explicit synchronization in order to avoid deadlocks when
  // accessing the JobTracker.
  @Override
  public void resourceOffers(SchedulerDriver schedulerDriver, List<Offer> offers) {
    policy.resourceOffers(schedulerDriver, offers);
  }

  @Override
  public synchronized void offerRescinded(SchedulerDriver schedulerDriver, OfferID offerID) {
    LOG.warn("Rescinded offer: " + offerID.getValue());
  }

  @Override
  public synchronized void statusUpdate(SchedulerDriver schedulerDriver, Protos.TaskStatus taskStatus) {
    LOG.info("Status update of " + taskStatus.getTaskId().getValue() + " to " + taskStatus.getState().name() + " with message " + taskStatus.getMessage());

    // Remove the TaskTracker if the corresponding Mesos task has reached a
    // terminal state.
    switch (taskStatus.getState()) {
      case TASK_FINISHED:
      case TASK_FAILED:
      case TASK_KILLED:
      case TASK_LOST:
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
  public synchronized void frameworkMessage(SchedulerDriver schedulerDriver, ExecutorID executorID, SlaveID slaveID, byte[] bytes) {
    LOG.info("Framework Message of " + bytes.length + " bytes" + " from executor " + executorID.getValue() + " on slave " + slaveID.getValue());
  }

  @Override
  public synchronized void disconnected(SchedulerDriver schedulerDriver) {
    LOG.warn("Disconnected from Mesos master.");
  }

  @Override
  public synchronized void slaveLost(SchedulerDriver schedulerDriver, SlaveID slaveID) {
    LOG.warn("Slave lost: " + slaveID.getValue());
  }

  @Override
  public synchronized void executorLost(SchedulerDriver schedulerDriver, ExecutorID executorID, SlaveID slaveID, int status) {
    LOG.warn("Executor " + executorID.getValue() + " lost with status " + status + " on slave " + slaveID);

    // TODO(tarnfeld): If the executor is lost what do we do?
  }

  @Override
  public synchronized void error(SchedulerDriver schedulerDriver, String s) {
    LOG.error("Error from scheduler driver: " + s);
  }

  @Override
  public List<Task> assignTasks(TaskTracker taskTracker) throws IOException {

    // Let the underlying task scheduler do the actual task scheduling.
    List<Task> tasks = taskScheduler.assignTasks(taskTracker);

    // The Hadoop Fair Scheduler is known to return null.
    if (tasks != null) {
      HttpHost tracker = new HttpHost(taskTracker.getStatus().getHost(), taskTracker.getStatus().getHttpPort());
      MesosTracker mesosTracker = mesosTrackers.get(tracker);
      if (mesosTracker != null) {
        synchronized (this) {
          for (Iterator<Task> iterator = tasks.iterator(); iterator.hasNext();) {

            // Throw away any task types that don't match up with the current known
            // slot allocation of the tracker. We do this here because when we change
            // the slot allocation of a running Task Tracker it can take time for
            // this information to propagate around the system and we can preemptively
            // avoid scheduling tasks to task trackers we know not to have capacity.
            Task task = iterator.next();
            if (task instanceof MapTask && mesosTracker.mapSlots == 0) {
              LOG.debug("Removed map task from TT assignment due to mismatching slots");
              iterator.remove();
              continue;
            } else if (task instanceof ReduceTask && mesosTracker.reduceSlots == 0) {
              LOG.debug("Removed reduce task from TT assignment due to mismatching slots");
              iterator.remove();
              continue;
            }

            // Keep track of which TaskTracker contains which tasks.
            mesosTracker.jobs.add(task.getJobID());
          }
        }
      }
    }

    return tasks;
  }

  @Override
  public void checkJobSubmission(JobInProgress job) throws IOException {
    taskScheduler.checkJobSubmission(job);
  }

  @Override
  public synchronized Collection<JobInProgress> getJobs(String queueName) {
    return taskScheduler.getJobs(queueName);
  }

  /**
   * For some reason, pendingMaps() and pendingReduces() doesn't return the values we expect. We observed negative
   * values, which may be related to https://issues.apache.org/jira/browse/MAPREDUCE-1238. Below is the algorithm
   * that is used to calculate the pending tasks within the Hadoop JobTracker sources (see 'printTaskSummary' in
   * src/org/apache/hadoop/mapred/jobdetails_jsp.java).
   *
   * @param tasks
   * @return
   */
  public int getPendingTasks(TaskInProgress[] tasks) {
    int totalTasks = tasks.length;
    int runningTasks = 0;
    int finishedTasks = 0;
    int killedTasks = 0;
    for (TaskInProgress task : tasks) {
      if (task == null) {
        continue;
      }
      if (task.isComplete()) {
        finishedTasks += 1;
      } else if (task.isRunning()) {
        runningTasks += 1;
      } else if (task.wasKilled()) {
        killedTasks += 1;
      }
    }
    return totalTasks - runningTasks - killedTasks - finishedTasks;
  }

  public void killTracker(MesosTracker tracker) {
    killTrackerSlots(tracker, TaskType.MAP);
    killTrackerSlots(tracker, TaskType.REDUCE);
  }

  // killTrackerSlots will ask the given MesosTraker to revoke
  // the allocated task slots, for the given type of slot (MAP/REDUCE).
  public void killTrackerSlots(MesosTracker tracker, TaskType type) {
    if (metrics != null) {
      metrics.killMeter.mark();
    }

    TaskID taskId = tracker.getTaskId(type);
    if (taskId != null) {
      driver.killTask(taskId);

      if (type == TaskType.MAP) {
        tracker.mapSlots = 0;
      } else if (type == TaskType.REDUCE) {
        tracker.reduceSlots = 0;
      }
    }
  }

  @Override
  public synchronized void refresh() throws IOException {
    taskScheduler.refresh();
  }

  public synchronized void scheduleTimer(Runnable command, long delay, TimeUnit unit) {
    timerScheduler.schedule(command, delay, unit);
  }

  // TaskScheduler methods.
  @Override
  public synchronized void start() throws IOException {
    conf = getConf();
    String taskTrackerClass = conf.get("mapred.mesos.taskScheduler", "org.apache.hadoop.mapred.JobQueueTaskScheduler");

    try {
      taskScheduler = (TaskScheduler) Class.forName(taskTrackerClass).newInstance();
      taskScheduler.setConf(conf);
      taskScheduler.setTaskTrackerManager(taskTrackerManager);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOG.fatal("Failed to initialize the TaskScheduler", e);
      System.exit(1);
    }

    // Add the job listener to get job related updates.
    taskTrackerManager.addJobInProgressListener(jobListener);

    LOG.info("Starting MesosScheduler");
    jobTracker = (JobTracker) super.taskTrackerManager;

    String master = conf.get("mapred.mesos.master", "local");

    try {
      FrameworkInfo frameworkInfo;
      FrameworkInfo.Builder frameworkInfoBuilder = FrameworkInfo.newBuilder()
              .setUser(conf.get("mapred.mesos.framework.user", "")) // Let Mesos fill in the user.
              .setCheckpoint(conf.getBoolean("mapred.mesos.checkpoint", false))
              .setRole(conf.get("mapred.mesos.role", "*"))
              .setName(conf.get("mapred.mesos.framework.name", "Hadoop: (RPC port: " + jobTracker.port + ","
                      + " WebUI port: " + jobTracker.infoPort + ")"));

      Credential credential = null;

      String frameworkPrincipal = conf.get("mapred.mesos.framework.principal");
      if (frameworkPrincipal != null) {
        frameworkInfoBuilder.setPrincipal(frameworkPrincipal);
        String secretFile = conf.get("mapred.mesos.framework.secretfile");
        if (secretFile != null) {
          credential = Credential.newBuilder()
                  .setSecret(ByteString.readFrom(new FileInputStream(secretFile)))
                  .setPrincipal(frameworkPrincipal)
                  .build();
        }
      }
      if (credential == null) {
        LOG.info("Creating Schedule Driver");
        driver = new MesosSchedulerDriver(this, frameworkInfoBuilder.build(), master);
      } else {
        LOG.info("Creatingg Schedule Driver, attempting to authenticate with Principal: " + credential.getPrincipal()
                + ", secret:" + credential.getSecret());
        driver = new MesosSchedulerDriver(this, frameworkInfoBuilder.build(), master, credential);
      }
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

    policyIsFixed = conf.getBoolean("mapred.mesos.scheduler.policy.fixed", policyIsFixed);

    if (policyIsFixed) {
      policy = new ResourcePolicyFixed(this);
    } else {
      policy = new ResourcePolicyVariable(this);
    }

    enableMetrics = conf.getBoolean("mapred.mesos.metrics.enabled", enableMetrics);

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
}
