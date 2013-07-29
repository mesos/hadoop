package org.apache.hadoop.mapred;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.httpclient.HttpHost;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.*;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.hadoop.*;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.apache.hadoop.util.StringUtils.join;

public class MesosScheduler extends TaskScheduler implements Scheduler {
  public static final Log LOG = LogFactory.getLog(MesosScheduler.class);
  // This is the memory overhead for a jvm process. This needs to be added
  // to a jvm process's resource requirement, in addition to its heap size.
  private static final double JVM_MEM_OVERHEAD_PERCENT_DEFAULT = 0.1; // 10%.
  // NOTE: It appears that there's no real resource requirements for a
  // map / reduce slot. We therefore define a default slot as:
  // 1 cores.
  // 1024 MB memory.
  // 1 GB of disk space.
  private static final double SLOT_CPUS_DEFAULT = 1; // 1 cores.
  private static final int SLOT_DISK_DEFAULT = 1024; // 1 GB.
  private static final int SLOT_JVM_HEAP_DEFAULT = 1024; // 1024MB.
  private static final double TASKTRACKER_CPUS = 1.0; // 1 core.
  private static final int TASKTRACKER_MEM_DEFAULT = 1024; // 1 GB.
  // The default behavior in Hadoop is to use 4 slots per TaskTracker:
  private static final int MAP_SLOTS_DEFAULT = 2;
  private static final int REDUCE_SLOTS_DEFAULT = 2;
  // The amount of time to wait for task trackers to launch before
  // giving up.
  private static final long LAUNCH_TIMEOUT_MS = 300000; // 5 minutes
  private SchedulerDriver driver;
  private TaskScheduler taskScheduler;
  private JobTracker jobTracker;
  private Configuration conf;
  private File stateFile;
  // Count of the launched trackers for TaskID generation.
  private long launchedTrackers = 0;
  // Use a fixed slot allocation policy?
  private boolean policyIsFixed = false;
  private ResourcePolicy policy;
  // Maintains a mapping from {tracker host:port -> MesosTracker}.
  // Used for tracking the slots of each TaskTracker and the corresponding
  // Mesos TaskID.
  private Map<HttpHost, MesosTracker> mesosTrackers =
      new HashMap<HttpHost, MesosTracker>();
  private JobInProgressListener jobListener = new JobInProgressListener() {
    @Override
    public void jobAdded(JobInProgress job) throws IOException {
      LOG.info("Added job " + job.getJobID());
    }

    @Override
    public void jobRemoved(JobInProgress job) {
      LOG.info("Removed job " + job.getJobID());
    }

    @Override
    public void jobUpdated(JobChangeEvent event) {
      synchronized (MesosScheduler.this) {
        JobInProgress job = event.getJobInProgress();

        // If the job is complete, kill all the corresponding idle TaskTrackers.
        if (!job.isComplete()) {
          return;
        }

        LOG.info("Completed job : " + job.getJobID());

        Set<HttpHost> trackers = new HashSet<HttpHost>(mesosTrackers.keySet());

        // Remove the task from the map.
        for (HttpHost tracker : trackers) {
          MesosTracker mesosTracker = mesosTrackers.get(tracker);
          mesosTracker.jobs.remove(job.getJobID());

          // If the TaskTracker doesn't have any running tasks, kill it.
          if (mesosTracker.jobs.isEmpty() && mesosTracker.active) {
            LOG.info("Killing Mesos task: " + mesosTracker.taskId + " on host "
                + mesosTracker.host + " because it is no longer needed");

            killTracker(mesosTracker);
          }
        }
      }
    }
  };

  public MesosScheduler() {
  }

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
          .setUser("")
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
  public synchronized List<Task> assignTasks(TaskTracker taskTracker)
      throws IOException {
    HttpHost tracker = new HttpHost(taskTracker.getStatus().getHost(),
        taskTracker.getStatus().getHttpPort());

    if (!mesosTrackers.containsKey(tracker)) {
      // TODO(bmahler): Consider allowing non-Mesos TaskTrackers.
      LOG.info("Unknown/exited TaskTracker: " + tracker + ". ");
      return null;
    }

    // Let the underlying task scheduler do the actual task scheduling.
    List<Task> tasks = taskScheduler.assignTasks(taskTracker);

    // The Hadoop Fair Scheduler is known to return null.
    if (tasks != null) {
      // Keep track of which TaskTracker contains which tasks.
      for (Task task : tasks) {
        mesosTrackers.get(tracker).jobs.add(task.getJobID());
      }
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

  public synchronized void killTracker(MesosTracker tracker) {
    driver.killTask(tracker.taskId);
    mesosTrackers.remove(tracker.host);
  }

  // For some reason, pendingMaps() and pendingReduces() doesn't return the
  // actual number we're looking for (presumably for some legacy
  // backward-compat reasons).  Below is the algorithm that is used to
  // calculate the pending tasks within the Hadoop JobTracker sources (see
  // 'printTaskSummary' in src/org/apache/hadoop/mapred/jobdetails_jsp.java).
  private int getPendingTasks(TaskInProgress[] tasks) {
    int totalTasks = tasks.length;
    int runningTasks = 0;
    int finishedTasks = 0;
    int killedTasks = 0;
    for (int i = 0; i < totalTasks; ++i) {
      TaskInProgress task = tasks[i];
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
    int pendingTasks = totalTasks - runningTasks - killedTasks - finishedTasks;
    return pendingTasks;
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
            mesosTrackers.get(tracker).timer.cancel();
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

  private class ResourcePolicy {
    public volatile MesosScheduler scheduler;
    public int neededMapSlots;
    public int neededReduceSlots;
    public long slots, mapSlots, reduceSlots;
    public int mapSlotsMax, reduceSlotsMax;
    double slotCpus;
    double slotDisk;
    int slotMem;
    long slotJVMHeap;
    String childOpts;
    int tasktrackerMem;
    long tasktrackerJVMHeap;
    // Minimum resource requirements for the container (TaskTracker + map/red
    // tasks).
    double containerCpus;
    double containerMem;
    double containerDisk;
    double cpus;
    double mem;
    double disk;

    public ResourcePolicy(MesosScheduler scheduler) {
      this.scheduler = scheduler;

      mapSlotsMax = conf.getInt("mapred.tasktracker.map.tasks.maximum",
          MAP_SLOTS_DEFAULT);
      reduceSlotsMax =
          conf.getInt("mapred.tasktracker.reduce.tasks.maximum",
              REDUCE_SLOTS_DEFAULT);

      slotCpus = conf.getFloat("mapred.mesos.slot.cpus",
          (float) SLOT_CPUS_DEFAULT);
      slotDisk = conf.getInt("mapred.mesos.slot.disk",
          SLOT_DISK_DEFAULT);

      slotMem = conf.getInt("mapred.mesos.slot.mem",
          SLOT_JVM_HEAP_DEFAULT);
      slotJVMHeap = Math.round((double) slotMem /
          (JVM_MEM_OVERHEAD_PERCENT_DEFAULT + 1));
      childOpts = conf.get("mapred.child.java.opts");

      tasktrackerMem = conf.getInt("mapred.mesos.tasktracker.mem",
          TASKTRACKER_MEM_DEFAULT);
      tasktrackerJVMHeap = Math.round((double) tasktrackerMem /
          (JVM_MEM_OVERHEAD_PERCENT_DEFAULT + 1));

      containerCpus = TASKTRACKER_CPUS;
      containerMem = tasktrackerMem;
      containerDisk = 0;

    }

    public void computeNeededSlots(List<JobInProgress> jobsInProgress,
                                   Collection<TaskTrackerStatus> taskTrackers) {
      // Compute the number of pending maps and reduces.
      int pendingMaps = 0;
      int pendingReduces = 0;
      int runningMaps = 0;
      int runningReduces = 0;
      for (JobInProgress progress : jobsInProgress) {
        // JobStatus.pendingMaps/Reduces may return the wrong value on
        // occasion.  This seems to be safer.
        pendingMaps += getPendingTasks(progress.getTasks(TaskType.MAP));
        pendingReduces += getPendingTasks(progress.getTasks(TaskType.REDUCE));
        runningMaps += progress.runningMaps();
        runningReduces += progress.runningReduces();
      }

      // Mark active (heartbeated) TaskTrackers and compute idle slots.
      int idleMapSlots = 0;
      int idleReduceSlots = 0;
      int unhealthyTrackers = 0;

      for (TaskTrackerStatus status : taskTrackers) {
        if (!status.getHealthStatus().isNodeHealthy()) {
          // Skip this node if it's unhealthy.
          ++unhealthyTrackers;
          continue;
        }

        HttpHost host = new HttpHost(status.getHost(), status.getHttpPort());
        if (mesosTrackers.containsKey(host)) {
          mesosTrackers.get(host).active = true;
          mesosTrackers.get(host).timer.cancel();
          idleMapSlots += status.getAvailableMapSlots();
          idleReduceSlots += status.getAvailableReduceSlots();
        }
      }

      // Consider the TaskTrackers that have yet to become active as being idle,
      // otherwise we will launch excessive TaskTrackers.
      int inactiveMapSlots = 0;
      int inactiveReduceSlots = 0;
      for (MesosTracker tracker : mesosTrackers.values()) {
        if (!tracker.active) {
          inactiveMapSlots += tracker.mapSlots;
          inactiveReduceSlots += tracker.reduceSlots;
        }
      }

      // To ensure Hadoop jobs begin promptly, we can specify a minimum number
      // of 'hot slots' to be available for use.  This addresses the
      // TaskTracker spin up delay that exists with Hadoop on Mesos.  This can
      // be a nuisance with lower latency applications, such as ad-hoc Hive
      // queries.
      int minimumMapSlots = conf.getInt("mapred.mesos.slot.map.minimum", 0);
      int minimumReduceSlots =
          conf.getInt("mapred.mesos.slot.reduce.minimum", 0);

      // Compute how many slots we need to allocate.
      neededMapSlots = Math.max(
          minimumMapSlots - (idleMapSlots + inactiveMapSlots),
          pendingMaps - (idleMapSlots + inactiveMapSlots));
      neededReduceSlots = Math.max(
          minimumReduceSlots - (idleReduceSlots + inactiveReduceSlots),
          pendingReduces - (idleReduceSlots + inactiveReduceSlots));

      LOG.info(join("\n", Arrays.asList(
          "JobTracker Status",
          "      Pending Map Tasks: " + pendingMaps,
          "   Pending Reduce Tasks: " + pendingReduces,
          "      Running Map Tasks: " + runningMaps,
          "   Running Reduce Tasks: " + runningReduces,
          "         Idle Map Slots: " + idleMapSlots,
          "      Idle Reduce Slots: " + idleReduceSlots,
          "     Inactive Map Slots: " + inactiveMapSlots
              + " (launched but no hearbeat yet)",
          "  Inactive Reduce Slots: " + inactiveReduceSlots
              + " (launched but no hearbeat yet)",
          "       Needed Map Slots: " + neededMapSlots,
          "    Needed Reduce Slots: " + neededReduceSlots,
          "     Unhealthy Trackers: " + unhealthyTrackers)));

      if (stateFile != null) {
        // Update state file
        Set<String> hosts = new HashSet<String>();
        for (MesosTracker tracker : mesosTrackers.values()) {
          hosts.add(tracker.host.getHostName());
        }
        try {
          File tmp = new File(stateFile.getAbsoluteFile() + ".tmp");
          FileWriter fstream = new FileWriter(tmp);
          fstream.write(join("\n", Arrays.asList(
              "time=" + System.currentTimeMillis(),
              "pendingMaps=" + pendingMaps,
              "pendingReduces=" + pendingReduces,
              "runningMaps=" + runningMaps,
              "runningReduces=" + runningReduces,
              "idleMapSlots=" + idleMapSlots,
              "idleReduceSlots=" + idleReduceSlots,
              "inactiveMapSlots=" + inactiveMapSlots,
              "inactiveReduceSlots=" + inactiveReduceSlots,
              "neededMapSlots=" + neededMapSlots,
              "neededReduceSlots=" + neededReduceSlots,
              "unhealthyTrackers=" + unhealthyTrackers,
              "hosts=" + join(",", hosts),
              "")));
          fstream.close();
          tmp.renameTo(stateFile);
        } catch (Exception e) {
          LOG.error("Can't write state file: " + e.getMessage());
        }
      }
    }

    // This method computes the number of slots to launch for this offer, and
    // returns true if the offer is sufficient.
    // Must be overridden.
    public boolean computeSlots() {
      return false;
    }

    public void resourceOffers(SchedulerDriver schedulerDriver,
                               List<Offer> offers) {
      // Before synchronizing, we pull all needed information from the JobTracker.
      final HttpHost jobTrackerAddress =
          new HttpHost(jobTracker.getHostname(), jobTracker.getTrackerPort());

      final Collection<TaskTrackerStatus> taskTrackers = jobTracker.taskTrackers();

      final List<JobInProgress> jobsInProgress = new ArrayList<JobInProgress>();
      for (JobStatus status : jobTracker.jobsToComplete()) {
        jobsInProgress.add(jobTracker.getJob(status.getJobID()));
      }

      synchronized (scheduler) {
        computeNeededSlots(jobsInProgress, taskTrackers);

        // Launch TaskTrackers to satisfy the slot requirements.
        for (Offer offer : offers) {
          if (neededMapSlots <= 0 && neededReduceSlots <= 0) {
            schedulerDriver.declineOffer(offer.getId());
            continue;
          }

          // Ensure these values aren't < 0.
          neededMapSlots = Math.max(0, neededMapSlots);
          neededReduceSlots = Math.max(0, neededReduceSlots);

          cpus = -1.0;
          mem = -1.0;
          disk = -1.0;
          Set<Integer> ports = new HashSet<Integer>();

          // Pull out the cpus, memory, disk, and 2 ports from the offer.
          for (Resource resource : offer.getResourcesList()) {
            if (resource.getName().equals("cpus")
                && resource.getType() == Value.Type.SCALAR) {
              cpus = resource.getScalar().getValue();
            } else if (resource.getName().equals("mem")
                && resource.getType() == Value.Type.SCALAR) {
              mem = resource.getScalar().getValue();
            } else if (resource.getName().equals("disk")
                && resource.getType() == Value.Type.SCALAR) {
              disk = resource.getScalar().getValue();
            } else if (resource.getName().equals("ports")
                && resource.getType() == Value.Type.RANGES) {
              for (Value.Range range : resource.getRanges().getRangeList()) {
                Integer begin = (int) Math.min(range.getBegin(), range.getEnd());
                Integer end = (int) Math.max(range.getBegin(), range.getEnd());
                while (begin <= end && ports.size() < 2) {
                  ports.add(begin);
                  begin += 1;
                }
              }
            }
          }

          final boolean sufficient = computeSlots();

          double taskCpus = (mapSlots + reduceSlots) * slotCpus + containerCpus;
          double taskMem = (mapSlots + reduceSlots) * slotMem + containerMem;
          double taskDisk = (mapSlots + reduceSlots) * slotDisk + containerDisk;

          if (!sufficient || ports.size() < 2) {
            LOG.info(join("\n", Arrays.asList(
                "Declining offer with insufficient resources for a TaskTracker: ",
                "  cpus: offered " + cpus + " needed at least " + taskCpus,
                "  mem : offered " + mem + " needed at least " + taskMem,
                "  disk: offered " + disk + " needed at least " + taskDisk,
                "  ports: " + (ports.size() < 2
                    ? " less than 2 offered"
                    : " at least 2 (sufficient)"))));

            schedulerDriver.declineOffer(offer.getId());
            continue;
          }

          Iterator<Integer> portIter = ports.iterator();
          HttpHost httpAddress = new HttpHost(offer.getHostname(), portIter.next());
          HttpHost reportAddress = new HttpHost(offer.getHostname(), portIter.next());

          // Check that this tracker is not already launched.  This problem was
          // observed on a few occasions, but not reliably.
          // TODO(brenden): Diagnose this to determine root cause.
          if (mesosTrackers.containsKey(httpAddress)) {
            LOG.info(join("\n", Arrays.asList(
                "Declining offer because host/port combination is in use: ",
                "  cpus: offered " + cpus + " needed " + taskCpus,
                "  mem : offered " + mem + " needed " + taskMem,
                "  disk: offered " + disk + " needed " + taskDisk,
                "  ports: " + ports)));

            schedulerDriver.declineOffer(offer.getId());
            continue;
          }

          TaskID taskId = TaskID.newBuilder()
              .setValue("Task_Tracker_" + launchedTrackers++).build();

          LOG.info("Launching task " + taskId.getValue() + " on "
              + httpAddress.toString() + " with mapSlots=" + mapSlots + " reduceSlots=" + reduceSlots);

          // Add this tracker to Mesos tasks.
          mesosTrackers.put(httpAddress, new MesosTracker(httpAddress, taskId,
              mapSlots, reduceSlots, scheduler));

          // Create the environment depending on whether the executor is going to be
          // run locally.
          // TODO(vinod): Do not pass the mapred config options as environment
          // variables.

          Configuration overrides = new Configuration(conf);

          overrides.set("mapred.job.tracker",
              jobTrackerAddress.getHostName() + ':' + jobTrackerAddress.getPort());

          overrides.set("mapred.task.tracker.http.address",
              httpAddress.getHostName() + ':' + httpAddress.getPort());

          overrides.set("mapred.task.tracker.report.address",
              reportAddress.getHostName() + ':' + reportAddress.getPort());

          overrides.set("mapred.child.java.opts",
              childOpts + " -Xmx" + slotJVMHeap + "m");

          overrides.setLong("mapred.tasktracker.map.tasks.maximum",
              mapSlots);

          overrides.setLong("mapred.tasktracker.reduce.tasks.maximum",
              reduceSlots);

          StringWriter sw = new StringWriter();
          String xmlString = null;
          try {
            overrides.writeXml(sw);
            xmlString = sw.getBuffer().toString();
            LOG.info("Configuration: " + org.apache.mesos.hadoop.Utils.formatXml(xmlString));
          } catch (Exception e) {
            LOG.warn("Malformed XML.", e);
            System.exit(1);
          }

          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          try {
            overrides.write(new DataOutputStream(baos));
            baos.flush();
          } catch (IOException e) {
            LOG.warn("Failed to serialize configuration.", e);
            System.exit(1);
          }

          String converted = Base64.encodeBase64String(baos.toByteArray());

          Protos.Environment.Builder envBuilder = Protos.Environment
              .newBuilder()
              .addVariables(
                  Protos.Environment.Variable.newBuilder()
                      .setName("HADOOP_HEAPSIZE")
                      .setValue("" + tasktrackerJVMHeap))
              .addVariables(
                  Protos.Environment.Variable.newBuilder()
                      .setName(Constants.HADOOP_MESOS_CONF_STRING)
                      .setValue(converted));

          // Set java specific environment, appropriately.
          Map<String, String> env = System.getenv();
          if (env.containsKey("JAVA_HOME")) {
            envBuilder.addVariables(Protos.Environment.Variable.newBuilder()
                .setName("JAVA_HOME")
                .setValue(env.get("JAVA_HOME")));
          }

          if (env.containsKey("JAVA_LIBRARY_PATH")) {
            envBuilder.addVariables(Protos.Environment.Variable.newBuilder()
                .setName("JAVA_LIBRARY_PATH")
                .setValue(env.get("JAVA_LIBRARY_PATH")));
          }

          // Command info differs when performing a local run.
          CommandInfo commandInfo = null;
          String master = conf.get("mapred.mesos.master", "local");

          if (master.equals("local")) {
            try {
              commandInfo = CommandInfo.newBuilder()
                  .setEnvironment(envBuilder)
                  .setValue(new File("bin/mesos-executor").getCanonicalPath())
                  .build();
            } catch (IOException e) {
              LOG.fatal("Failed to find Mesos executor ", e);
              System.exit(1);
            }
          } else {

            String uri = conf.get("mapred.mesos.executor.uri");
            if (uri == null) {
              throw new RuntimeException(
                  "Expecting configuration property 'mapred.mesos.executor'");
            }

            String directory = conf.get("mapred.mesos.executor.directory");
            if (directory == null || directory.equals("")) {
              LOG.info("URI: " + uri + ", name: " + new File(uri).getName());

              directory = new File(uri).getName().split("\\.")[0] + "*";
            }

            String command = conf.get("mapred.mesos.executor.command");
            if (command == null || command.equals("")) {
              command = "echo $(env) ; " +
                  " ./bin/hadoop  org.apache.hadoop.mapred.MesosExecutor";
            }

            commandInfo = CommandInfo.newBuilder()
                .setEnvironment(envBuilder)
                .setValue(String.format("cd %s && %s", directory, command))
                .addUris(CommandInfo.URI.newBuilder().setValue(uri)).build();
          }

          TaskInfo info = TaskInfo
              .newBuilder()
              .setName(taskId.getValue())
              .setTaskId(taskId)
              .setSlaveId(offer.getSlaveId())
              .addResources(
                  Resource
                      .newBuilder()
                      .setName("cpus")
                      .setType(Value.Type.SCALAR)
                      .setScalar(Value.Scalar.newBuilder().setValue(taskCpus)))
              .addResources(
                  Resource
                      .newBuilder()
                      .setName("mem")
                      .setType(Value.Type.SCALAR)
                      .setScalar(Value.Scalar.newBuilder().setValue(taskMem)))
              .addResources(
                  Resource
                      .newBuilder()
                      .setName("disk")
                      .setType(Value.Type.SCALAR)
                      .setScalar(Value.Scalar.newBuilder().setValue(taskDisk)))
              .addResources(
                  Resource
                      .newBuilder()
                      .setName("ports")
                      .setType(Value.Type.RANGES)
                      .setRanges(
                          Value.Ranges
                              .newBuilder()
                              .addRange(Value.Range.newBuilder()
                                  .setBegin(httpAddress.getPort())
                                  .setEnd(httpAddress.getPort()))
                              .addRange(Value.Range.newBuilder()
                                  .setBegin(reportAddress.getPort())
                                  .setEnd(reportAddress.getPort()))))
              .setExecutor(
                  ExecutorInfo
                      .newBuilder()
                      .setExecutorId(ExecutorID.newBuilder().setValue(
                          "executor_" + taskId.getValue()))
                      .setName("Hadoop TaskTracker")
                      .setSource(taskId.getValue())
                      .addResources(
                          Resource
                              .newBuilder()
                              .setName("cpus")
                              .setType(Value.Type.SCALAR)
                              .setScalar(Value.Scalar.newBuilder().setValue(
                                  (TASKTRACKER_CPUS))))
                      .addResources(
                          Resource
                              .newBuilder()
                              .setName("mem")
                              .setType(Value.Type.SCALAR)
                              .setScalar(Value.Scalar.newBuilder().setValue(
                                  (tasktrackerMem)))).setCommand(commandInfo))
              .build();

          schedulerDriver.launchTasks(offer.getId(), Arrays.asList(info));

          neededMapSlots -= mapSlots;
          neededReduceSlots -= reduceSlots;
        }

        if (neededMapSlots <= 0 && neededReduceSlots <= 0) {
          LOG.info("Satisfied map and reduce slots needed.");
        } else {
          LOG.info("Unable to fully satisfy needed map/reduce slots: "
              + (neededMapSlots > 0 ? neededMapSlots + " map slots " : "")
              + (neededReduceSlots > 0 ? neededReduceSlots + " reduce slots " : "")
              + "remaining");
        }
      }
    }
  }

  private class ResourcePolicyFixed extends ResourcePolicy {

    public ResourcePolicyFixed(MesosScheduler scheduler) {
      super(scheduler);
    }

    // This method computes the number of slots to launch for this offer, and
    // returns true if the offer is sufficient.
    @Override
    public boolean computeSlots() {
      mapSlots = mapSlotsMax;
      reduceSlots = reduceSlotsMax;

      slots = Integer.MAX_VALUE;
      slots = (int) Math.min(slots, (cpus - containerCpus) / slotCpus);
      slots = (int) Math.min(slots, (mem - containerMem) / slotMem);
      slots = (int) Math.min(slots, (disk - containerDisk) / slotDisk);

      // Is this offer too small for even the minimum slots?
      if (slots < mapSlots + reduceSlots) {
        return false;
      }
      return true;
    }
  }

  private class ResourcePolicyVariable extends ResourcePolicy {
    public ResourcePolicyVariable(MesosScheduler scheduler) {
      super(scheduler);
    }

    // This method computes the number of slots to launch for this offer, and
    // returns true if the offer is sufficient.
    @Override
    public boolean computeSlots() {
      // What's the minimum number of map and reduce slots we should try to
      // launch?
      mapSlots = 0;
      reduceSlots = 0;

      // Determine how many slots we can allocate.
      int slots = mapSlotsMax + reduceSlotsMax;
      slots = (int) Math.min(slots, (cpus - containerCpus) / slotCpus);
      slots = (int) Math.min(slots, (mem - containerMem) / slotMem);
      slots = (int) Math.min(slots, (disk - containerDisk) / slotDisk);

      // Is this offer too small for even the minimum slots?
      if (slots < 1) {
        return false;
      }

      // Is the number of slots we need sufficiently small? If so, we can
      // allocate exactly the number we need.
      if (slots >= neededMapSlots + neededReduceSlots && neededMapSlots <
          mapSlotsMax && neededReduceSlots < reduceSlotsMax) {
        mapSlots = neededMapSlots;
        reduceSlots = neededReduceSlots;
      } else {
        // Allocate slots fairly for this resource offer.
        double mapFactor = (double) neededMapSlots / (neededMapSlots + neededReduceSlots);
        double reduceFactor = (double) neededReduceSlots / (neededMapSlots + neededReduceSlots);
        // To avoid map/reduce slot starvation, don't allow more than 50%
        // spread between map/reduce slots when we need both mappers and
        // reducers.
        if (neededMapSlots > 0 && neededReduceSlots > 0) {
          if (mapFactor < 0.25) {
            mapFactor = 0.25;
          } else if (mapFactor > 0.75) {
            mapFactor = 0.75;
          }
          if (reduceFactor < 0.25) {
            reduceFactor = 0.25;
          } else if (reduceFactor > 0.75) {
            reduceFactor = 0.75;
          }
        }
        mapSlots = Math.min(Math.min((long) (mapFactor * slots), mapSlotsMax), neededMapSlots);

        // The remaining slots are allocated for reduces.
        slots -= mapSlots;
        reduceSlots = Math.min(Math.min(slots, reduceSlotsMax), neededReduceSlots);
      }
      return true;
    }
  }

  /**
   * Used to track the our launched TaskTrackers.
   */
  private class MesosTracker {
    public volatile HttpHost host;
    public TaskID taskId;
    public long mapSlots;
    public long reduceSlots;
    public volatile boolean active = false; // Set once tracked by the JobTracker.
    public Timer timer;
    public volatile MesosScheduler scheduler;
    // Tracks Hadoop jobs running on the tracker.
    public Set<JobID> jobs = new HashSet<JobID>();

    public MesosTracker(HttpHost host, TaskID taskId, long mapSlots,
                        long reduceSlots, MesosScheduler scheduler) {
      this.host = host;
      this.taskId = taskId;
      this.mapSlots = mapSlots;
      this.reduceSlots = reduceSlots;
      this.scheduler = scheduler;

      this.timer = new Timer();
      timer.schedule(new TimerTask() {
        @Override
        public void run() {
          synchronized (MesosTracker.this.scheduler) {
            // If the tracker activated while we were awaiting to acquire the
            // lock, return.
            if (MesosTracker.this.active) return;

            LOG.warn("Tracker " + MesosTracker.this.host + " failed to launch within " +
                LAUNCH_TIMEOUT_MS / 1000 + " seconds, killing it");
            MesosTracker.this.scheduler.killTracker(MesosTracker.this);
          }
        }
      }, LAUNCH_TIMEOUT_MS);
    }
  }
}