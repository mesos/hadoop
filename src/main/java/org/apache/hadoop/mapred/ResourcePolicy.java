package org.apache.hadoop.mapred;

import org.apache.commons.httpclient.HttpHost;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.StringUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.*;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.SchedulerDriver;
import com.google.protobuf.ByteString;

import java.io.*;
import java.util.*;

import static org.apache.hadoop.util.StringUtils.join;

public abstract class ResourcePolicy {
  public static final Log LOG = LogFactory.getLog(ResourcePolicy.class);
  public volatile MesosScheduler scheduler;
  public int neededMapSlots;
  public int neededReduceSlots;
  public long slots, mapSlots, reduceSlots;
  public int mapSlotsMax, reduceSlotsMax;
  double slotCpus;
  double slotDisk;
  int slotMem;
  long slotJVMHeap;
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

    mapSlotsMax = scheduler.conf.getInt("mapred.tasktracker.map.tasks.maximum",
        MesosScheduler.MAP_SLOTS_DEFAULT);
    reduceSlotsMax =
        scheduler.conf.getInt("mapred.tasktracker.reduce.tasks.maximum",
            MesosScheduler.REDUCE_SLOTS_DEFAULT);

    slotCpus = scheduler.conf.getFloat("mapred.mesos.slot.cpus",
        (float) MesosScheduler.SLOT_CPUS_DEFAULT);
    slotDisk = scheduler.conf.getInt("mapred.mesos.slot.disk",
        MesosScheduler.SLOT_DISK_DEFAULT);

    slotMem = scheduler.conf.getInt("mapred.mesos.slot.mem",
        MesosScheduler.SLOT_JVM_HEAP_DEFAULT);
    slotJVMHeap = Math.round((double) slotMem /
        (MesosScheduler.JVM_MEM_OVERHEAD_PERCENT_DEFAULT + 1));

    tasktrackerMem = scheduler.conf.getInt("mapred.mesos.tasktracker.mem",
        MesosScheduler.TASKTRACKER_MEM_DEFAULT);
    tasktrackerJVMHeap = Math.round((double) tasktrackerMem /
        (MesosScheduler.JVM_MEM_OVERHEAD_PERCENT_DEFAULT + 1));

    containerCpus = scheduler.conf.getFloat("mapred.mesos.tasktracker.cpus",
        (float) MesosScheduler.TASKTRACKER_CPUS_DEFAULT);
    containerDisk = scheduler.conf.getInt("mapred.mesos.tasktracker.disk",
        MesosScheduler.TASKTRACKER_DISK_DEFAULT);

    containerMem = tasktrackerMem;
  }

  public void resourceOffers(SchedulerDriver schedulerDriver, List<Offer> offers) {
//    final HttpHost jobTrackerAddress =
//        new HttpHost(scheduler.jobTracker.getHostname(), scheduler.jobTracker.getTrackerPort());

    final Collection<TaskTrackerStatus> taskTrackers = scheduler.jobTracker.taskTrackers();

    final List<JobInProgress> jobsInProgress = new ArrayList<>();
    for (JobStatus status : scheduler.jobTracker.jobsToComplete()) {
      jobsInProgress.add(scheduler.jobTracker.getJob(status.getJobID()));
    }

    synchronized (this) {
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
        String cpuRole = "*";
        String memRole = cpuRole;
        String diskRole = cpuRole;
        String portsRole = cpuRole;

        // Pull out the cpus, memory, disk, and 2 ports from the offer.
        for (Resource resource : offer.getResourcesList()) {
          if (resource.getName().equals("cpus")
              && resource.getType() == Value.Type.SCALAR) {
            cpus = resource.getScalar().getValue();
            cpuRole = resource.getRole();
          } else if (resource.getName().equals("mem")
              && resource.getType() == Value.Type.SCALAR) {
            mem = resource.getScalar().getValue();
            memRole = resource.getRole();
          } else if (resource.getName().equals("disk")
              && resource.getType() == Value.Type.SCALAR) {
            disk = resource.getScalar().getValue();
            diskRole = resource.getRole();
          } else if (resource.getName().equals("ports")
              && resource.getType() == Value.Type.RANGES) {
            portsRole = resource.getRole();
            for (Value.Range range : resource.getRanges().getRangeList()) {
              Integer begin = (int) range.getBegin();
              Integer end = (int) range.getEnd();
              if (end < begin) {
                LOG.warn("Ignoring invalid port range: begin=" + begin + " end=" + end);
                continue;
              }
              while (begin <= end && ports.size() < 2) {
                int port = begin + (int)(Math.random() * ((end - begin) + 1));
                ports.add(port);
                begin += 1;
              }
            }
          }
        }

        // Verify the resource roles are what we need
        if (scheduler.conf.getBoolean("mapred.mesos.role.strict", false)) {
          String expectedRole = scheduler.conf.get("mapred.mesos.role", "*");
          if (!cpuRole.equals(expectedRole) ||
              !memRole.equals(expectedRole) ||
              !diskRole.equals(expectedRole) ||
              !portsRole.equals(expectedRole)) {
            LOG.info("Declining offer with invalid role " + expectedRole);

            schedulerDriver.declineOffer(offer.getId());
            continue;
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
        // observed on a few occasions, but not reliably.  The main symptom was
        // that entries in `mesosTrackers` were being lost, and task trackers
        // would be 'lost' mysteriously (probably because the ports were in
        // use).  This problem has since gone away with a rewrite of the port
        // selection code, but the check + logging is left here.
        // TODO(brenden): Diagnose this to determine root cause.
        if (scheduler.mesosTrackers.containsKey(httpAddress)) {
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
            .setValue("Task_Tracker_" + scheduler.launchedTrackers++).build();

        LOG.info("Launching task " + taskId.getValue() + " on "
            + httpAddress.toString() + " with mapSlots=" + mapSlots + " reduceSlots=" + reduceSlots);

        List<String> defaultJvmOpts = Arrays.asList(
            "-XX:+UseConcMarkSweepGC",
            "-XX:+CMSParallelRemarkEnabled",
            "-XX:+CMSClassUnloadingEnabled",
            "-XX:+UseParNewGC",
            "-XX:TargetSurvivorRatio=80",
            "-XX:+UseTLAB",
            "-XX:ParallelGCThreads=2",
            "-XX:+AggressiveOpts",
            "-XX:+UseCompressedOops",
            "-XX:+UseFastEmptyMethods",
            "-XX:+UseFastAccessorMethods",
            "-Xss512k",
            "-XX:+AlwaysPreTouch",
            "-XX:CMSInitiatingOccupancyFraction=80"
        );

        String jvmOpts = scheduler.conf.get("mapred.mesos.executor.jvm.opts");
        if (jvmOpts == null) {
            jvmOpts = StringUtils.join(" ", defaultJvmOpts);
        }

        // Set up the environment for running the TaskTracker.
        Protos.Environment.Builder envBuilder = Protos.Environment
            .newBuilder()
            .addVariables(
                Protos.Environment.Variable.newBuilder()
                    .setName("HADOOP_OPTS")
                    .setValue(
                        jvmOpts +
                            " -Xmx" + tasktrackerJVMHeap + "m" +
                            " -XX:NewSize=" + tasktrackerJVMHeap / 3 + "m -XX:MaxNewSize=" + (int)Math.floor
                            (tasktrackerJVMHeap * 0.6) + "m"
                    ));

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
        String master = scheduler.conf.get("mapred.mesos.master");

        if (master == null) {
          throw new RuntimeException(
              "Expecting configuration property 'mapred.mesos.master'");
        } else if (Objects.equals(master, "local")) {
          throw new RuntimeException(
              "Can not use 'local' for 'mapred.mesos.executor'");
        }

        String uri = scheduler.conf.get("mapred.mesos.executor.uri");
        String directory = scheduler.conf.get("mapred.mesos.executor.directory");
        boolean isUriSet = uri != null && !uri.equals("");
        boolean isDirectorySet = directory != null && !directory.equals("");

        if (!isUriSet && !isDirectorySet) {
          throw new RuntimeException(
              "Expecting configuration property 'mapred.mesos.executor'");
        } else if (isUriSet && isDirectorySet) {
          throw new RuntimeException(
              "Conflicting properties 'mapred.mesos.executor.uri' and 'mapred.mesos.executor.directory', only one can be set");
        } else if (!isDirectorySet) {
          LOG.info("URI: " + uri + ", name: " + new File(uri).getName());

          directory = new File(uri).getName().split("\\.")[0] + "*";
        } else if (!isUriSet) {
	    LOG.info("mapred.mesos.executor.uri is not set, relying on configured 'mapred.mesos.executor.directory' for working Hadoop distribution");
        }

        String command = scheduler.conf.get("mapred.mesos.executor.command");
        if (command == null || command.equals("")) {
          command = "env ; ./bin/hadoop org.apache.hadoop.mapred.MesosExecutor";
        }

        CommandInfo.Builder commandInfo = CommandInfo.newBuilder();
        commandInfo
            .setEnvironment(envBuilder)
            .setValue(String.format("cd %s && %s", directory, command));
        if (uri != null) {
            commandInfo.addUris(CommandInfo.URI.newBuilder().setValue(uri));
        }

        // Populate ContainerInfo if needed
        String containerImage = scheduler.conf.get("mapred.mesos.container.image");
        String[] containerOptions = scheduler.conf.getStrings("mapred.mesos.container.options");

        if (containerImage != null || (containerOptions != null && containerOptions.length > 0)) {
          CommandInfo.ContainerInfo.Builder containerInfo =
              CommandInfo.ContainerInfo.newBuilder();

          if (containerImage != null) {
            containerInfo.setImage(containerImage);
          }

          if (containerOptions != null) {
            for (String containerOption : containerOptions) {
              containerInfo.addOptions(containerOption);
            }
          }

          commandInfo.setContainer(containerInfo.build());
        }

        // Create a configuration from the current configuration and
        // override properties as appropriate for the TaskTracker.
        Configuration overrides = new Configuration(scheduler.conf);

        overrides.set("mapred.task.tracker.http.address",
            httpAddress.getHostName() + ':' + httpAddress.getPort());

        overrides.set("mapred.task.tracker.report.address",
            reportAddress.getHostName() + ':' + reportAddress.getPort());

        overrides.setLong("mapred.tasktracker.map.tasks.maximum", mapSlots);
        overrides.setLong("mapred.tasktracker.reduce.tasks.maximum", reduceSlots);

        // Build up the executor info
        ExecutorInfo executor = ExecutorInfo
            .newBuilder()
            .setExecutorId(ExecutorID.newBuilder().setValue(
                "Executor_" + taskId.getValue()))
            .setName("Hadoop TaskTracker")
            .setSource(taskId.getValue())
            .addResources(
                Resource
                    .newBuilder()
                    .setName("cpus")
                    .setType(Value.Type.SCALAR)
                    .setRole(cpuRole)
                    .setScalar(Value.Scalar.newBuilder().setValue(containerCpus)))
            .addResources(
                Resource
                    .newBuilder()
                    .setName("mem")
                    .setType(Value.Type.SCALAR)
                    .setRole(memRole)
                    .setScalar(Value.Scalar.newBuilder().setValue(containerMem)))
            .addResources(
                Resource
                    .newBuilder()
                    .setName("disk")
                    .setType(Value.Type.SCALAR)
                    .setRole(diskRole)
                    .setScalar(Value.Scalar.newBuilder().setValue(containerDisk)))
            .setCommand(commandInfo.build())
            .build();

        ByteString taskData;

        try {
          taskData = org.apache.mesos.hadoop.Utils.confToBytes(overrides);
        } catch (IOException e) {
          LOG.error("Caught exception serializing configuration");

          // Skip this offer completely
          schedulerDriver.declineOffer(offer.getId());
          continue;
        }

        List<TaskInfo> tasks = new ArrayList();
        TaskID mapTaskId = null;
        TaskID reduceTaskId = null;

        if (mapSlots > 0) {
          TaskInfo mapTask = buildTaskInfo(executor, taskId.getValue() + "_Map", offer,
                                           httpAddress.getPort(), reportAddress.getPort(), mapSlots, taskData,
                                           portsRole, cpuRole, memRole);
          
          mapTaskId = mapTask.getTaskId();
          tasks.add(mapTask);
        }

        if (reduceSlots > 0) {
          TaskInfo reduceTask = buildTaskInfo(executor, taskId.getValue() + "_Reduce", offer,
                                              httpAddress.getPort(), reportAddress.getPort(), reduceSlots, taskData,
                                              portsRole, cpuRole, memRole);

          reduceTaskId = reduceTask.getTaskId();
          tasks.add(reduceTask);
        }

        // Add this tracker to Mesos tasks.
        scheduler.mesosTrackers.put(httpAddress, new MesosTracker(httpAddress,
            mapTaskId, reduceTaskId, mapSlots, reduceSlots, scheduler));

        // Launch the task
        schedulerDriver.launchTasks(Arrays.asList(offer.getId()), tasks);

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

  protected TaskInfo buildTaskInfo(ExecutorInfo executor, String taskId, Offer offer,
                                   Integer httpPort, Integer reportPort, long slots, ByteString taskData,
                                   String portsRole, String cpuRole, String memRole) {

    TaskInfo taskInfo = TaskInfo
      .newBuilder()
      .setName(taskId)
      .setTaskId(TaskID.newBuilder().setValue(taskId))
      .setSlaveId(offer.getSlaveId())
      .addResources(
          Resource
              .newBuilder()
              .setName("ports")
              .setType(Value.Type.RANGES)
              .setRole(portsRole)
              .setRanges(
                  Value.Ranges
                      .newBuilder()
                      .addRange(Value.Range.newBuilder()
                          .setBegin(httpPort)
                          .setEnd(httpPort))
                      .addRange(Value.Range.newBuilder()
                          .setBegin(reportPort)
                          .setEnd(reportPort))))
      .addResources(
          Resource
              .newBuilder()
              .setName("cpus")
              .setType(Value.Type.SCALAR)
              .setRole(cpuRole)
              .setScalar(Value.Scalar.newBuilder().setValue(slotCpus * slots)))
      .addResources(
          Resource
              .newBuilder()
              .setName("mem")
              .setType(Value.Type.SCALAR)
              .setRole(memRole)
              .setScalar(Value.Scalar.newBuilder().setValue(slotMem * slots)))
      .setData(taskData)
      .setExecutor(executor)
      .build();

      return taskInfo;
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
      pendingMaps += scheduler.getPendingTasks(progress.getTasks(TaskType.MAP));
      pendingReduces += scheduler.getPendingTasks(progress.getTasks(TaskType.REDUCE));
      runningMaps += progress.runningMaps();
      runningReduces += progress.runningReduces();

      // If the task is waiting to launch the cleanup task, let us make sure we have
      // capacity to run the task.
      if (!progress.isCleanupLaunched()) {
        pendingMaps += scheduler.getPendingTasks(progress.getTasks(TaskType.JOB_CLEANUP));
      }
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
      if (scheduler.mesosTrackers.containsKey(host)) {
        scheduler.mesosTrackers.get(host).active = true;
        idleMapSlots += status.getAvailableMapSlots();
        idleReduceSlots += status.getAvailableReduceSlots();
      }
    }

    // Consider the TaskTrackers that have yet to become active as being idle,
    // otherwise we will launch excessive TaskTrackers.
    int inactiveMapSlots = 0;
    int inactiveReduceSlots = 0;
    for (MesosTracker tracker : scheduler.mesosTrackers.values()) {
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
    int minimumMapSlots = scheduler.conf.getInt("mapred.mesos.total.map.slots.minimum", 0);
    int minimumReduceSlots =
        scheduler.conf.getInt("mapred.mesos.total.reduce.slots.minimum", 0);

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

      File stateFile = scheduler.stateFile;
      if (stateFile != null) {
      // Update state file
      synchronized (this) {
        Set<String> hosts = new HashSet<>();
        for (MesosTracker tracker : scheduler.mesosTrackers.values()) {
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
          if (!tmp.renameTo(stateFile)) {
              LOG.error("Can't overwrite state " + stateFile.getAbsolutePath());
          }
        } catch (Exception e) {
          LOG.error("Can't write state file: " + e.getMessage());
        }
      }
    }
  }

  // This method computes the number of slots to launch for this offer, and
  // returns true if the offer is sufficient.
  // Must be overridden.
  public abstract boolean computeSlots();
}
