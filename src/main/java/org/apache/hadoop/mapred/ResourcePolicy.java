package org.apache.hadoop.mapred;

import com.google.protobuf.ByteString;
import org.apache.commons.httpclient.HttpHost;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.*;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.SchedulerDriver;

import java.io.*;
import java.util.*;

import static org.apache.hadoop.util.StringUtils.join;

public class ResourcePolicy {
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
        (float) MesosScheduler.TASKTRACKER_CPUS);

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
      pendingMaps += scheduler.getPendingTasks(progress.getTasks(TaskType.MAP));
      pendingReduces += scheduler.getPendingTasks(progress.getTasks(TaskType.REDUCE));
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

    if (scheduler.stateFile != null) {
      // Update state file
      synchronized (this) {
        Set<String> hosts = new HashSet<String>();
        for (MesosTracker tracker : scheduler.mesosTrackers.values()) {
          hosts.add(tracker.host.getHostName());
        }
        try {
          File tmp = new File(scheduler.stateFile.getAbsoluteFile() + ".tmp");
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
          tmp.renameTo(scheduler.stateFile);
        } catch (Exception e) {
          LOG.error("Can't write state file: " + e.getMessage());
        }
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
    final HttpHost jobTrackerAddress =
        new HttpHost(scheduler.jobTracker.getHostname(), scheduler.jobTracker.getTrackerPort());

    final Collection<TaskTrackerStatus> taskTrackers = scheduler.jobTracker.taskTrackers();

    final List<JobInProgress> jobsInProgress = new ArrayList<JobInProgress>();
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
        String cpuRole = new String("*");
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

        // Add this tracker to Mesos tasks.
        scheduler.mesosTrackers.put(httpAddress, new MesosTracker(httpAddress, taskId,
            mapSlots, reduceSlots, scheduler));

        // Set up the environment for running the TaskTracker.
        Protos.Environment.Builder envBuilder = Protos.Environment
            .newBuilder()
            .addVariables(
                Protos.Environment.Variable.newBuilder()
                    .setName("HADOOP_HEAPSIZE")
                    .setValue("" + tasktrackerJVMHeap));

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
        String master = scheduler.conf.get("mapred.mesos.master");

        if (master == null) {
          throw new RuntimeException(
              "Expecting configuration property 'mapred.mesos.master'");
        } else if (master == "local") {
          throw new RuntimeException(
              "Can not use 'local' for 'mapred.mesos.executor'");
        }

        String uri = scheduler.conf.get("mapred.mesos.executor.uri");
        if (uri == null) {
          throw new RuntimeException(
              "Expecting configuration property 'mapred.mesos.executor'");
        }

        String directory = scheduler.conf.get("mapred.mesos.executor.directory");
        if (directory == null || directory.equals("")) {
          LOG.info("URI: " + uri + ", name: " + new File(uri).getName());

          directory = new File(uri).getName().split("\\.")[0] + "*";
        }

        String command = scheduler.conf.get("mapred.mesos.executor.command");
        if (command == null || command.equals("")) {
          command = "env ; ./bin/hadoop org.apache.hadoop.mapred.MesosExecutor";
        }

        commandInfo = CommandInfo.newBuilder()
            .setEnvironment(envBuilder)
            .setValue(String.format("cd %s && %s", directory, command))
            .addUris(CommandInfo.URI.newBuilder().setValue(uri)).build();

        // Create a configuration from the current configuration and
        // override properties as appropriate for the TaskTracker.
        Configuration overrides = new Configuration(scheduler.conf);

        overrides.set("mapred.job.tracker",
            jobTrackerAddress.getHostName() + ':' + jobTrackerAddress.getPort());

        overrides.set("mapred.task.tracker.http.address",
            httpAddress.getHostName() + ':' + httpAddress.getPort());

        overrides.set("mapred.task.tracker.report.address",
            reportAddress.getHostName() + ':' + reportAddress.getPort());

        overrides.set("mapred.child.java.opts",
            scheduler.conf.get("mapred.child.java.opts") +
                " -XX:+UseParallelGC -Xmx" + slotJVMHeap + "m -Xms" + slotJVMHeap + "m");

        overrides.set("mapred.map.child.java.opts",
            scheduler.conf.get("mapred.map.child.java.opts") +
                " -XX:+UseParallelGC -Xmx" + slotJVMHeap + "m -Xms" + slotJVMHeap + "m");

        overrides.set("mapred.reduce.child.java.opts",
            scheduler.conf.get("mapred.reduce.child.java.opts") +
                " -XX:+UseParallelGC -Xmx" + slotJVMHeap + "m -Xms" + slotJVMHeap + "m");

        overrides.setLong("mapred.tasktracker.map.tasks.maximum",
            mapSlots);

        overrides.setLong("mapred.tasktracker.reduce.tasks.maximum",
            reduceSlots);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
          overrides.write(new DataOutputStream(baos));
          baos.flush();
        } catch (IOException e) {
          LOG.warn("Failed to serialize configuration.", e);
          System.exit(1);
        }

        byte[] bytes = baos.toByteArray();

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
                    .setRole(cpuRole)
                    .setScalar(Value.Scalar.newBuilder().setValue(taskCpus - containerCpus)))
            .addResources(
                Resource
                    .newBuilder()
                    .setName("mem")
                    .setType(Value.Type.SCALAR)
                    .setRole(memRole)
                    .setScalar(Value.Scalar.newBuilder().setValue(taskMem - containerMem)))
            .addResources(
                Resource
                    .newBuilder()
                    .setName("disk")
                    .setType(Value.Type.SCALAR)
                    .setRole(diskRole)
                    .setScalar(Value.Scalar.newBuilder().setValue(taskDisk - containerDisk)))
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
                            .setRole(cpuRole)
                            .setScalar(Value.Scalar.newBuilder().setValue(
                                (containerCpus))))
                    .addResources(
                        Resource
                            .newBuilder()
                            .setName("mem")
                            .setType(Value.Type.SCALAR)
                            .setRole(memRole)
                            .setScalar(Value.Scalar.newBuilder().setValue(containerMem)))
                    .setCommand(commandInfo))
            .setData(ByteString.copyFrom(bytes))
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
