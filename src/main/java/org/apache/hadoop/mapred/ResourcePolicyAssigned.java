
package org.apache.hadoop.mapred;

import java.io.IOException;

import java.util.List;
import java.util.Set;
import java.util.Collection;
import java.util.ArrayList;
import java.util.HashSet;

import java.lang.NoSuchMethodException;
import java.lang.reflect.Method;

import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;

/**
 *
 */
public class ResourcePolicyAssigned extends ResourcePolicy {

  Method canLaunchSetupTask;
  Method canLaunchCleanupTask;

  class MockTaskTrackerManager implements TaskTrackerManager {
    List<TaskTrackerStatus> trackers;
    TaskTrackerStatus mockTracker;
    Set<String> uniqueHosts = new HashSet<String>();
    JobTracker manager;

    public MockTaskTrackerManager(TaskTrackerStatus mockTracker,
                                  JobTracker manager) {

      this.manager = manager;
      this.mockTracker = mockTracker;

      trackers = new ArrayList<TaskTrackerStatus>();
      trackers.add(mockTracker);
      uniqueHosts.add(mockTracker.getHost());

      for (TaskTrackerStatus tt : manager.taskTrackers()) {
        trackers.add(tt);
        uniqueHosts.add(tt.getHost());
      }
    }

    public Collection<TaskTrackerStatus> taskTrackers() {
      return trackers;
    }

    public int getNumberOfUniqueHosts() {
      return uniqueHosts.size();
    }

    public ClusterStatus getClusterStatus() {
      ClusterStatus originalStatus = manager.getClusterStatus();

      return new ClusterStatus(trackers.size() -
            manager.getBlacklistedTrackerCount(),
            manager.getBlacklistedTrackerCount(),
            originalStatus.getTTExpiryInterval(),
            originalStatus.getMapTasks(),
            originalStatus.getReduceTasks(),
            originalStatus.getMaxMapTasks() + mockTracker.getMaxMapSlots(),
            originalStatus.getMaxReduceTasks() + mockTracker.getMaxReduceSlots(),
            originalStatus.getJobTrackerStatus());
    }

    public QueueManager getQueueManager() {
      return manager.getQueueManager();
    }

    public int getNextHeartbeatInterval() {
      throw new RuntimeException("NOPE");
    }

    public void addJobInProgressListener(JobInProgressListener listener) {
      throw new RuntimeException("NOPE");
    }

    public void removeJobInProgressListener(JobInProgressListener listener) {
      throw new RuntimeException("NOPE");
    }

    public void killJob(JobID jobid)
        throws IOException {
      throw new RuntimeException("NOPE");
    }

    public JobInProgress getJob(JobID jobid) {
      throw new RuntimeException("NOPE");
    }

    public void initJob(JobInProgress job) {
      throw new RuntimeException("NOPE");
    }

    public void failJob(JobInProgress job) {
      throw new RuntimeException("NOPE");
    }

    public boolean killTask(TaskAttemptID taskid, boolean shouldFail)
        throws IOException {
      throw new RuntimeException("NOPE");
    }
  }

  public ResourcePolicyAssigned(MesosScheduler scheduler) throws NoSuchMethodException {
    super(scheduler);

    canLaunchSetupTask = JobInProgress.class.getDeclaredMethod("canLaunchSetupTask");
    canLaunchSetupTask.setAccessible(true);

    canLaunchCleanupTask = JobInProgress.class.getDeclaredMethod("canLaunchJobCleanupTask");
    canLaunchCleanupTask.setAccessible(true);
  }

  /**
   * Computes the number of slots to launch for this offer
   *
   * @return true if the offer is sufficient
   */
  @Override
  public boolean computeSlots() {

    mapSlots = 0;
    reduceSlots = 0;

    // Determine how many slots we are able to allocate in total from
    // this offer.
    int slots = mapSlotsMax + reduceSlotsMax;
    slots = (int) Math.min(slots, (cpus - containerCpus) / slotCpus);
    slots = (int) Math.min(slots, (mem - containerMem) / slotMem);
    slots = (int) Math.min(slots, (disk - containerDisk) / slotDisk);

    // Is this offer too small for even the minimum slots?
    if (slots < 1) {
      return false;
    }

    // Construct a fake TaskTracker object to trick the scheduler
    TaskTrackerStatus mockStatus = new TaskTrackerStatus(
      "mesos.scheduler.facade", "http", "foo.bar.baz", 0, new ArrayList<TaskStatus>(), 0, 0, slots, slots
    );

    // TODO: Comment this properly
    JobTracker jobTracker = scheduler.getJobTracker();
    MockTaskTrackerManager mockManager = new MockTaskTrackerManager(mockStatus, jobTracker);

    TaskScheduler taskScheduler = scheduler.getTaskScheduler();
    taskScheduler.setTaskTrackerManager(mockManager);

    // Ask the scheduler what it would assign to these slots
    long assignedMaps = 0;
    long assignedReduces = 0;

    try {
      // Look for any jobs that are in the PREP phase and
      for (JobStatus status : scheduler.jobTracker.jobsToComplete()) {
        JobInProgress job = scheduler.jobTracker.getJob(status.getJobID());
        if ((boolean) canLaunchSetupTask.invoke(job) || (boolean) canLaunchCleanupTask.invoke(job)) {
          assignedMaps ++;
        }
      }

      TaskTracker taskTracker = new TaskTracker("mesos.scheduler.facade");
      taskTracker.setStatus(mockStatus);

      List<Task> assignedTasks = taskScheduler.assignTasks(taskTracker);
      if (assignedTasks != null) {
        for (Task task : assignedTasks) {
          if (task.isMapOrReduce()) {
            if (!task.isMapTask()) {
              assignedReduces++;
              continue;
            }
          }
          assignedMaps++;
        }
      }
    } catch (Exception e) {
      LOG.warn("Caught exception mocking assignTasks(): ", e);
      assignedMaps = 0;
      assignedReduces = 0;
    } finally {
      taskScheduler.setTaskTrackerManager(jobTracker);
    }

    if ((assignedMaps + assignedReduces) <= 0) {
      LOG.info("Scheduler assigned zero tasks (offered " + slots + " MAP and " + slots + " REDUCE)");
      return false;
    }

    LOG.info("Scheduler assigned " + assignedMaps + " MAP tasks and " + assignedReduces + " REDUCE tasks");

    // Is the number of slots we need sufficiently small? If so, we can
    // allocate exactly the number we need.
    if (slots >= assignedMaps + assignedReduces) {
      mapSlots = Math.min(assignedMaps, mapSlotsMax);
      reduceSlots = Math.min(assignedReduces, reduceSlotsMax);
    } else {
      // Allocate slots fairly for this resource offer.
      double mapFactor = (double) assignedMaps / (assignedMaps + assignedReduces);

      // To avoid map/reduce slot starvation, don't allow more than 50%
      // spread between map/reduce slots when we need both mappers and
      // reducers.
      if (assignedMaps > 0 && assignedReduces > 0) {
        if (mapFactor < 0.25) {
          mapFactor = 0.25;
        } else if (mapFactor > 0.75) {
          mapFactor = 0.75;
        }
      }

      mapSlots = Math.min(Math.min(Math.max(Math.round(mapFactor * slots), 1), mapSlotsMax), assignedMaps);

      // The remaining slots are allocated for reduces.
      slots -= mapSlots;
      reduceSlots = Math.min(Math.min(slots, reduceSlotsMax), assignedReduces);
    }

    return true;
  }
}
