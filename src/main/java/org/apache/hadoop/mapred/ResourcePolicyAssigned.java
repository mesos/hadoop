
package org.apache.hadoop.mapred;

import java.util.List;
import java.util.ArrayList;

import java.lang.NoSuchMethodException;
import java.lang.reflect.Method;

import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;

/**
 *
 */
public class ResourcePolicyAssigned extends ResourcePolicy {

  Method canLaunchSetupTask;
  Method canLaunchCleanupTask;

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
    TaskTracker taskTracker = new TaskTracker("mesos.scheduler.facade");
    taskTracker.setStatus(new TaskTrackerStatus(
      "mesos.scheduler.facade", null, null, 0, new ArrayList<TaskStatus>(), 0, 0, slots, slots
    ));

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

      TaskScheduler taskScheduler = scheduler.getTaskScheduler();
      if (taskScheduler != null) {
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
      }
    } catch (Exception e) {
      LOG.warn("Caught exception mocking assignTasks(): ", e);
      return false;
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
