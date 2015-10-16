package org.apache.hadoop.mapred;

import org.apache.commons.httpclient.HttpHost;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.mesos.Protos.TaskID;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Used to track the our launched TaskTrackers.
 */
public class MesosTracker {
  public static final Log LOG = LogFactory.getLog(MesosScheduler.class);
  public volatile HttpHost host;
  public TaskID mapTaskId;
  public TaskID reduceTaskId;
  public long mapSlots;
  public long reduceSlots;
  // Number of idle check cycles all map slots are idle
  public volatile long idleMapCounter = 0;
  // Number of idle check cycles all reduce slots are idle
  public volatile long idleReduceCounter = 0;
  public volatile long idleCheckInterval = 0;
  public volatile long idleCheckMax = 0;
  public volatile boolean active = false; // Set once tracked by the JobTracker.
  public volatile MesosScheduler scheduler;
  // Tracks Hadoop jobs running on the tracker.
  public Set<JobID> jobs = Collections.newSetFromMap(new ConcurrentHashMap<JobID, Boolean>());
  public com.codahale.metrics.Timer.Context context;

  public MesosTracker(HttpHost host, TaskID mapTaskId, TaskID reduceTaskId,
                      long mapSlots, long reduceSlots, MesosScheduler scheduler) {
    this.host = host;
    this.mapTaskId = mapTaskId;
    this.reduceTaskId = reduceTaskId;
    this.mapSlots = mapSlots;
    this.reduceSlots = reduceSlots;
    this.scheduler = scheduler;

    if (scheduler.metrics != null) {
      this.context = scheduler.metrics.trackerTimer.time();
    }

    this.idleCheckInterval = scheduler.conf.getLong("mapred.mesos.tracker.idle.interval",
                                MesosScheduler.DEFAULT_IDLE_CHECK_INTERVAL);
    this.idleCheckMax = scheduler.conf.getLong("mapred.mesos.tracker.idle.checks",
                            MesosScheduler.DEFAULT_IDLE_REVOCATION_CHECKS);

    scheduleStartupTimer();
    if (this.idleCheckInterval > 0 && this.idleCheckMax > 0) {
      scheduleIdleCheck();
    }
  }

  public TaskID getTaskId(TaskType type) {
    if (type == TaskType.MAP) {
      return mapTaskId;
    } else if (type == TaskType.REDUCE) {
      return reduceTaskId;
    }

    return null;
  }

  protected void scheduleStartupTimer() {
    scheduler.scheduleTimer(new Runnable() {
      @Override
      public void run() {
        if (MesosTracker.this.active) {
          // If the tracker activated while we were awaiting to acquire the
          // lock, start the periodic cleanup timer and return.
          schedulePeriodic();
          return;
        }

        // When the scheduler is busy or doesn't receive offers, it may
        // fail to mark some TaskTrackers as active even though they are.
        // Here we do a final check with the JobTracker to make sure this
        // TaskTracker is really not there before we kill it.
        final Collection<TaskTrackerStatus> taskTrackers =
          MesosTracker.this.scheduler.jobTracker.taskTrackers();

        for (TaskTrackerStatus status : taskTrackers) {
          HttpHost host = new HttpHost(status.getHost(), status.getHttpPort());
          if (status.getHealthStatus().isNodeHealthy() && MesosTracker.this.host.equals(host)) {
            schedulePeriodic();
            return;
          }
        }

        if (MesosTracker.this.scheduler.metrics != null) {
          MesosTracker.this.scheduler.metrics.launchTimeout.mark();
        }

        LOG.warn("Tracker " + MesosTracker.this.host + " failed to launch within " +
            MesosScheduler.LAUNCH_TIMEOUT_MS / 1000 + " seconds, killing it");

        // Kill the MAP and REDUCE slot tasks. This doesn't directly kill the
        // task tracker but it will result in the task tracker receiving no
        // tasks and ultimately lead to it's death. Best case the task is broken
        // and it will never come up on Mesos.
        MesosTracker.this.scheduler.killTracker(MesosTracker.this);
      }
    }, MesosScheduler.LAUNCH_TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  protected void schedulePeriodic() {
    scheduler.scheduleTimer(new Runnable() {
      @Override
      public void run() {
        if (MesosTracker.this.scheduler.mesosTrackers.containsKey(host) &&
            MesosTracker.this == MesosTracker.this.scheduler.mesosTrackers.get(host)) {
          // Periodically check if the jobs assigned to this TaskTracker are
          // still running (lazy GC).
          final Set<JobID> jobsCopy = new HashSet<>(MesosTracker.this.jobs);
          for (JobID id : jobsCopy) {
            JobStatus jobStatus = MesosTracker.this.scheduler.jobTracker.getJobStatus(id);
            if (jobStatus == null || jobStatus.isJobComplete()) {
              if (MesosTracker.this.scheduler.metrics != null) {
                MesosTracker.this.scheduler.metrics.periodicGC.mark();
              }
              MesosTracker.this.jobs.remove(id);
            }
          }
          schedulePeriodic();
        }
      }
    }, MesosScheduler.PERIODIC_MS, TimeUnit.MILLISECONDS);
  }

  protected void scheduleIdleCheck() {
    scheduler.scheduleTimer(new Runnable() {
      @Override
      public void run() {
        // If the task tracker isn't active, wait until it is active.
        // If the task tracker has no jobs assigned to it, ignore it. We're
        // only interested in a tracker that has jobs but isn't using any of
        // the slots.
        if (!MesosTracker.this.active || MesosTracker.this.jobs.isEmpty()) {
          scheduleIdleCheck();
          return;
        }

        // Perform the idle checks for map and reduce slots
        if (MesosTracker.this.mapSlots > 0) {
          idleMapCheck();
        }

        if (MesosTracker.this.reduceSlots > 0) {
          idleReduceCheck();
        }

        scheduleIdleCheck();
      }
    }, MesosTracker.this.idleCheckInterval, TimeUnit.SECONDS);
  }

  protected void idleMapCheck() {

    // If the map slots has been idle for too long, kill them.
    if (this.idleMapCounter >= MesosTracker.this.idleCheckMax) {
      LOG.info("Killing MAP slots on idle Task Tracker " + MesosTracker.this.host);
      MesosTracker.this.scheduler.killTrackerSlots(MesosTracker.this, TaskType.MAP);
      return;
    }

    long occupiedMapSlots =  0;
    Collection<TaskTrackerStatus> taskTrackers = scheduler.jobTracker.taskTrackers();
    for (TaskTrackerStatus status : taskTrackers) {
      HttpHost host = new HttpHost(status.getHost(), status.getHttpPort());
      if (host.toString().equals(MesosTracker.this.host.toString())) {
        occupiedMapSlots += status.countOccupiedMapSlots();
        break;
      }
    }

    if (occupiedMapSlots == 0) {
      LOG.info("TaskTracker MAP slots appear idle right now: " + MesosTracker.this.host);
      MesosTracker.this.idleMapCounter += 1;
    } else {
      MesosTracker.this.idleMapCounter = 0;
    }
  }

  protected void idleReduceCheck() {

    // If the reduce slots has been idle for too long, kill them.
    if (this.idleReduceCounter >= MesosTracker.this.idleCheckMax) {
      LOG.info("Killing REDUCE slots on idle Task Tracker " + MesosTracker.this.host);
      MesosTracker.this.scheduler.killTrackerSlots(MesosTracker.this, TaskType.REDUCE);
      return;
    }

    long occupiedReduceSlots =  0;
    Collection<TaskTrackerStatus> taskTrackers = scheduler.jobTracker.taskTrackers();
    for (TaskTrackerStatus status : taskTrackers) {
      HttpHost host = new HttpHost(status.getHost(), status.getHttpPort());
      if (host.toString().equals(MesosTracker.this.host.toString())) {
        occupiedReduceSlots += status.countOccupiedReduceSlots();
        break;
      }
    }

    if (occupiedReduceSlots == 0) {
      LOG.info("TaskTracker REDUCE slots appear idle right now: " + MesosTracker.this.host);
      MesosTracker.this.idleReduceCounter += 1;
    } else {
      MesosTracker.this.idleReduceCounter = 0;
    }
  }
}
