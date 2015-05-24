package org.apache.hadoop.mapred;

import org.apache.commons.httpclient.HttpHost;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
  public TaskID taskId;
  public long mapSlots;
  public long reduceSlots;
  public volatile long idleCounter = 0;
  public volatile long idleCheckInterval = 0;
  public volatile long idleCheckMax = 0;
  public volatile boolean active = false; // Set once tracked by the JobTracker.
  public volatile boolean stopped = false;
  public volatile MesosScheduler scheduler;
  // Tracks Hadoop jobs running on the tracker.
  public Set<JobID> jobs = Collections.newSetFromMap(new ConcurrentHashMap<JobID, Boolean>());
  public com.codahale.metrics.Timer.Context context;

  public MesosTracker(HttpHost host, TaskID taskId, long mapSlots,
                      long reduceSlots, MesosScheduler scheduler) {
    this.host = host;
    this.taskId = taskId;
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
        final Collection<TaskTrackerStatus> taskTrackers = MesosTracker.this.scheduler.jobTracker.taskTrackers();

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
        synchronized (MesosTracker.this.scheduler) {

          // Stop the idle check timer if the scheduler has been stopped.
          if (MesosTracker.this.stopped) {
            return;
          }

          // If the task tracker isn't active, wait until it is active.
          // If the task tracker has no jobs assigned to it, ignore it. We're
          // only interested in a tracker that has jobs but isn't using any of
          // the slots.
          if (!MesosTracker.this.active || MesosTracker.this.jobs.isEmpty()) {
            scheduleIdleCheck();
            return;
          }

          // If the tracker has been idle for too long, kill it.
          if (MesosTracker.this.idleCounter >= MesosTracker.this.idleCheckMax) {
            LOG.info("Killing idle tasktracker " + MesosTracker.this.host);
            MesosTracker.this.scheduler.killTracker(MesosTracker.this);
            scheduleIdleCheck();
            return;
          }

          // Calculate the number of map and reduce slots that are currently
          // occupied on the task tracker.
          long occupiedMapSlots =  0;
          long occupiedReduceSlots = 0;
          Collection<TaskTrackerStatus> taskTrackers = scheduler.jobTracker.taskTrackers();
          for (TaskTrackerStatus status : taskTrackers) {
            HttpHost host = new HttpHost(status.getHost(), status.getHttpPort());
            if (host.toString().equals(MesosTracker.this.host.toString())) {
              occupiedMapSlots += status.countOccupiedMapSlots();
              occupiedReduceSlots += status.countOccupiedMapSlots();
              break;
            }
          }

          // If there are zero slots occupied (either map OR reduce slots) then
          // we class the tracker as idle.
          if (occupiedMapSlots == 0 && occupiedReduceSlots == 0) {
            LOG.info("TaskTracker appears idle right now: " + MesosTracker.this.host);
            MesosTracker.this.idleCounter += 1;
          } else {
            if (MesosTracker.this.idleCounter > 0) {
              LOG.info("TaskTracker is no longer idle: " + MesosTracker.this.host);
            }
            MesosTracker.this.idleCounter = 0;
          }

          scheduleIdleCheck();
        }
      }
    }, MesosTracker.this.idleCheckInterval, TimeUnit.SECONDS);
  }

  public void stop() {
    active = false;
    stopped = true;
    if (context != null) {
      context.stop();
    }
  }
}
