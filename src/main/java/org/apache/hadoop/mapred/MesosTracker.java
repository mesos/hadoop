package org.apache.hadoop.mapred;

import org.apache.commons.httpclient.HttpHost;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos.TaskID;

import java.util.*;

/**
 * Used to track the our launched TaskTrackers.
 */
public class MesosTracker {

  public static final Log LOG = LogFactory.getLog(MesosScheduler.class);
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

          // When the scheduler is busy or doesn't receive offers, it may
          // fail to mark some TaskTrackers as active even though they are.
          // Here we do a final check with the JobTracker to make sure this
          // TaskTracker is really not there before we kill it.
          final Collection<TaskTrackerStatus> taskTrackers =
              MesosTracker.this.scheduler.jobTracker.taskTrackers();

          for (TaskTrackerStatus status : taskTrackers) {
            HttpHost host = new HttpHost(status.getHost(), status.getHttpPort());
            if (MesosTracker.this.host.equals(host)) {
              return;
            }
          }

          LOG.warn("Tracker " + MesosTracker.this.host + " failed to launch within " +
              MesosScheduler.LAUNCH_TIMEOUT_MS / 1000 + " seconds, killing it");
          MesosTracker.this.scheduler.killTracker(MesosTracker.this);
        }
      }
    }, MesosScheduler.LAUNCH_TIMEOUT_MS);
  }
}
