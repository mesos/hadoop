package org.apache.hadoop.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.*;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskStatus;

import java.io.*;

public class MesosExecutor implements Executor {
  public static final Log LOG = LogFactory.getLog(MesosExecutor.class);
  private SlaveInfo slaveInfo;
  private TaskTracker taskTracker;

  public static void main(String[] args) {
    MesosExecutorDriver driver = new MesosExecutorDriver(new MesosExecutor());
    System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
  }

  private JobConf configure(final TaskInfo task) {
    JobConf conf = new JobConf(false);
    try {
      byte[] bytes = task.getData().toByteArray();
      conf.readFields(new DataInputStream(new ByteArrayInputStream(bytes)));
    } catch (IOException e) {
      LOG.warn("Failed to deserialize configuration.", e);
      System.exit(1);
    }

    // Output the configuration as XML for easy debugging.
    try {
      StringWriter writer = new StringWriter();
      conf.writeXml(writer);
      writer.flush();
      String xml = writer.getBuffer().toString();
      String xmlFormatted =
          org.apache.mesos.hadoop.Utils.formatXml(xml);
      LOG.info("XML Configuration received:\n" +
          xmlFormatted);
    } catch (Exception e) {
      LOG.warn("Failed to output configuration as XML.", e);
    }

    // Get hostname from Mesos to make sure we match what it reports
    // to the JobTracker.
    conf.set("slave.host.name", slaveInfo.getHostname());

    // Set the mapred.local directory inside the executor sandbox, so that
    // different TaskTrackers on the same host do not step on each other.
    conf.set("mapred.local.dir", System.getenv("MESOS_DIRECTORY") + "/mapred");

    return conf;
  }

  @Override
  public void registered(ExecutorDriver driver, ExecutorInfo executorInfo,
                         FrameworkInfo frameworkInfo, SlaveInfo slaveInfo) {
    LOG.info("Executor registered with the slave");
    this.slaveInfo = slaveInfo;
  }

  @Override
  public void launchTask(final ExecutorDriver driver, final TaskInfo task) {
    LOG.info("Launching task : " + task.getTaskId().getValue());

    // Get configuration from task data (prepared by the JobTracker).
    JobConf conf = configure(task);

    // NOTE: We need to manually set the context class loader here because,
    // the TaskTracker is unable to find LoginModule class otherwise.
    Thread.currentThread().setContextClassLoader(
        TaskTracker.class.getClassLoader());

    try {
      taskTracker = new TaskTracker(conf);
    } catch (IOException e) {
      LOG.fatal("Failed to start TaskTracker", e);
      System.exit(1);
    } catch (InterruptedException e) {
      LOG.fatal("Failed to start TaskTracker", e);
      System.exit(1);
    }

    // Spin up a TaskTracker in a new thread.
    new Thread("TaskTracker Run Thread") {
      @Override
      public void run() {
        try {
          taskTracker.run();

          // Send a TASK_FINISHED status update.
          // We do this here because we want to send it in a separate thread
          // than was used to call killTask().
          driver.sendStatusUpdate(TaskStatus.newBuilder()
              .setTaskId(task.getTaskId())
              .setState(TaskState.TASK_FINISHED)
              .build());

          // Give some time for the update to reach the slave.
          try {
            Thread.sleep(2000);
          } catch (InterruptedException e) {
            LOG.error("Failed to sleep TaskTracker thread", e);
          }

          // Stop the executor.
          driver.stop();
        } catch (Throwable t) {
          LOG.error("Caught exception, committing suicide.", t);
          driver.stop();
          System.exit(1);
        }
      }
    }.start();

    driver.sendStatusUpdate(TaskStatus.newBuilder()
        .setTaskId(task.getTaskId())
        .setState(TaskState.TASK_RUNNING).build());
  }

  @Override
  public void killTask(ExecutorDriver driver, TaskID taskId) {
    LOG.info("Killing task : " + taskId.getValue());
    try {
      taskTracker.shutdown();
    } catch (IOException e) {
      LOG.error("Failed to shutdown TaskTracker", e);
    } catch (InterruptedException e) {
      LOG.error("Failed to shutdown TaskTracker", e);
    }
  }

  @Override
  public void reregistered(ExecutorDriver driver, SlaveInfo slaveInfo) {
    LOG.info("Executor reregistered with the slave");
  }

  @Override
  public void disconnected(ExecutorDriver driver) {
    LOG.info("Executor disconnected from the slave");
  }

  @Override
  public void frameworkMessage(ExecutorDriver d, byte[] msg) {
    LOG.info("Executor received framework message of length: " + msg.length
        + " bytes");
  }

  @Override
  public void error(ExecutorDriver d, String message) {
    LOG.error("MesosExecutor.error: " + message);
  }

  @Override
  public void shutdown(ExecutorDriver d) {
    LOG.info("Executor asked to shutdown");
  }
}
