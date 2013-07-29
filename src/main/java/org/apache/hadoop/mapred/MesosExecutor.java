package org.apache.hadoop.mapred;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.Environment.Variable;
import org.apache.mesos.Protos.*;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.hadoop.Constants;

import javax.xml.transform.TransformerException;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class MesosExecutor implements Executor {
  public static final Log LOG = LogFactory.getLog(MesosExecutor.class);
  private JobConf conf;
  private TaskTracker taskTracker;

  public static void main(String[] args) {
    MesosExecutorDriver driver = new MesosExecutorDriver(new MesosExecutor());
    System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
  }

  @Override
  public void registered(ExecutorDriver driver, ExecutorInfo executorInfo,
                         FrameworkInfo frameworkInfo, SlaveInfo slaveInfo) {
    LOG.info("Executor registered with the slave");

    Configuration c = new Configuration(false);
    conf = new JobConf();
    // Get TaskTracker's config options from environment variables set by the
    // JobTracker.
    if (executorInfo.getCommand().hasEnvironment()) {
      for (Variable variable : executorInfo.getCommand().getEnvironment()
          .getVariablesList()) {
        if (variable.getName().equals(Constants.HADOOP_MESOS_CONF_STRING)) {
          LOG.info("CONVERTED: " + variable.getValue());
          try {
            byte[] base64 = Base64.decodeBase64(variable.getValue());
            c.readFields(new DataInputStream(new ByteArrayInputStream(base64)));
          } catch (IOException e) {
            e.printStackTrace();
          }

          for (Map.Entry<String, String> entry : c) {
            LOG.info("Setting: " + entry.getKey() + " -> " + entry.getValue());
            conf.set(entry.getKey(), entry.getValue());
          }

          try {
            StringWriter sw = new StringWriter();
            String xmlString = null;
            conf.writeXml(sw);
            sw.flush();
            xmlString = sw.getBuffer().toString();
            LOG.info("XML Configuration received:\n" +
                org.apache.mesos.hadoop.Utils.formatXml(xmlString));
          } catch (TransformerException e) {
            LOG.fatal("Error, received invalid XML:\n"
                + variable.getValue(), e);
            System.exit(1);
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    }

    // Get hostname from Mesos to make sure we match what it reports
    // to the JobTracker.
    conf.set("slave.host.name", slaveInfo.getHostname());

    // Set the mapred.local directory inside the executor sandbox, so that
    // different TaskTrackers on the same host do not step on each other.
    conf.set("mapred.local.dir", System.getProperty("user.dir") + "/mapred");
  }

  @Override
  public void launchTask(final ExecutorDriver driver, final TaskInfo task) {
    LOG.info("Launching task : " + task.getTaskId().getValue());

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
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          LOG.error("Failed to sleep TaskTracker thread", e);
        }

        // Stop the executor.
        driver.stop();
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