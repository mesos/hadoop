Configuration
---------------

Mesos introduces some new parameters for configuring your Hadoop cluster.
Below is a sample configuration with a description of the fields and their
default values.

```
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
  <property>
    <name>mapred.job.tracker</name>
    <value>localhost:54311</value>
  </property>
  <property>
    <name>mapred.jobtracker.taskScheduler</name>
    <value>org.apache.hadoop.mapred.MesosScheduler</value>
  </property>
  <property>
    <name>mapred.mesos.taskScheduler</name>
    <value>org.apache.hadoop.mapred.JobQueueTaskScheduler</value>
    <description>
      This is the underlying task scheduler for the jobtracker. You may use
      other schedulers, like org.apache.hadoop.mapred.FairScheduler.
    </description>
  </property>
  <property>
    <name>mapred.mesos.master</name>
    <value>local</value>
    <description>
      This is the address of the Mesos master instance. If you're using
      Zookeeper for master election, use the Zookeeper address here (i.e.,
      zk://zk.apache.org:2181/hadoop/mesos).
    </description>
  </property>
  <property>
    <name>mapred.mesos.executor.uri</name>
    <value>hdfs://hdfs.name.node:port/hadoop.tar.gz</value>
    <description>
      This is the URI of the Hadoop on Mesos distribution.
      NOTE: You need to MANUALLY upload this yourself!
    </description>
  </property>
  <!-- The properties below indicate the amount of resources that are allocated
    to a Hadoop slot (i.e., map/reduce task) by Mesos. -->
  <property>
    <name>mapred.mesos.slot.cpus</name>
    <value>1</value>
    <description>This is the amount of CPU share allocated per slot.  This number may be fractional (i.e., 0.5).</description>
  </property>
  <property>
    <name>mapred.mesos.slot.disk</name>
    <value>1024</value>
    <description>This is the disk space required per slot.  The value is in
      MiB.</description>
  </property>
  <property>
    <name>mapred.mesos.slot.mem</name>
    <value>1024</value>
    <description>
      This is the total memory required for JVM overhead (10% of this value)
      and the heap (-Xmx) of the task. The value is in MiB.
    </description>
  </property>
  <property>
    <name>mapred.mesos.total.map.slots.minimum</name>
    <value>0</value>
    <description>
      Mesos will attempt to make at least this many number of map slots
      available at a given time.  This does not necessarily mean the slots will
      be idle, and this does not guarantee these slots will be available.
    </description>
  </property>
  <property>
    <name>mapred.mesos.total.reduce.slots.minimum</name>
    <value>0</value>
    <description>
      Mesos will attempt to make at least this many number of reduce slots
      available at a given time. This does not necessarily mean the slots will
      be idle, and this does not guarantee these slots will be available.
    </description>
  </property>
  <property>
    <name>mapred.tasktracker.map.tasks.maximum</name>
    <value>50</value>
    <description>
      This is the maximum number of tasks per task tracker. If you use the
      fixed resource policy, Mesos will always allocate this many slots per
      task tracker.
    </description>
  </property>
  <property>
    <name>mapred.tasktracker.reduce.tasks.maximum</name>
    <value>50</value>
    <description>
      This is the maximum number of tasks per task tracker. If you use the
      fixed resource policy, Mesos will always allocate this many slots per
      task tracker.
    </description>
  </property>
  <property>
    <name>mapred.mesos.scheduler.policy.fixed</name>
    <value>false</value>
    <description>
      If this is set to true, Mesos will always allocate a fixed number of
      slots per task tracker based on the maximum map/reduce slot
      specification.  If a resource offer is not large enough for the number of
      slots specified, that resource offer will be declined.
    </description>
  </property>
  <property>
    <name>mapred.mesos.checkpoint</name>
    <value>false</value>
    <description>
      This value enables/disables checkpointing for this framework.
    </description>
  </property>
  <property>
    <name>mapred.mesos.role</name>
    <value>*</value>
    <description>
      This is the Mesos framework role.  This can be used in conjunction with
      Mesos reservations.  Consult the Mesos documentation for details.
    </description>
  </property>
</configuration>
```
