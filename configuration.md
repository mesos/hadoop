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
  <!-- Basic properties -->
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
    <description>This is the amount of CPU share allocated per slot. This number may be fractional (i.e., 0.5).</description>
  </property>
  <property>
    <name>mapred.mesos.slot.disk</name>
    <value>1024</value>
    <description>This is the disk space required per slot. The value is in
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

  <!-- Resource policies -->
  <property>
    <name>mapred.mesos.total.map.slots.minimum</name>
    <value>0</value>
    <description>
      Mesos will attempt to make at least this many number of map slots
      available at a given time. This does not necessarily mean the slots will
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
      specification. If a resource offer is not large enough for the number of
      slots specified, that resource offer will be declined.
    </description>
  </property>

  <!-- Additional Mesos parameters -->
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
      This is the Mesos framework role. This can be used in conjunction with
      Mesos reservations. Consult the Mesos documentation for details.
    </description>
  </property>
  <property>
    <name>mapred.mesos.role.strict</name>
    <value>false</value>
    <description>
      Force the framework to only ever accept resource offers that are of the
      role configured in "mapred.mesos.role".
    </description>
  </property>

  <!-- If you're using Mesos Native Docker -->
  <property>
    <name>mapred.mesos.docker.image</name>
    <value>my-registry.com/image/foo:tag</value>
    <description>
      If you want the TaskTracker executor processes to start inside Docker containers,
      specify the docker image here.
    </description>
  </property>
  <property>
    <name>mapred.mesos.docker.network</name>
    <value>1</value>
    <description>
      Use this option to change the networking configuration for containers. The
      default here is to use HOST networking (the container shares the network)
      with the host, no isolation.
      1 = HOST, 2 = BRIDGE, 3 = NONE;
    </description>
  </property>
  <property>
    <name>mapred.mesos.docker.privileged</name>
    <value>false</value>
    <description>
      Enable the --privileged option on the executor containers.
    </description>
  </property>
  <property>
    <name>mapred.mesos.docker.force_pull_image</name>
    <value>false</value>
    <description>
      Tell the mesos slave to always check it has the latest version of the container
      image before starting the container.
    </description>
  </property>
  <property>
    <name>mapred.mesos.docker.parameters</name>
    <value></value>
    <description>
      Comma separated list of command line arguments to pass directly to the
      docker run invocation. For example...

      "env,FOO=bar,env,BAZ=test"
    </description>
  </property>
  <property>
    <name>mapred.mesos.docker.volumes</name>
    <value></value>
    <description>
      Comma separated list of volumes to mount into the container. The format for
      the volume definition is similar to the docker CLI, for example...

      "/host/path:/container/path:rw" (mount /host/path to /container/path read-write)
      "/host/path:/container/path:ro" (mount /host/path to /container/path read-only)
      "/host/path:ro" (mount /host/path to /host/path read-only)
    </description>
  </property>

  <!-- If you're using a custom Mesos Containerizer -->
  <property>
    <name>mapred.mesos.container.image</name>
    <value>docker:///ubuntu</value>
    <description>
      If you're using a custom Mesos Containerizer (like the External Containerizer)
      that uses images, you can set this option to cause Hadoop TaskTrackers to
      be launched within this container image.
    </description>
  </property>
  <property>
    <name>mapred.mesos.container.options</name>
    <value></value>
    <description>
      Comma separated list of options to pass to the containerizer. The meaning
      of this entirely depends on the containerizer in use.
    </description>
  </property>

  <!-- TaskTracker Idle Slots Revocation -->
  <property>
    <name>mapred.mesos.tracker.idle.interval</name>
    <value>5</value>
    <description>
      Internal (in seconds) to check for TaskTrackers that have idle
      slots. Default is 5 seconds.
    </description>
  </property>
  <property>
    <name>mapred.mesos.tracker.idle.checks</name>
    <value>5</value>
    <description>
      After this many successful idle checks (meaning all slots *are* idle) the
      slots will be revoked from the TaskTracker.
    </description>
  </property>

  <!-- Metrics -->
  <property>
    <name>mapred.mesos.metrics.enabled</name>
    <value>false</value>
    <description>
      Set this to `true` to enable metric reporting with the Coda Hale Metrics
      library.
    </description>
  </property>

  <!-- Metrics - CSV reporting -->
  <property>
    <name>mapred.mesos.metrics.csv.enabled</name>
    <value>false</value>
    <description>
      Set this to `true` to enable CSV reporting with the Coda Hale Metrics
      library.
    </description>
  </property>
  <property>
    <name>mapred.mesos.metrics.csv.path</name>
    <value>/path/to/metrics/csv/metrics.csv</value>
    <description>
      Set this to a file which will be created with CSV metrics data.
    </description>
    <property>
      <name>mapred.mesos.metrics.csv.interval</name>
      <value>60</value>
    </property>
  </property>

  <!-- Metrics - Graphite reporting -->
  <property>
    <name>mapred.mesos.metrics.graphite.enabled</name>
    <value>false</value>
    <description>
      Set this to `true` to enable Graphite reporting with the Coda Hale Metrics
      library.
    </description>
  </property>
  <property>
    <name>mapred.mesos.metrics.graphite.host</name>
    <value>graphite.host.name</value>
  </property>
  <property>
    <name>mapred.mesos.metrics.graphite.port</name>
    <value>2003</value>
  </property>
  <property>
    <name>mapred.mesos.metrics.graphite.prefix</name>
    <value>prefix</value>
  </property>
  <property>
    <name>mapred.mesos.metrics.graphite.interval</name>
    <value>60</value>
  </property>

  <!-- Metrics - Cassandra reporting -->
  <property>
    <name>mapred.mesos.metrics.cassandra.enabled</name>
    <value>false</value>
    <description>
      Set this to `true` to enable Cassandra reporting with the Coda Hale
      Metrics library.
    </description>
  </property>
  <property>
    <name>mapred.mesos.metrics.cassandra.hosts</name>
    <value>localhost</value>
  </property>
  <property>
    <name>mapred.mesos.metrics.cassandra.port</name>
    <value>9042</value>
  </property>
  <property>
    <name>mapred.mesos.metrics.cassandra.interval</name>
    <value>60</value>
  </property>
  <property>
    <name>mapred.mesos.metrics.cassandra.prefix</name>
    <value>prefix</value>
  </property>
  <property>
    <name>mapred.mesos.metrics.cassandra.ttl</name>
    <value>864000</value>
  </property>
  <property>
    <name>mapred.mesos.metrics.cassandra.keyspace</name>
    <value>metrics</value>
  </property>
  <property>
    <name>mapred.mesos.metrics.cassandra.table</name>
    <value>metrics</value>
  </property>
  <property>
    <name>mapred.mesos.metrics.cassandra.consistency</name>
    <value>QUORUM</value>
  </property>
</configuration>
```
