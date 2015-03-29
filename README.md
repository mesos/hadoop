Hadoop on Mesos
---------------

[![Build Status](https://travis-ci.org/mesos/hadoop.svg?branch=master)](https://travis-ci.org/mesos/hadoop)

#### Overview ####

To run _Hadoop on Mesos_ you need to add the `hadoop-mesos-0.1.0.jar`
library to your Hadoop distribution (any distribution that uses protobuf > 2.5.0)
and set some new configuration properties. Read on for details.

The `pom.xml` included is configured and tested against CDH5 and MRv1. Hadoop on
Mesos does not currently support YARN (and MRv2).

#### Prerequisites ####

To use the metrics feature (which uses the [CodaHale Metrics][CodaHale Metrics] library), you need to
install `libsnappy`.  The [`snappy-java`][snappy-java] package also includes a bundled version of
`libsnappyjava`.

[CodaHale Metrics]: http://metrics.codahale.com/
[snappy-java]: https://github.com/xerial/snappy-java

#### Build ####

You can build `hadoop-mesos-0.1.0.jar` using Maven:

```shell
mvn package
```

If successful, the JAR will be at `target/hadoop-mesos-0.1.0.jar`.

> NOTE: If you want to build against a different version of Mesos than
> the default you'll need to update `mesos-version` in `pom.xml`.

We plan to provide already built JARs at http://repository.apache.org
in the near future!

#### Package ####

You'll need to download an existing Hadoop distribution. For this
guide, we'll use [CDH5][CDH5.1.3]. First grab the tar archive and
extract it.

```shell
wget http://archive.cloudera.com/cdh5/cdh/5/hadoop-2.5.0-cdh5.2.0.tar.gz
...
tar zxf hadoop-2.5.0-cdh5.2.0.tar.gz
```

> **Take note**, the extracted directory is `hadoop-2.5.0-cdh5.2.0`.

Now copy `hadoop-mesos-0.1.0.jar` into the `share/hadoop/common/lib` folder.

```shell
cp /path/to/hadoop-mesos-0.1.0.jar hadoop-2.5.0-cdh5.2.0/share/hadoop/common/lib/
```

Since CDH5 includes both MRv1 and MRv2 (YARN) and is configured for YARN by
default, we need update the symlinks to point to the correct directories.

```shell
cd hadoop-2.5.0-cdh5.2.0

mv bin bin-mapreduce2
mv examples examples-mapreduce2 
ln -s bin-mapreduce1 bin
ln -s examples-mapreduce1 examples

pushd etc
mv hadoop hadoop-mapreduce2
ln -s hadoop-mapreduce1 hadoop
popd

pushd share/hadoop
rm mapreduce
ln -s mapreduce1 mapreduce
popd
```

_That's it!_ You now have a _Hadoop on Mesos_ distribution!

[CDH5.1.3]: http://www.cloudera.com/content/support/en/documentation/cdh5-documentation/cdh5-documentation-v5-latest.html

#### Upload ####

You'll want to upload your _Hadoop on Mesos_ distribution somewhere
that Mesos can access in order to launch each `TaskTracker`. For
example, if you're already running HDFS:

```
$ tar czf hadoop-2.5.0-cdh5.2.0.tar.gz hadoop-2.5.0-cdh5.2.0
$ hadoop fs -put hadoop-2.5.0-cdh5.2.0.tar.gz /hadoop-2.5.0-cdh5.2.0.tar.gz
```

> **Consider** any permissions issues with your uploaded location
> (i.e., on HDFS you'll probably want to make the file world
> readable).

Now you'll need to configure your `JobTracker` to launch each
`TaskTracker` on Mesos!

#### Configure ####

Along with the normal configuration properties you might want to set
to launch a `JobTracker`, you'll need to set some Mesos specific ones
too.

Here are the mandatory configuration properties for
`conf/mapred-site.xml` (initialized to values representative of
running in [pseudo distributed
operation](http://hadoop.apache.org/docs/stable/single_node_setup.html#PseudoDistributed):

```
<property>
  <name>mapred.job.tracker</name>
  <value>localhost:9001</value>
</property>
<property>
  <name>mapred.jobtracker.taskScheduler</name>
  <value>org.apache.hadoop.mapred.MesosScheduler</value>
</property>
<property>
  <name>mapred.mesos.taskScheduler</name>
  <value>org.apache.hadoop.mapred.JobQueueTaskScheduler</value>
</property>
<property>
  <name>mapred.mesos.master</name>
  <value>localhost:5050</value>
</property>
<property>
  <name>mapred.mesos.executor.uri</name>
  <value>hdfs://localhost:9000/hadoop-2.5.0-cdh5.2.0.tar.gz</value>
</property>
```

[More details on configuration propertios can be found here.](configuration.md)

#### Start ####

Now you can start the `JobTracker` but you'll need to include the path
to the Mesos native library.

On Linux:

```
$ MESOS_NATIVE_LIBRARY=/path/to/libmesos.so hadoop jobtracker
```

And on OS X:

```
$ MESOS_NATIVE_LIBRARY=/path/to/libmesos.dylib hadoop jobtracker
```

> **NOTE: You do not need to worry about distributing your Hadoop
> configuration! All of the configuration properties read by the**
> `JobTracker` **along with any necessary** `TaskTracker` **specific
> _overrides_ will get serialized and passed to each** `TaskTracker`
> **on startup.**

#### Containers ####

As of Mesos 0.19.0 you can now specify a container to be used when isolating a task on a Mesos Slave. If you're making use of this new container mechanism, you can configure the hadoop jobtracker to send a custom container image and set of options using two new JobConf options.

This is purely opt-in, so omitting these jobconf options will cause no `ContainerInfo` to be sent to Mesos. Also, if you don't use these options there's no requirement to use version 0.19.0 of the mesos native library.

This feature can be especially useful if your hadoop jobs have software dependencies on the slaves themselves, as using a container can isolate these dependencies between other users of a Mesos cluster.

*It's important to note that the container/image you use does need to have the mesos native library installed already.*

```
<property>
  <name>mapred.mesos.container.image</name>
  <value>docker:///ubuntu</value>
</property>
<property>
  <name>mapred.mesos.container.options</name>
  <value>-v,/foo/bar:/bar</value>
</property>
```

_Please email user@mesos.apache.org with questions!_

----------
