Hadoop on Mesos
---------------

### Download ###

You can grab distributions of Hadoop on Mesos from
[releases](https://github.com/mesos/hadoop/releases). Note that each
distribution has a designated "release" (the suffix '-RELEASE') and
you'll likely want to grab the latest one.

### Compile ###

You can compile the distribution with 'ant'. For example, for CDH
4.2.1 (mr1) you can do:

        $ ant -Dreactor.repo=file://$HOME/.m2/repository \
          -Dversion=2.0.0-mr1-cdh4.2.1 -Dcompile.c++=true compile bin-package

For '0.20.205.0' (and likewise CDH3 just replace the 'version'):

        $ ant -Dversion=0.20.205.0 compile bin-package

### Configure ###

Each distribution already has some default configuration properties
set in 'conf/mapred-site.xml', but you'll need to update a few others:

> mapred.job.tracker: ... see Hadoop documenation.

> mapred.mesos.master: IP:PORT of Mesos master.

> mapred.mesos.executor: URI of _this_ package which we'll archive and
  upload below.

### Repackage ###

If you're using Mesos 0.13.x or eariler you'll need to copy the Mesos
native library (e.g., libmesos-VERSION.so) into 'lib/native/PLATFORM'
where PLATFORM should be the platform that you plan on running your
JobTrackers and/or TaskTrackers (so if you're launching a JobTracker
on OS X but launching TaskTrackers through Mesos on Linux you'll need
both the .dylib and .so native libraries).

Now archive using 'tar' and/or 'gzip', for example:

        $ tar czf hadoop-DISTRIBUTION-mesos-VERSION-RELEASE.tar.gz \
        hadoop-DISTRIBUTION-mesos-VERSION-RELEASE

### Upload ###

Okay! Now upload your package to the location you used above for
'mapred.mesos.executor'. You must use a URI which Mesos understands,
such as HDFS. Also, consider any permissions issues (i.e., on HDFS
you'll probably want to make the file world readable).

### Launch! ###

Now launch a JobTracker as normal and start running jobs!

_Please email user@mesos.apache.org with questions!_

----------

#### Want to create your own Hadoop on Mesos distribution? ####

You can fork this repository and use the Makefile to create your own
distribution. You'll need to add a Makefile target to download the
base distribution (e.g., hadoop-0.20.2-cdh3u3) as well as create a
DISTRIBUTION.patch file in the 'mesos' directory. If the distribution
will require any special building instructions you can add another
'ant-' rule in the Makefile as well (see ant-2.0.0-mr1-cdh%). The
Makefile expects a few arguments in the form of environment variables,
these include:

> RELEASE: the release version of this Hadoop on Mesos distribution.

> MESOS_JAR: path to the mesos-VERSION.jar that you want to include in
  this distribution.

> PROTOBUF_JAR: path to the protobuf-VERSION.jar (you'll want to
  confirm the version with the dependency in the Mesos POM file).

Here's an example invocation:

        $ RELEASE=1 \
        MESOS_JAR=../mesos-0.14.0.jar \
        PROTOBUF_JAR=../protobuf-2.4.1.jar \
        make 0.20.2-cdh3u3

Your current directory should now be a Hadoop on Mesos distribution.
Want to share it? You'll probably want to compile and test it first!
And you might want to delete the 'Makefile', this README.md, and the
'mesos' directory as they are no longer necessary (and could be
confusing for others). See the script 'mesos/make-ant-push' for how we
finalize and push distribution as a tag to Github.
