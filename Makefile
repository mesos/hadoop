# Makefile for building a Hadoop on Mesos distribution.

# Look for our "arguments".
ifndef MESOS_JAR
  $(error Missing MESOS_JAR=/path/to/jar)
endif

ifndef PROTOBUF_JAR
  $(error Missing PROTOBUF_JAR=/path/to/jar)
endif

default:

hadoop-0.20.205.0.tar.gz:
	wget http://archive.apache.org/dist/hadoop/core/hadoop-0.20.205.0/$@

hadoop-0.20.2-cdh3u3.tar.gz:
	wget http://archive.cloudera.com/cdh/3/$@

hadoop-0.20.2-cdh3u5.tar.gz:
	wget http://archive.cloudera.com/cdh/3/$@

hadoop-mr1-2.0.0-mr1-cdh4.1.2.tar.gz:
	wget http://archive.cloudera.com/cdh4/cdh/4/mr1-2.0.0-mr1-cdh4.1.2.tar.gz
	mv mr1-2.0.0-mr1-cdh4.1.2.tar.gz $@

hadoop-mr1-2.0.0-mr1-cdh4.2.1.tar.gz:
	wget http://archive.cloudera.com/cdh4/cdh/4/mr1-2.0.0-mr1-cdh4.2.1.tar.gz
	mv mr1-2.0.0-mr1-cdh4.2.1.tar.gz $@

ant-%:
	ant -Dversion=$* compile bin-package

ant-2.0.0-mr1-cdh%:
	ant -Dreactor.repo=file://$HOME/.m2/repository \
	  -Dversion=2.0.0-mr1-cdh$* -Dcompile.c++=true compile bin-package

# Extract the archive, copy it's contents to '.', remove it, and copy
# necessary JARs. TODO(benh): Rather than copying the JARs, can we
# just update the necessary build files for 'ant' or 'maven' to just
# pull down the JARs (i.e., from a local repository if necessary)?
hadoop-%: hadoop-%.tar.gz
	tar zxf $@.tar.gz
	cp -r $@/. .
	cp -r $@/.eclipse.templates .
	cp ${PROTOBUF_JAR} lib
	cp ${MESOS_JAR} lib

%: mesos/%.patch hadoop-%
	patch -p1 <$<

.PHONY: % ant-% ant-2.0.0-mr1-cdh%

# Make tries to delete this "intermediate" via 'rm -f' which fails
# since this is a directory. The .PRECIOUS target keeps make from
# trying to delete the intermediate.
.PRECIOUS: hadoop-%
