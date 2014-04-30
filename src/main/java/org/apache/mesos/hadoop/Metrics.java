package org.apache.mesos.hadoop;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.cassandra.Cassandra;
import com.codahale.metrics.cassandra.CassandraReporter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.mesos.Protos.TaskState;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class Metrics {
  public MetricRegistry registry;
  public Meter killMeter, flakyTrackerKilledMeter, launchTimeout, periodicGC;
  public Map<Integer, Meter> jobStateMeter =
    new ConcurrentHashMap<Integer, Meter>();
  public Map<TaskState, Meter> taskStateMeter =
    new ConcurrentHashMap<TaskState, Meter>();
  public com.codahale.metrics.Timer jobTimer, trackerTimer;
  public Map<JobID, com.codahale.metrics.Timer.Context> jobTimerContexts =
    new ConcurrentHashMap<JobID, com.codahale.metrics.Timer.Context>();

  public Metrics(Configuration conf) {
    registry = new MetricRegistry();

    killMeter = registry.meter(MetricRegistry.name(Metrics.class, "killMeter"));
    flakyTrackerKilledMeter = registry.meter(MetricRegistry.name(Metrics.class, "flakyTrackerKilledMeter"));
    launchTimeout = registry.meter(MetricRegistry.name(Metrics.class, "launchTimeout"));
    periodicGC = registry.meter(MetricRegistry.name(Metrics.class, "periodicGC"));
    jobTimer = registry.timer(MetricRegistry.name(Metrics.class, "jobTimes"));
    trackerTimer = registry.timer(MetricRegistry.name(Metrics.class, "trackerTimes"));

    for (int i = 1; i <= 5; ++i) {
      jobStateMeter.put(i, registry.meter(MetricRegistry.name(Metrics.class, "jobState", JobStatus.getJobRunState(i))));
    }

    for (TaskState state : TaskState.values()) {
      taskStateMeter.put(state, registry.meter(MetricRegistry.name(Metrics.class, "taskState", state.name())));
    }

    registry.register(MetricRegistry.name(ThreadStatesGaugeSet.class), new ThreadStatesGaugeSet());
    registry.register(MetricRegistry.name(GarbageCollectorMetricSet.class), new GarbageCollectorMetricSet());
    registry.register(MetricRegistry.name(MemoryUsageGaugeSet.class), new MemoryUsageGaugeSet());

    final boolean csvEnabled = conf.getBoolean("mapred.mesos.metrics.csv.enabled", false);
    if (csvEnabled) {
      final String path = conf.get("mapred.mesos.metrics.csv.path");
      final int interval = conf.getInt("mapred.mesos.metrics.csv.interval", 60);

      CsvReporter csvReporter = CsvReporter.forRegistry(registry)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .filter(MetricFilter.ALL)
        .build(new File(path));
      csvReporter.start(interval, TimeUnit.SECONDS);
    }

    final boolean graphiteEnabled = conf.getBoolean("mapred.mesos.metrics.graphite.enabled", false);
    if (graphiteEnabled) {
      final String host = conf.get("mapred.mesos.metrics.graphite.host");
      final int port = conf.getInt("mapred.mesos.metrics.graphite.port", 2003);
      final String prefix = conf.get("mapred.mesos.metrics.graphite.prefix");
      final int interval = conf.getInt("mapred.mesos.metrics.graphite.interval", 60);

      Graphite graphite = new Graphite(new InetSocketAddress(host, port));
      GraphiteReporter graphiteReporter = GraphiteReporter.forRegistry(registry)
        .prefixedWith(prefix)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .filter(MetricFilter.ALL)
        .build(graphite);
      graphiteReporter.start(interval, TimeUnit.SECONDS);
    }

    final boolean cassandraEnabled = conf.getBoolean("mapred.mesos.metrics.cassandra.enabled", false);
    if (cassandraEnabled) {
      final String hosts = conf.get("mapred.mesos.metrics.cassandra.hosts");
      final int port = conf.getInt("mapred.mesos.metrics.cassandra.port", 9042);
      final String prefix = conf.get("mapred.mesos.metrics.cassandra.prefix");
      final int interval = conf.getInt("mapred.mesos.metrics.cassandra.interval", 60);
      final int ttl = conf.getInt("mapred.mesos.metrics.cassandra.ttl", 864000);
      final String keyspace = conf.get("mapred.mesos.metrics.cassandra.keyspace");
      final String table = conf.get("mapred.mesos.metrics.cassandra.table");
      final String consistency = conf.get("mapred.mesos.metrics.cassandra.consistency");

      Cassandra cassandra = new Cassandra(
          Arrays.asList(hosts.split(",")),
          keyspace,
          table,
          ttl,
          port,
          consistency);

      CassandraReporter cassandraReporter = CassandraReporter.forRegistry(registry)
        .prefixedWith(prefix)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .filter(MetricFilter.ALL)
        .build(cassandra);
      cassandraReporter.start(interval, TimeUnit.SECONDS);
    }
  }
}
