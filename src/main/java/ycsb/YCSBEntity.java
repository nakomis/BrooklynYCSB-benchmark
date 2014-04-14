package ycsb;

import brooklyn.config.ConfigKey;
import brooklyn.entity.annotation.Effector;
import brooklyn.entity.annotation.EffectorParam;
import brooklyn.entity.basic.ConfigKeys;
import brooklyn.entity.basic.MethodEffector;
import brooklyn.entity.java.VanillaJavaApp;
import brooklyn.entity.proxying.ImplementedBy;
import brooklyn.event.AttributeSensor;
import brooklyn.event.basic.BasicConfigKey;
import brooklyn.event.basic.Sensors;
import com.google.common.collect.Lists;

import java.util.List;

@ImplementedBy(YCSBEntityImpl.class)
public interface YCSBEntity extends VanillaJavaApp {

    ConfigKey<List> HOSTNAMES = new BasicConfigKey<List>(List.class, "ycsb.hostnames", "list of all hostnames to benchmark", Lists.newArrayList());
    AttributeSensor<Integer> INSERT_START = Sensors.newIntegerSensor("ycsb.insertstart", "inital records number to start loading");
    AttributeSensor<Integer> INSERT_COUNT = Sensors.newIntegerSensor("ycsb.insertcount", "number of records the ycsb client is responsible for inserting");
    AttributeSensor<Integer> RECORD_COUNT = Sensors.newIntegerSensor("ycsb.recordcount", "the total number of records");
    AttributeSensor<Integer> OPERATIONS_COUNT = Sensors.newIntegerSensor("ycsb.operationcount", "the number of operations to to run on a database");
    ConfigKey<Boolean> TIMESERIES = ConfigKeys.newBooleanConfigKey("ycsb.timeseries", "flag to specify if timeseries to be calculated in results");
    ConfigKey<Integer> TIMESERIES_GRANULARITY = ConfigKeys.newIntegerConfigKey("ycsb.timseries.granularity", "time for intervals between timeseries averages");
    ConfigKey<String> DB_TO_BENCHMARK = ConfigKeys.newStringConfigKey("ycsb.db_to_benchmark", "name of the db to benchmark", "com.yahoo.ycsb.db.CassandraClient10");
    MethodEffector<Void> RUN_WORKLOAD = new MethodEffector<Void>(YCSBEntity.class, "runWorkloadEffector");

    ConfigKey<String> LOCAL_OUTPUT_PATH = ConfigKeys.newStringConfigKey("ycsb.localOutputPath", "the path to fetch the output files to");

    @Effector(description = "Runs a workload on the database")
    void runWorkloadEffector(@EffectorParam(name = "workload", description = "The name of the workload file") String workload);

}
