package ycsb;

import brooklyn.config.ConfigKey;
import brooklyn.entity.annotation.Effector;
import brooklyn.entity.annotation.EffectorParam;
import brooklyn.entity.basic.MethodEffector;
import brooklyn.entity.effector.Effectors;
import brooklyn.entity.java.VanillaJavaApp;
import brooklyn.entity.proxying.ImplementedBy;
import brooklyn.event.AttributeSensor;
import brooklyn.event.basic.BasicAttributeSensorAndConfigKey;
import brooklyn.event.basic.BasicConfigKey;
import brooklyn.event.basic.Sensors;
import com.google.common.collect.Lists;

import java.util.List;

@ImplementedBy(YCSBEntityImpl.class)
public interface YCSBEntity extends VanillaJavaApp {


    ConfigKey<List> HOSTNAMES = new BasicConfigKey<List>(List.class, "ycsb.hostnames", "list of all hostnames to benchmark", Lists.newArrayList());
    AttributeSensor<Integer> INSERT_START = Sensors.newIntegerSensor("ycsb.insertstart","inital records number to start loading");
    AttributeSensor<Integer> RECORD_COUNT = Sensors.newIntegerSensor("ycsb.recordcount","the total number of records");

    Integer getInsertStart();

    Integer getRecordCount();
    @Effector(description="Loads a new workload to the database")
    void loadWorkloadEffector(@EffectorParam(name="workload", description="The name of the workload file") String workload);
    @Effector(description="Runs a workload on the database")
    void runWorkloadEffector(@EffectorParam(name="workload", description="The name of the workload file") String workload);


    MethodEffector<Void> LOAD_WORKLOAD = new MethodEffector<Void>(YCSBEntity.class,"loadWorkloadEffector");
    MethodEffector<Void> RUN_WORKLOAD = new MethodEffector<Void>(YCSBEntity.class,"runWorkloadEffector");


}
