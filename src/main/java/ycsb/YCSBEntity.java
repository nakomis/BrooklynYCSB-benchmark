package ycsb;

import brooklyn.config.ConfigKey;
import brooklyn.entity.Effector;
import brooklyn.entity.basic.ConfigKeys;
import brooklyn.entity.effector.Effectors;
import brooklyn.entity.java.VanillaJavaApp;
import brooklyn.entity.java.VanillaJavaAppImpl;
import brooklyn.entity.proxying.ImplementedBy;
import brooklyn.event.AttributeSensor;
import brooklyn.event.basic.BasicConfigKey;
import brooklyn.event.basic.Sensors;
import com.google.common.collect.Lists;

import java.util.List;

@ImplementedBy(YCSBEntityImpl.class)
public interface YCSBEntity extends VanillaJavaApp {


    ConfigKey<List> HOSTNAMES = new BasicConfigKey<List>(List.class,"ycsb.hostnames","list of all hostnames to benchmark",Lists.newArrayList());
    AttributeSensor<Integer> INSERT_START = Sensors.newIntegerSensor("insertstart");
    //ConfigKey<Integer> NO_OF_RECORDS = ConfigKeys.newIntegerConfigKey("noOfRecords");


    brooklyn.entity.Effector<String> LOAD_EFFECTOR = Effectors.effector(String.class, "loadEffector")
            .description("Loads a new workload to the database")
            .parameter(String.class, "workload")
            .buildAbstract();

    brooklyn.entity.Effector<String> RUN_EFFECTOR = Effectors.effector(String.class, "runEffector")
            .description("Runs a new workload to the database")
            .parameter(String.class, "workload")
            .buildAbstract();

//    @brooklyn.entity.annotation.Effector(description="Load WorkloadA")
//    public void loadWorkloadEffector(@EffectorParam(name="workload name") String workload);
//
//    @brooklyn.entity.annotation.Effector(description="Run WorkloadA")
//    public void runWorkloadEffector(@EffectorParam(name="workload name") String workload);


    Integer getInsertStart();
    //Integer getNoOfRecords();
}
