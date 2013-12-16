package ycsb;

import brooklyn.config.ConfigKey;
import brooklyn.entity.annotation.Effector;
import brooklyn.entity.basic.ConfigKeys;
import brooklyn.entity.java.VanillaJavaApp;
import brooklyn.entity.java.VanillaJavaAppImpl;
import brooklyn.entity.proxying.ImplementedBy;
import brooklyn.event.basic.BasicConfigKey;
import com.google.common.collect.Lists;

import java.util.List;

@ImplementedBy(YCSBEntityImpl.class)
public interface YCSBEntity extends VanillaJavaApp {


    ConfigKey<List> HOSTNAMES = new BasicConfigKey<List>(List.class,"ycsb.hostnames","list of all hostnames to benchmark",Lists.newArrayList());
    ConfigKey<Integer> INSERT_START = ConfigKeys.newIntegerConfigKey("insertstart");
    //ConfigKey<Integer> NO_OF_RECORDS = ConfigKeys.newIntegerConfigKey("noOfRecords");



    @Effector(description="Load WorkloadA")
    public void loadWorkloadAEffector();

    @Effector(description="Run WorkloadA")
    public void runWorkloadAEffector();


    Integer getInsertStart();
    //Integer getNoOfRecords();
}
