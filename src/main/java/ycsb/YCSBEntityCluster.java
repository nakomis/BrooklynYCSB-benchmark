package ycsb;

import brooklyn.config.ConfigKey;
import brooklyn.entity.Entity;
import brooklyn.entity.annotation.Effector;
import brooklyn.entity.annotation.EffectorParam;
import brooklyn.entity.basic.ConfigKeys;
import brooklyn.entity.group.DynamicCluster;
import brooklyn.entity.proxying.ImplementedBy;

import java.util.Collection;

/**
 * Created by zaid.mohsin on 16/12/2013.
 */
@ImplementedBy(YCSBEntityClusterImpl.class)
public interface YCSBEntityCluster extends DynamicCluster {


    ConfigKey<Integer> NO_OF_RECORDS = ConfigKeys.newIntegerConfigKey("noOfRecords");

    @Override
    void setMembers(Collection<Entity> m);

    @Effector(description = "Load Workload in all YCSB clients")
    public void loadWorkloadForAll(@EffectorParam(name = "workload name") String workload);

    @Effector(description = "Run Workload in all YCSB clients")
    public void runWorkloadForAll(@EffectorParam(name = "workload name") String workload);
}
