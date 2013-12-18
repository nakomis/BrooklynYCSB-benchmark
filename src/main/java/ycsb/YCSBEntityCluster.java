package ycsb;

import brooklyn.config.ConfigKey;
import brooklyn.entity.Entity;
import brooklyn.entity.Group;
import brooklyn.entity.basic.AbstractGroup;
import brooklyn.entity.basic.ConfigKeys;
import brooklyn.entity.group.Cluster;
import brooklyn.entity.group.DynamicCluster;
import brooklyn.entity.proxying.EntitySpec;
import brooklyn.entity.proxying.ImplementedBy;
import brooklyn.event.basic.BasicConfigKey;
import brooklyn.util.flags.SetFromFlag;

import java.util.Collection;

/**
 * Created by zaid.mohsin on 16/12/2013.
 */
@ImplementedBy(YCSBEntityClusterImpl.class)
public interface YCSBEntityCluster extends DynamicCluster {


    ConfigKey<Integer> NO_OF_RECORDS = ConfigKeys.newIntegerConfigKey("noOfRecords");
    String executeScript(String commands);



    @Override
    void setMembers(Collection<Entity> m);
}