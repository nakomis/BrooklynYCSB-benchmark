package ycsb;

import brooklyn.entity.Entity;
import brooklyn.entity.annotation.EffectorParam;
import brooklyn.entity.basic.AbstractGroupImpl;
import brooklyn.entity.basic.Attributes;
import brooklyn.entity.basic.EntityFactory;
import brooklyn.entity.basic.EntityInternal;
import brooklyn.entity.group.AbstractMembershipTrackingPolicy;
import brooklyn.entity.group.DynamicCluster;
import brooklyn.entity.group.DynamicClusterImpl;
import brooklyn.entity.proxying.EntitySpec;
import brooklyn.event.SensorEvent;
import brooklyn.event.SensorEventListener;
import brooklyn.location.Location;
import brooklyn.policy.Enricher;
import brooklyn.policy.Policy;
import brooklyn.util.collections.MutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zaid.mohsin on 16/12/2013.
 */
public class YCSBEntityClusterImpl extends DynamicClusterImpl implements YCSBEntityCluster {

    private static AtomicInteger insertStartForEntity = new AtomicInteger();
    private static Integer numOfRecords;
    private static Integer insertStartFactor;

    @Override
    public String executeScript(String commands) {
        return null;
    }


    @Override
    public void start(@EffectorParam(name = "locations") Collection<? extends Location> locations) {
    super.start(locations);
    }

    @Override
    public void stop() {
     super.stop();
    }

    @Override
    public void restart() {
    super.restart();
    }

    @Override
    public Integer resize(@EffectorParam(name = "desiredSize", description = "The new size of the cluster") Integer desiredSize) {
        return null;
    }

    public void init() {

        numOfRecords = this.getConfig(YCSBEntityCluster.NO_OF_RECORDS);
        insertStartFactor = Lists.newArrayList(Iterables.filter(getMembers(), YCSBEntity.class)).size();

        // track members
        AbstractMembershipTrackingPolicy policy = new AbstractMembershipTrackingPolicy(MutableMap.of("name", "YCSB Entities")) {
            @Override
            protected void onEntityChange(Entity member) { }
            @Override
            protected void onEntityAdded(Entity member) {

                ((EntityInternal) member).setAttribute(YCSBEntity.INSERT_START, insertStartForEntity.get() * insertStartFactor);
                insertStartForEntity.incrementAndGet();
            }
            @Override
            protected void onEntityRemoved(Entity member) { }
        };
        addPolicy(policy);
        policy.setGroup(this);
    }

    @Override
    protected EntitySpec<?> getMemberSpec() {
        return getConfig(MEMBER_SPEC, EntitySpec.create(YCSBEntity.class));
    }

}
