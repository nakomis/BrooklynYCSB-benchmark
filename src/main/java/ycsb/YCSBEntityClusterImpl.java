package ycsb;

import brooklyn.entity.Entity;
import brooklyn.entity.basic.*;
import brooklyn.entity.group.AbstractMembershipTrackingPolicy;
import brooklyn.entity.group.DynamicClusterImpl;
import brooklyn.entity.proxying.EntitySpec;
import brooklyn.util.collections.MutableMap;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zaid.mohsin on 16/12/2013.
 */
public class YCSBEntityClusterImpl extends DynamicClusterImpl implements YCSBEntityCluster {

    private static AtomicInteger insertStartForEntity = new AtomicInteger();
    private static Integer numOfRecords;
    private static Integer insertStartFactor;
    private static final Logger log = LoggerFactory.getLogger(YCSBEntityClusterImpl.class);

    public YCSBEntityClusterImpl() {
        super();
    }

    @Override
    public String executeScript(String commands) {
        return null;
    }

    @Override
    public void loadWorkloadForAll(String workload) {

        log.info("Loading workload: {} to the database.",workload);
        final String myWorkload = workload;

        Iterables.transform(Iterables.filter(getMembers(), YCSBEntity.class), new Function<YCSBEntity, Void>() {

            @Nullable
            @Override
            public Void apply(@Nullable YCSBEntity ycsbEntity) {

                Entities.invokeEffectorWithArgs(YCSBEntityClusterImpl.this,ycsbEntity,YCSBEntity.LOAD_EFFECTOR,myWorkload);
                return null;
            }
        });

    }

    @Override
    public void runWorkloadForAll(String workload) {

        log.info("Running workload: {} on the database.",workload);
        final String myWorkload = workload;

        Iterables.transform(Iterables.filter(getMembers(), YCSBEntity.class), new Function<YCSBEntity, Void>() {

            @Nullable
            @Override
            public Void apply(@Nullable YCSBEntity ycsbEntity) {

                Entities.invokeEffectorWithArgs(YCSBEntityClusterImpl.this,ycsbEntity,YCSBEntity.RUN_EFFECTOR,myWorkload);
                return null;
            }
        });

    }

    public void init() {

        log.info("Initializing the YCSB Cluster");
        super.init();
        numOfRecords = this.getConfig(YCSBEntityCluster.NO_OF_RECORDS);
        insertStartFactor = numOfRecords / getConfig(YCSBEntityCluster.INITIAL_SIZE);

        // track members
        AbstractMembershipTrackingPolicy policy = new AbstractMembershipTrackingPolicy(MutableMap.of("name", "YCSB Entities")) {
            @Override
            protected void onEntityChange(Entity member) {
            }

            @Override
            protected void onEntityAdded(Entity member) {

                ((EntityInternal) member).setAttribute(YCSBEntity.INSERT_START, insertStartForEntity.get() * insertStartFactor);
                insertStartForEntity.incrementAndGet();
            }

            @Override
            protected void onEntityRemoved(Entity member) {
            }
        };
        addPolicy(policy);
        policy.setGroup(this);

    }

    @Override
    protected EntitySpec<?> getMemberSpec() {
        return getConfig(MEMBER_SPEC, EntitySpec.create(YCSBEntity.class));
    }

    @Override
    public synchronized boolean addMember(Entity member) {
        boolean result = super.addMember(member);
        setAttribute(SERVICE_UP, calculateServiceUp());
        return result;
    }

    @Override
    public synchronized boolean removeMember(Entity member) {
        boolean result = super.removeMember(member);
        setAttribute(SERVICE_UP, calculateServiceUp());
        return result;
    }

    @Override
    protected boolean calculateServiceUp() {
        boolean up = false;
        for (Entity member : getMembers()) {
            if (Boolean.TRUE.equals(member.getAttribute(SERVICE_UP))) up = true;
        }
        return up;
    }

}
