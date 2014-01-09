package ycsb;

import brooklyn.entity.Entity;
import brooklyn.entity.basic.Attributes;
import brooklyn.entity.basic.Entities;
import brooklyn.entity.basic.EntityInternal;
import brooklyn.entity.basic.EntityPredicates;
import brooklyn.entity.group.AbstractMembershipTrackingPolicy;
import brooklyn.entity.group.DynamicClusterImpl;
import brooklyn.entity.proxying.EntitySpec;
import brooklyn.entity.trait.Startable;
import brooklyn.entity.trait.StartableMethods;
import brooklyn.management.Task;
import brooklyn.util.collections.MutableMap;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zaid.mohsin on 16/12/2013.
 */
public class YCSBEntityClusterImpl extends DynamicClusterImpl implements YCSBEntityCluster {

    private static final Logger log = LoggerFactory.getLogger(YCSBEntityClusterImpl.class);
    private static AtomicInteger insertStartForEntity = new AtomicInteger();
    private static Integer numOfRecords;
    private static Integer insertStartFactor;
    private static List<String> hostnamesList = Lists.newArrayList();
    private final Object mutex = new Object[0];

    public YCSBEntityClusterImpl() {
        super();
    }

    @Override
    public void loadWorkloadForAll(String workload) {
        log.info("Loading workload: {} to the database.", workload);
        //final String myWorkload = workload;

//        List<YCSBEntity> myMembers = Lists.newArrayList(Iterables.filter(getMembers(),YCSBEntity.class));
//        for (YCSBEntity myEntity: myMembers)
//        {
//            log.info("Invoking load " + workload + " on " + myEntity.getId());
//
//            myEntity.loadWorkloadEffector(workload);
//        }
       try
       {
        Iterable<Entity> loadableChildren = Iterables.filter(getChildren(), Predicates.instanceOf(YCSBEntity.class));
        Task<?> invoke = Entities.invokeEffectorListWithArgs(this, loadableChildren, YCSBEntity.LOAD_WORKLOAD,workload);
        if (invoke != null) invoke.get();
       }
       catch (Exception e)
       {
           log.info("Exception is caught {}",e.toString());
       }

    }

    @Override
    public void runWorkloadForAll(String workload) {

        log.info("Running workload: {} on the database.", workload);
//        List<YCSBEntity> myMembers = Lists.newArrayList(Iterables.filter(getMembers(),YCSBEntity.class));
//        for (YCSBEntity myEntity: myMembers)
//        {
//            log.info("Invoking run " + workload + " on " + myEntity.getId());
//
//            myEntity.runWorkloadEffector(workload);
//        }

        try
        {
            Iterable<Entity> loadableChildren = Iterables.filter(getChildren(), Predicates.instanceOf(YCSBEntity.class));
            Task<?> invoke = Entities.invokeEffectorListWithArgs(this, loadableChildren, YCSBEntity.RUN_WORKLOAD,workload);
            if (invoke != null) invoke.get();
        }
        catch (Exception e)
        {
            log.info("Exception is caught {}",e.toString());
        }

    }

    public void init() {

        log.info("Initializing the YCSB Cluster");
        super.init();
        numOfRecords = this.getConfig(YCSBEntityCluster.NO_OF_RECORDS);
        Integer clusterSize = getConfig(YCSBEntityCluster.INITIAL_SIZE);
        insertStartFactor = numOfRecords / getConfig(YCSBEntityCluster.INITIAL_SIZE);

        log.info("populating fields of cluster size: {}",Integer.toString(clusterSize));
        log.info("number of records: {}",numOfRecords);
        log.info("insertStartFactor: {}", insertStartFactor);



        // track members
        AbstractMembershipTrackingPolicy policy = new AbstractMembershipTrackingPolicy(MutableMap.of("name", "YCSB Entities")) {
            @Override
            protected void onEntityChange(Entity member) {
            }

            @Override
            protected void onEntityAdded(Entity member) {

                if (member.getAttribute(YCSBEntity.INSERT_START) == null) {
                    log.info("Setting the insert start for entity: {}",insertStartForEntity.get());
                    ((EntityInternal) member).setAttribute(YCSBEntity.RECORD_COUNT, numOfRecords);


                ((EntityInternal) member).setAttribute(YCSBEntity.INSERT_START, insertStartForEntity.getAndIncrement() * insertStartFactor);
                    hostnamesList.add(member.getAttribute(Attributes.HOSTNAME));

                }


            }

            @Override
            protected void onEntityRemoved(Entity member) {
            }
        };
        addPolicy(policy);
        policy.setGroup(this);

        //set the list of hostnames for the ycsb nodes.
        setAttribute(YCSBEntityCluster.YCSB_CLUSTER_NODES,hostnamesList);

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
