package ycsb;

import brooklyn.entity.Entity;
import brooklyn.entity.basic.Attributes;
import brooklyn.entity.basic.Entities;
import brooklyn.entity.basic.EntityInternal;
import brooklyn.entity.group.AbstractMembershipTrackingPolicy;
import brooklyn.entity.group.DynamicClusterImpl;
import brooklyn.entity.proxying.EntitySpec;
import brooklyn.management.Task;
import brooklyn.util.collections.MutableMap;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zaid.mohsin on 16/12/2013.
 */
public class YCSBEntityClusterImpl extends DynamicClusterImpl implements YCSBEntityCluster {

    private static final Logger log = LoggerFactory.getLogger(YCSBEntityClusterImpl.class);
    private static AtomicInteger insertStartForEntity = new AtomicInteger();
    private static Integer numOfRecords;
    private static Integer insertCount;
    private static Integer operationCountPerNode;
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
        try {
            Iterable<Entity> loadableChildren = Iterables.filter(getChildren(), Predicates.instanceOf(YCSBEntity.class));
            Task<?> invoke = Entities.invokeEffectorListWithArgs(this, loadableChildren, YCSBEntity.LOAD_WORKLOAD, workload);
            if (invoke != null) invoke.get();
        } catch (Exception e) {
            log.info("Exception is caught {}", e.toString());
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

        try {
            Iterable<Entity> loadableChildren = Iterables.filter(getChildren(), Predicates.instanceOf(YCSBEntity.class));
            Task<?> invoke = Entities.invokeEffectorListWithArgs(this, loadableChildren, YCSBEntity.RUN_WORKLOAD, workload);
            if (invoke != null) invoke.get();
        } catch (Exception e) {
            log.info("Exception is caught {}", e.toString());
        }

    }

    @Override
    public void fetchOutputs() {
        try {
            Iterable<Entity> loadableChildren = Iterables.filter(getChildren(), Predicates.instanceOf(YCSBEntity.class));
            String localpath = getConfig(YCSBEntityCluster.LOCAL_OUTPUT_PATH);

            Task<?> invoke = Entities.invokeEffectorListWithArgs(this, loadableChildren, YCSBEntity.FETCH_OUTPUTS, localpath);
            if (invoke != null) invoke.get();
        } catch (Exception e) {
            log.info("Exception is caught {}", e.toString());
        }
    }

    public void init() {

        log.info("Initializing the YCSB Cluster");
        super.init();
        numOfRecords = this.getConfig(YCSBEntityCluster.NO_OF_RECORDS);
        Integer clusterSize = getConfig(YCSBEntityCluster.INITIAL_SIZE);
        insertCount = numOfRecords/clusterSize;

        Integer totalOperationCount = getConfig(YCSBEntityCluster.TOTAL_OPERATIONS_COUNT);
        operationCountPerNode = Math.round(totalOperationCount/clusterSize);

        log.info("populating fields of cluster size: {}", Integer.toString(clusterSize));
        log.info("number of records: {}", numOfRecords);


        // track members
        AbstractMembershipTrackingPolicy policy = new AbstractMembershipTrackingPolicy(MutableMap.of("name", "YCSB Entities")) {
            @Override
            protected void onEntityChange(Entity member) {
            }

            @Override
            protected void onEntityAdded(Entity member) {

                if (member.getAttribute(YCSBEntity.INSERT_START) == null) {
                    log.info("Setting the insert start for entity: {}", insertStartForEntity.get());
                    //add the total record count to all ycsb entities
                    ((EntityInternal) member).setAttribute(YCSBEntity.RECORD_COUNT, numOfRecords);

                    //add the insert count (number of recrods each node inserts)
                    ((EntityInternal) member).setAttribute(YCSBEntity.INSERT_COUNT,insertCount);

                    //add the insert start (number of records segment each node is responsible for)
                    ((EntityInternal) member).setAttribute(YCSBEntity.INSERT_START, insertStartForEntity.getAndIncrement() * insertCount);

                    //add the number of operations each node is responsible for.
                    ((EntityInternal) member).setAttribute(YCSBEntity.OPERATIONS_COUNT, operationCountPerNode);
                }
            }

            @Override
            protected void onEntityRemoved(Entity member) {
            }
        };
        addPolicy(policy);
        policy.setGroup(this);

        //set the list of hostnames for the ycsb nodes.
        setAttribute(YCSBEntityCluster.YCSB_CLUSTER_NODES, hostnamesList);

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
