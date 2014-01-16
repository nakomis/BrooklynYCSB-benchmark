package ycsb;

import brooklyn.entity.Entity;
import brooklyn.entity.basic.Entities;
import brooklyn.entity.basic.EntityInternal;
import brooklyn.entity.group.AbstractMembershipTrackingPolicy;
import brooklyn.entity.group.DynamicClusterImpl;
import brooklyn.entity.proxying.EntitySpec;
import brooklyn.management.Task;
import brooklyn.util.ResourceUtils;
import brooklyn.util.collections.MutableMap;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;

/**
 * Created by zaid.mohsin on 16/12/2013.
 */
public class YCSBEntityClusterImpl extends DynamicClusterImpl implements YCSBEntityCluster {

    private static final Logger log = LoggerFactory.getLogger(YCSBEntityClusterImpl.class);
    private static final Set<String> hostnamesList = Sets.newHashSet();

    public YCSBEntityClusterImpl() {
        super();
    }

    @Override
    public void loadWorkloadForAll(String workload) throws IOException {
        log.info("Loading workload: {} to the database.", workload);

        ResourceUtils resourceUtils = ResourceUtils.create(YCSBEntityCluster.class);
        InputStream workloadFile = resourceUtils.getResourceFromUrl("classpath://" + workload);

        Properties props = new Properties();
        props.load(workloadFile);




        //fetch the attributes for the seleted workload
        Integer opscount = Integer.parseInt(props.getProperty("operationcount"));
        Integer recordCount = Integer.parseInt(props.getProperty("recordcount"));

        Integer clusterSize = getConfig(YCSBEntityCluster.INITIAL_SIZE);

        Integer opsPerNode = Math.round(opscount / clusterSize);

        //insertStart defines the index of the record count each ycsb entity is responsible for loading (e.g. 0 for first  entity 0, second 10, ... if we have 20 total records)
        Integer insertStart = 0;
        //insertCount defines the number of records each ycsb entity is responsible for loading
        Integer insertCount = recordCount / clusterSize;

        for (Entity member : getMembers()) {
            if (member instanceof YCSBEntity) {
                ((EntityInternal) member).setAttribute(YCSBEntity.OPERATIONS_COUNT, opsPerNode);
                ((EntityInternal) member).setAttribute(YCSBEntity.RECORD_COUNT, recordCount);
                ((EntityInternal) member).setAttribute(YCSBEntity.INSERT_COUNT, insertCount);
                ((EntityInternal) member).setAttribute(YCSBEntity.INSERT_START, insertStart * insertCount);

                insertStart++;

            }
        }


        try {
            Iterable<Entity> loadableChildren = Iterables.filter(getChildren(), Predicates.instanceOf(YCSBEntity.class));
            Task<?> invoke = Entities.invokeEffectorListWithArgs(this, loadableChildren, YCSBEntity.LOAD_WORKLOAD, workload);
            if (invoke != null) invoke.get();
        } catch (Exception e) {
            log.info("Exception is caught {}", e.toString());
        }

    }

    @Override
    public void runWorkloadForAll(String workload) throws IOException {

        log.info("Running workload: {} on the database.", workload);

        ResourceUtils resourceUtils = ResourceUtils.create(YCSBEntityCluster.class);
        InputStream workloadFile = resourceUtils.getResourceFromUrl("classpath://" + workload);

        Properties props = new Properties();
        props.load(workloadFile);

        //fetch the attributes for the seleted workload
        String opscount = props.getProperty("operationcount");

        Integer clusterSize = getConfig(YCSBEntityCluster.INITIAL_SIZE);

        //the number of operations is divided by the number of ycsb client nodes
        Integer opsPerNode = Math.round(Integer.parseInt(opscount) / clusterSize);

        //each ycsb client entity in the cluster gets assigned a subset of the operations to carry out
        for (Entity member : getMembers()) {
            if (member instanceof YCSBEntity) {
                ((EntityInternal) member).setAttribute(YCSBEntity.OPERATIONS_COUNT, opsPerNode);
            }

        }

        try {
            Iterable<Entity> loadableChildren = Iterables.filter(getChildren(), Predicates.instanceOf(YCSBEntity.class));
            Task<?> invoke = Entities.invokeEffectorListWithArgs(this, loadableChildren, YCSBEntity.RUN_WORKLOAD, workload);
            if (invoke != null) invoke.get();
        } catch (Exception e) {
            log.info("Exception is caught {}", e.toString());
        }


    }

    @Override
    public void fetchOutputs(String workload) {
        try {
            Iterable<Entity> loadableChildren = Iterables.filter(getChildren(), Predicates.instanceOf(YCSBEntity.class));
            String localpath = getConfig(YCSBEntityCluster.LOCAL_OUTPUT_PATH);

            Task<?> invoke = Entities.invokeEffectorListWithArgs(this, loadableChildren, YCSBEntity.FETCH_OUTPUTS, localpath, workload);
            if (invoke != null) invoke.get();
        } catch (Exception e) {
            log.info("Exception is caught {}", e.toString());
        }
    }

    public void init() {

        log.info("Initializing the YCSB Cluster");
        super.init();


        // track members
        AbstractMembershipTrackingPolicy policy = new AbstractMembershipTrackingPolicy(MutableMap.of("name", "YCSB Entities")) {
            @Override
            protected void onEntityChange(Entity member) {
            }

            @Override
            protected void onEntityAdded(Entity member) {


                if (Boolean.TRUE.equals(member.getAttribute(SERVICE_UP))) {
                    //set the hostnames for the YCSB cluster
                    hostnamesList.add(member.getAttribute(YCSBEntity.HOSTNAME));
                    setAttribute(YCSB_CLUSTER_NODES, Lists.newArrayList(hostnamesList));

                    //set the db type to be benchmarked
                    ((EntityInternal) member).setAttribute(YCSBEntity.DB_TO_BENCHMARK, resolveDB());
                }
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

    private String resolveDB() {
        String dbname = getConfig(YCSBEntityCluster.DB_TO_TEST);

        if (dbname.equals("basic"))
            return "com.yahoo.ycsb.BasicDB";
        else if (dbname.equals("cassandra-7"))
            return "com.yahoo.ycsb.db.CassandraClient7";
        else if (dbname.equals("cassandra-8"))
            return "com.yahoo.ycsb.db.CassandraClient8";
        else if (dbname.equals("cassandra-10"))
            return "com.yahoo.ycsb.db.CassandraClient10";
        else if (dbname.equals("gemfire"))
            return "com.yahoo.ycsb.db.GemFireClient";
        else if (dbname.equals("hbase"))
            return "com.yahoo.ycsb.db.HBaseClient";
        else if (dbname.equals("infinispan"))
            return "com.yahoo.ycsb.db.InfinispanClient";
        else if (dbname.equals("jdbc"))
            return "com.yahoo.ycsb.db.JdbcDBClient";
        else if (dbname.equals("mapkeeper"))
            return "com.yahoo.ycsb.db.MapKeeperClient";
        else if (dbname.equals("mongodb"))
            return "com.yahoo.ycsb.db.MongoDbClient";
        else if (dbname.equals("nosqldb"))
            return "com.yahoo.ycsb.db.NoSqlDbClient";
        else if (dbname.equals("redis"))
            return "com.yahoo.ycsb.db.RedisClient";
        else if (dbname.equals("voldemort"))
            return "com.yahoo.ycsb.db.VoldemortClient";
        else
            return null;

    }


}
