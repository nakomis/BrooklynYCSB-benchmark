import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import brooklyn.catalog.Catalog;
import brooklyn.config.ConfigKey;
import brooklyn.entity.basic.AbstractApplication;
import brooklyn.entity.basic.ConfigKeys;
import brooklyn.entity.basic.Entities;
import brooklyn.entity.basic.StartableApplication;
import brooklyn.entity.nosql.cassandra.CassandraDatacenter;
import brooklyn.entity.nosql.cassandra.CassandraNode;
import brooklyn.entity.proxying.EntitySpec;
import brooklyn.event.AttributeSensor;
import brooklyn.event.SensorEvent;
import brooklyn.event.SensorEventListener;
import brooklyn.event.basic.DependentConfiguration;
import brooklyn.event.basic.Sensors;
import brooklyn.launcher.BrooklynLauncher;
import brooklyn.policy.PolicySpec;
import brooklyn.policy.ha.ServiceFailureDetector;
import brooklyn.policy.ha.ServiceReplacer;
import brooklyn.policy.ha.ServiceRestarter;
import brooklyn.util.CommandLineUtil;
import ycsb.YCSBEntity;

@Catalog(name = "Cassandra Benchmarking with YCSB Entity", description = "Deploys A Cassandra Cluster with a YCSB Client to benchmark the cluster")
public class CassandraYCSBBenchmarking extends AbstractApplication {

    public static final AttributeSensor<Boolean> scriptExecuted = Sensors.newBooleanSensor("scriptExecuted");
    public static final String DEFAULT_LOCATION_SPEC = "aws-ec2:us-east-1";
    public static final ConfigKey<Integer> NUM_AVAILABILITY_ZONES = ConfigKeys.newConfigKey(
            "cassandra.cluster.numAvailabilityZones", "Number of availability zones to spread the cluster across", 1);
    public static final ConfigKey<Integer> CASSANDRA_CLUSTER_SIZE = ConfigKeys.newConfigKey(
            "cassandra.cluster.initialSize", "Initial size of the Cassandra cluster", 3);
    private static final Logger log = LoggerFactory.getLogger(CassandraYCSBBenchmarking.class);
    private static final AtomicBoolean scriptBoolean = new AtomicBoolean();
    private CassandraDatacenter cassandraCluster;


    public static void main(String[] argv) {
        List<String> args = Lists.newArrayList(argv);
        String port = CommandLineUtil.getCommandLineOption(args, "--port", "8081+");
        String location = CommandLineUtil.getCommandLineOption(args, "--location", DEFAULT_LOCATION_SPEC);

        BrooklynLauncher launcher = BrooklynLauncher.newInstance()
                .application(EntitySpec.create(StartableApplication.class, CassandraYCSBBenchmarking.class)
                        .displayName("Cassandra Cluster to Benchmark"))
                .webconsolePort(port)
                .location(location)
                .start();

        Entities.dumpInfo(launcher.getApplications());
    }

    @Override
    public void init() {
        //initialize the Cassandra Cluster
        cassandraCluster = addChild(EntitySpec.create(CassandraDatacenter.class)
                .configure(CassandraDatacenter.CLUSTER_NAME, "Brooklyn")
                .configure(CassandraDatacenter.INITIAL_SIZE, getConfig(CASSANDRA_CLUSTER_SIZE))
                .configure(CassandraDatacenter.ENDPOINT_SNITCH_NAME, "GossipingPropertyFileSnitch")
                .configure(CassandraDatacenter.MEMBER_SPEC, EntitySpec.create(CassandraNode.class)
                        .policy(PolicySpec.create(ServiceFailureDetector.class))
                        .policy(PolicySpec.create(ServiceRestarter.class)
                                .configure(ServiceRestarter.FAILURE_SENSOR_TO_MONITOR, ServiceFailureDetector.ENTITY_FAILED)))
                .policy(PolicySpec.create(ServiceReplacer.class)
                        .configure(ServiceReplacer.FAILURE_SENSOR_TO_MONITOR, ServiceRestarter.ENTITY_RESTART_FAILED)));

        //create the benchmarking table on the Cassandra Cluster
        subscribeToMembers(cassandraCluster, CassandraDatacenter.SERVICE_UP, new SensorEventListener<Boolean>() {
            @Override
            public void onEvent(SensorEvent<Boolean> event) {
                if (Boolean.TRUE.equals(event.getValue()))
                    if (event.getSource() instanceof CassandraNode && scriptBoolean.compareAndSet(false, true)) {

                        CassandraNode anyNode = (CassandraNode) event.getSource();
                        log.info("Creating keyspace 'usertable' with column family 'data' on Node {}", event.getSource().getId());

                        Entities.invokeEffectorWithArgs(CassandraYCSBBenchmarking.this, anyNode, CassandraNode.EXECUTE_SCRIPT, "create keyspace usertable with placement_strategy = " +
                                "'org.apache.cassandra.locator.SimpleStrategy' and strategy_options = {replication_factor:3};" +
                                "\nuse usertable;" +
                                "\ncreate column family data;");

                        setAttribute(scriptExecuted, true);
                    }
            }
        });

        //create the YCSB client to benchmark the cassandra cluster.
        addChild(EntitySpec.create(YCSBEntity.class)
                .configure(YCSBEntity.MAIN_CLASS, "com.yahoo.ycsb.Client")
                .configure(YCSBEntity.CLASSPATH, ImmutableList.of("classpath://cassandra-binding-0.1.4.jar"
                        , "classpath://core-0.1.4.jar", "classpath://slf4j-simple-1.7.5.jar"))
                .configure(YCSBEntity.HOSTNAMES, DependentConfiguration.attributeWhenReady(cassandraCluster, CassandraDatacenter.CASSANDRA_CLUSTER_NODES)));
    }

}


