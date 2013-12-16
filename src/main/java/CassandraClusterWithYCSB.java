import brooklyn.config.ConfigKey;
import brooklyn.entity.Entity;
import brooklyn.entity.annotation.Effector;
import brooklyn.entity.basic.AbstractApplication;
import brooklyn.entity.basic.ConfigKeys;
import brooklyn.entity.basic.Entities;
import brooklyn.entity.basic.StartableApplication;
import brooklyn.entity.effector.Effectors;
import brooklyn.entity.java.VanillaJavaApp;
import brooklyn.entity.nosql.cassandra.CassandraCluster;
import brooklyn.entity.nosql.cassandra.CassandraNode;
import brooklyn.entity.proxying.EntitySpec;
import brooklyn.event.SensorEvent;
import brooklyn.event.SensorEventListener;
import brooklyn.launcher.BrooklynLauncher;
import brooklyn.policy.PolicySpec;
import brooklyn.policy.ha.ServiceFailureDetector;
import brooklyn.policy.ha.ServiceReplacer;
import brooklyn.policy.ha.ServiceRestarter;
import brooklyn.util.CommandLineUtil;
import brooklyn.util.text.Strings;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import brooklyn.event.basic.DependentConfiguration;
import com.google.common.collect.Maps;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class CassandraClusterWithYCSB extends AbstractApplication {

    public static final String DEFAULT_LOCATION_SPEC = "aws-ec2:us-east-1";
    public static final ConfigKey<Integer> NUM_AVAILABILITY_ZONES = ConfigKeys.newConfigKey(
            "cassandra.cluster.numAvailabilityZones", "Number of availability zones to spread the cluster across", 1);
    public static final ConfigKey<Integer> CASSANDRA_CLUSTER_SIZE = ConfigKeys.newConfigKey(
            "cassandra.cluster.initialSize", "Initial size of the Cassandra cluster", 3);
    public static boolean CLUSTER_SERVICE_UP = false;

    public static void main(String[] argv) {
        List<String> args = Lists.newArrayList(argv);
        String port =  CommandLineUtil.getCommandLineOption(args, "--port", "8081+");
        String location = CommandLineUtil.getCommandLineOption(args, "--location", "aws-ec2:us-west-1");

        BrooklynLauncher launcher = BrooklynLauncher.newInstance()
                .application(EntitySpec.create(StartableApplication.class, CassandraClusterWithYCSB.class)
                        .displayName("Cassandra YCSB Test"))
                .webconsolePort(port)
                .location(location)
                .start();

        Entities.dumpInfo(launcher.getApplications());
    }

    public VanillaJavaApp ycsbClient;
    @Override
    public void init() {
        CassandraCluster myCluster = addChild(EntitySpec.create(CassandraCluster.class)
                .configure(CassandraCluster.CLUSTER_NAME, "BrooklynBenchmark")
                .configure(CassandraCluster.INITIAL_SIZE, getConfig(CASSANDRA_CLUSTER_SIZE))
                .configure(CassandraCluster.ENABLE_AVAILABILITY_ZONES, true)
                .configure(CassandraCluster.NUM_AVAILABILITY_ZONES, getConfig(NUM_AVAILABILITY_ZONES))
                        //See https://github.com/brooklyncentral/brooklyn/issues/973
                        //.configure(CassandraCluster.AVAILABILITY_ZONE_NAMES, ImmutableList.of("us-east-1b", "us-east-1c", "us-east-1e"))
                .configure(CassandraCluster.ENDPOINT_SNITCH_NAME, "GossipingPropertyFileSnitch")
                .configure(CassandraCluster.MEMBER_SPEC, EntitySpec.create(CassandraNode.class)
                        .policy(PolicySpec.create(ServiceFailureDetector.class))
                        .policy(PolicySpec.create(ServiceRestarter.class)
                                .configure(ServiceRestarter.FAILURE_SENSOR_TO_MONITOR, ServiceFailureDetector.ENTITY_FAILED)))
                .policy(PolicySpec.create(ServiceReplacer.class)
                        .configure(ServiceReplacer.FAILURE_SENSOR_TO_MONITOR, ServiceRestarter.ENTITY_RESTART_FAILED)));

//        //wait for the cluster to be up before starting the clients
//        subscribe(myCluster,CassandraCluster.SERVICE_UP,new SensorEventListener<Boolean>(){
//
//            @Override
//            public void onEvent(SensorEvent<Boolean> event) {
//                CLUSTER_SERVICE_UP=true;
//            }
//        });
//
//        while (!CLUSTER_SERVICE_UP);
//
//        //get all the hostnames of the CassandraNodes.
//        Collection myChildren = myCluster.getChildren();
//
//        ArrayList myList = (ArrayList)Iterables.filter(myChildren, CassandraNode.class);
//        myList = (ArrayList)Iterables.transform(myList,new Function<CassandraNode,String>(){
//
//            @Nullable
//            @Override
//            public String apply(@Nullable CassandraNode cassandraNode) {
//                return cassandraNode.getPublicIp();
//            }
//        });
//
//        //produce the hostnames from the list
//        String hostnames = Strings.join(myList, ",");
//
//        log.info("here are the hostnames: {}", hostnames);

        //add the depedencies from the resources folder
        ArrayList myResources = Lists.newArrayList();
        myResources.add("classpath://cassandra-binding-0.1.4.jar");
        //myResources.add("classpath://resources/ycsb-0.1.4.tar.gz");
        myResources.add("classpath://core-0.1.4.jar");
        myResources.add("classpath://workloada");

        //add the arguments to run the java app
        ArrayList myArgs = Lists.newArrayList();
        myArgs.add("load");
        myArgs.add("cassandra-10");
        myArgs.add("-db com.yahoo.ycsb.db.CassandraClient10");
        myArgs.add("-P workloada");

        //attributeWhenReady();


        ycsbClient = addChild(EntitySpec.create(VanillaJavaApp.class)
                .configure(VanillaJavaApp.MAIN_CLASS, "com.yahoo.ycsb.Client")
                .configure(VanillaJavaApp.CLASSPATH, myResources)
                .configure(VanillaJavaApp.ARGS, myArgs));
    }

    @Effector(description="Load Workload A to Cassandra")
    public void loadWorkloadA()
    {
        Map configurations = Maps.newHashMap();
        ArrayList myResources = Lists.newArrayList();

        myResources.add("classpath://cassandra-binding-0.1.4.jar");
        //myResources.add("classpath://resources/ycsb-0.1.4.tar.gz");
        myResources.add("classpath://core-0.1.4.jar");
        myResources.add("classpath://workloada");

        //add the arguments to run the java app
        ArrayList myArgs = Lists.newArrayList();
        myArgs.add("load");
        myArgs.add("cassandra-10");
        myArgs.add("-db com.yahoo.ycsb.db.CassandraClient10");
        myArgs.add("-P workloada");

        configurations.put(VanillaJavaApp.MAIN_CLASS,"com.yahoo.ycsb.Client");
        configurations.put(VanillaJavaApp.CLASSPATH,myResources);
        configurations.put(VanillaJavaApp.ARGS,myArgs);

        ycsbClient.invoke(ycsbClient.START,configurations);
    }
}
