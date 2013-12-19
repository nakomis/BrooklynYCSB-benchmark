/*
 * Copyright 2012-2013 by Cloudsoft Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import brooklyn.config.ConfigKey;
import brooklyn.entity.basic.*;
import brooklyn.entity.nosql.cassandra.CassandraCluster;
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ycsb.YCSBEntity;
import ycsb.YCSBEntityCluster;


public class HighAvailabilityCassandraCluster extends AbstractApplication {

    public static final AttributeSensor<Boolean> scriptExecuted = Sensors.newBooleanSensor("scriptExecuted");
    public static final String DEFAULT_LOCATION_SPEC = "aws-ec2:us-east-1";
    public static final ConfigKey<Integer> NUM_AVAILABILITY_ZONES = ConfigKeys.newConfigKey(
            "cassandra.cluster.numAvailabilityZones", "Number of availability zones to spread the cluster across", 1);
    public static final ConfigKey<Integer> CASSANDRA_CLUSTER_SIZE = ConfigKeys.newConfigKey(
            "cassandra.cluster.initialSize", "Initial size of the Cassandra cluster", 2);
    private static final Logger log = LoggerFactory.getLogger(HighAvailabilityCassandraCluster.class);

    private final Object mutex = new Object[0];
    private static final AtomicBoolean scriptBoolean = new AtomicBoolean();

    private CassandraCluster cassandraCluster;

    public static void main(String[] argv) {
        List<String> args = Lists.newArrayList(argv);
        String port = CommandLineUtil.getCommandLineOption(args, "--port", "8081+");
        String location = CommandLineUtil.getCommandLineOption(args, "--location", DEFAULT_LOCATION_SPEC);

        BrooklynLauncher launcher = BrooklynLauncher.newInstance()
                .application(EntitySpec.create(StartableApplication.class, HighAvailabilityCassandraCluster.class)
                        .displayName("Cassandra Cluster to Benchmark"))
                .webconsolePort(port)
                .location(location)
                .start();

        Entities.dumpInfo(launcher.getApplications());
    }

    @Override
    public void init() {

        //initialize the Cassandra Cluster
        cassandraCluster = addChild(EntitySpec.create(CassandraCluster.class)
                .configure(CassandraCluster.CLUSTER_NAME, "Brooklyn")
                .configure(CassandraCluster.INITIAL_SIZE, getConfig(CASSANDRA_CLUSTER_SIZE))
                .configure(CassandraCluster.ENDPOINT_SNITCH_NAME, "GossipingPropertyFileSnitch")
                .configure(CassandraCluster.MEMBER_SPEC, EntitySpec.create(CassandraNode.class)
                        .policy(PolicySpec.create(ServiceFailureDetector.class))
                        .policy(PolicySpec.create(ServiceRestarter.class)
                                .configure(ServiceRestarter.FAILURE_SENSOR_TO_MONITOR, ServiceFailureDetector.ENTITY_FAILED)))
                .policy(PolicySpec.create(ServiceReplacer.class)
                        .configure(ServiceReplacer.FAILURE_SENSOR_TO_MONITOR, ServiceRestarter.ENTITY_RESTART_FAILED)));


        //create the benchmarking table on the Cassandra Cluster
        subscribeToMembers(cassandraCluster, CassandraCluster.SERVICE_UP, new SensorEventListener<Boolean>() {
            @Override
            public void onEvent(SensorEvent<Boolean> event) {

                if (event.getSource() instanceof CassandraNode && scriptBoolean.compareAndSet(false, true)) {

                    CassandraNode anyNode = (CassandraNode) event.getSource();
                  log.info("Creating keyspace 'usertable' with column family 'data' on Node {}", event.getSource().getId());

                    Entities.invokeEffectorWithArgs(HighAvailabilityCassandraCluster.this, anyNode, CassandraNode.EXECUTE_SCRIPT, "create keyspace usertable with placement_strategy = " +
                            "'org.apache.cassandra.locator.SimpleStrategy' and strategy_options = {replication_factor:3};" +
                            "\nuse usertable;" +
                            "\ncreate column family data;");

                    setAttribute(scriptExecuted, true);
                }

            }


        });


        //create the YCSB Entities
        addChild(EntitySpec.create(YCSBEntityCluster.class)
                .configure(YCSBEntityCluster.INITIAL_SIZE, 2)
                .configure(YCSBEntityCluster.NO_OF_RECORDS, 1000000)
                .configure(YCSBEntityCluster.MEMBER_SPEC, EntitySpec.create(YCSBEntity.class)
                        .configure(YCSBEntity.MAIN_CLASS, "com.yahoo.ycsb.Client")
                        .configure(YCSBEntity.CLASSPATH, ImmutableList.of("classpath://cassandra-binding-0.1.4.jar"
                                , "classpath://core-0.1.4.jar",
                                "classpath://workloada", "classpath://workloadb",
                                "classpath://workloadc", "classpath://workloadd",
                                "classpath://workloade", "classpath://workloadf"
                                , "classpath://slf4j-simple-1.7.5.jar")))
                .configure(YCSBEntity.HOSTNAMES, DependentConfiguration.attributeWhenReady(cassandraCluster, CassandraCluster.CASSANDRA_CLUSTER_NODES)));
        //.configure(YCSBEntity.ARGS,ImmutableList.of("-s", "-p recordcount=10000000", "-threads 10")));
    }

}
