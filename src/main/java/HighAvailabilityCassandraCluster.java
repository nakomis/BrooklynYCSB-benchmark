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

import brooklyn.config.ConfigKey;
import brooklyn.entity.basic.ConfigKeys;
import brooklyn.entity.basic.AbstractApplication;
import brooklyn.entity.basic.Entities;
import brooklyn.entity.basic.StartableApplication;
import brooklyn.entity.nosql.cassandra.CassandraCluster;
import brooklyn.entity.nosql.cassandra.CassandraNode;
import brooklyn.entity.proxying.EntitySpec;
import brooklyn.event.basic.DependentConfiguration;
import brooklyn.launcher.BrooklynLauncher;
import brooklyn.policy.PolicySpec;
import brooklyn.policy.ha.ServiceFailureDetector;
import brooklyn.policy.ha.ServiceReplacer;
import brooklyn.policy.ha.ServiceRestarter;
import brooklyn.util.CommandLineUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import ycsb.YCSBEntity;
import ycsb.YCSBEntityCluster;


public class HighAvailabilityCassandraCluster extends AbstractApplication {

    public static final String DEFAULT_LOCATION_SPEC = "aws-ec2:us-east-1";
    public static final ConfigKey<Integer> NUM_AVAILABILITY_ZONES = ConfigKeys.newConfigKey(
            "cassandra.cluster.numAvailabilityZones", "Number of availability zones to spread the cluster across", 1);
    public static final ConfigKey<Integer> CASSANDRA_CLUSTER_SIZE = ConfigKeys.newConfigKey(
            "cassandra.cluster.initialSize", "Initial size of the Cassandra cluster", 2);

    public static void main(String[] argv) {
        List<String> args = Lists.newArrayList(argv);
        String port =  CommandLineUtil.getCommandLineOption(args, "--port", "8081+");
        String location = CommandLineUtil.getCommandLineOption(args, "--location", DEFAULT_LOCATION_SPEC);

        BrooklynLauncher launcher = BrooklynLauncher.newInstance()
                .application(EntitySpec.create(StartableApplication.class, HighAvailabilityCassandraCluster.class)
                        .displayName("Cassandra"))
                .webconsolePort(port)
                .location(location)
                .start();

        Entities.dumpInfo(launcher.getApplications());
    }

    @Override
    public void init() {
//        CassandraCluster cluster = addChild(EntitySpec.create(CassandraCluster.class)
//                .configure(CassandraCluster.CLUSTER_NAME, "Brooklyn")
//                .configure(CassandraCluster.INITIAL_SIZE, getConfig(CASSANDRA_CLUSTER_SIZE))
//                        //See https://github.com/brooklyncentral/brooklyn/issues/973
//                        //.configure(CassandraCluster.AVAILABILITY_ZONE_NAMES, ImmutableList.of("us-east-1b", "us-east-1c", "us-east-1e"))
//                .configure(CassandraCluster.ENDPOINT_SNITCH_NAME, "GossipingPropertyFileSnitch")
//                .configure(CassandraCluster.MEMBER_SPEC, EntitySpec.create(CassandraNode.class)
//                        .policy(PolicySpec.create(ServiceFailureDetector.class))
//                        .policy(PolicySpec.create(ServiceRestarter.class)
//                                .configure(ServiceRestarter.FAILURE_SENSOR_TO_MONITOR, ServiceFailureDetector.ENTITY_FAILED)))
//                .policy(PolicySpec.create(ServiceReplacer.class)
//                        .configure(ServiceReplacer.FAILURE_SENSOR_TO_MONITOR, ServiceRestarter.ENTITY_RESTART_FAILED)));



        addChild(EntitySpec.create(YCSBEntityCluster.class)
                .configure(YCSBEntityCluster.INITIAL_SIZE,4)
                .configure(YCSBEntityCluster.NO_OF_RECORDS,1000000)
                .configure(YCSBEntityCluster.MEMBER_SPEC, EntitySpec.create(YCSBEntity.class)
                .configure(YCSBEntity.MAIN_CLASS, "com.yahoo.ycsb.Client")
                .configure(YCSBEntity.CLASSPATH, ImmutableList.of("classpath://cassandra-binding-0.1.4.jar", "classpath://core-0.1.4.jar", "classpath://workloada", "classpath://slf4j-simple-1.7.5.jar"))));
                        //.configure(YCSBEntity.HOSTNAMES, DependentConfiguration.attributeWhenReady(cluster, CassandraCluster.CASSANDRA_CLUSTER_NODES))
        //.configure(YCSBEntity.ARGS,ImmutableList.of("-s", "-p recordcount=10000000", "-threads 10")));
    }

}
