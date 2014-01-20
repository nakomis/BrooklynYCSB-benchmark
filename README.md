BrooklynYCSB-benchmark
======================

a YCSB entity with a Cassandra Cluster example.

A Brooklyn.io (https://github.com/brooklyncentral/brooklyn) Entity that manages a cluster of 
Y!CSB (https://github.com/brianfrankcooper/YCSB) to automate and
scale benchmark client and to deploy the same benchmark over multiple cloud vendors.

--- Usage ---
$ export BROOKLYN_CLASSPATH=/path/to/target/  //Where the HighAvailabilityCassandraCluster.class is located.
$ brooklyn launch --app HighAvailabilityCassandraCluster --location aws-ec2:us-west-2 //brooklyn locations

