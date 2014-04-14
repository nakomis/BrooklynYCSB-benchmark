package ycsb;

import brooklyn.entity.java.VanillaJavaAppDriver;

import java.util.List;

public interface YCSBEntityDriver extends VanillaJavaAppDriver {

    void runWorkload(String commands);

    void fetchOutputs(String workload);
}
