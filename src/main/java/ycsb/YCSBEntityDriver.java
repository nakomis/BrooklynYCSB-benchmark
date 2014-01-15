package ycsb;

import brooklyn.entity.java.VanillaJavaAppDriver;

public interface YCSBEntityDriver extends VanillaJavaAppDriver {

    void loadWorkload(String commands);

    void runWorkload(String commands);

    void fetchOutputs(String localpath, String workloadname);
}
