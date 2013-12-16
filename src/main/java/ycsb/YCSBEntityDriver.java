package ycsb;

import brooklyn.entity.java.VanillaJavaAppDriver;

public interface YCSBEntityDriver extends VanillaJavaAppDriver {

    public void loadWorkloadA();
    public void runWorkloadA();
}
