package ycsb;

import brooklyn.entity.java.VanillaJavaAppDriver;

public interface YCSBEntityDriver extends VanillaJavaAppDriver {

    void runWorkload(String commands);

}
