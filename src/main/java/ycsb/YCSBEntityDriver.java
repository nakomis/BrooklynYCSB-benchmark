package ycsb;

import brooklyn.entity.java.VanillaJavaAppDriver;

import java.util.List;

public interface YCSBEntityDriver extends VanillaJavaAppDriver {

    void loadWorkload(String commands, int id);

    void runWorkload(String commands, int id);

    void fetchOutputs(String localpath, List<Integer> loadoutputs, List<Integer> transactionoutputs);
}
