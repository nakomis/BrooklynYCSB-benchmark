package ycsb;

import brooklyn.entity.java.VanillaJavaAppDriver;
import brooklyn.util.task.system.ProcessTaskWrapper;

public interface YCSBEntityDriver extends VanillaJavaAppDriver {

    ProcessTaskWrapper<Integer> loadWorkload(String commands);

    ProcessTaskWrapper<Integer> runWorkload(String commands);
}
