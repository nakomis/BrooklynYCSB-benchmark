package ycsb;


import brooklyn.entity.basic.SoftwareProcess;
import brooklyn.entity.effector.EffectorBody;
import brooklyn.entity.java.VanillaJavaAppImpl;
import brooklyn.util.config.ConfigBag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class YCSBEntityImpl extends VanillaJavaAppImpl implements YCSBEntity {

    private static final Logger log = LoggerFactory.getLogger(YCSBEntityImpl.class);

    @Override
    public void init() {
        super.init();
//        getMutableEntityType().addEffector(LOAD_EFFECTOR, new EffectorBody<String>() {
//            @Override
//            public String call(ConfigBag parameters) {
//
//                return getDriver().loadWorkload((String) parameters.getStringKey("workload")).block().getStdout();
//            }
//        });
//
//        getMutableEntityType().addEffector(RUN_EFFECTOR, new EffectorBody<String>() {
//            @Override
//            public String call(ConfigBag parameters) {
//                return getDriver().runWorkload((String) parameters.getStringKey("workload")).block().getStdout();
//            }
//        });
    }

    public void loadWorkloadEffector(String workload) {

        log.info("Loading wokload {} on YCSB Entity with id: {}", workload, getId());
        YCSBEntityDriver driver = getDriver();
        driver.loadWorkload(workload);

    }

    public void runWorkloadEffector(String workload) {

        log.info("Running wokload {} on YCSB Entity with id: {}", workload, getId());
        YCSBEntityDriver driver = getDriver();
        driver.runWorkload(workload);
    }

    @Override
    public Integer getInsertStart() {
        return getAttribute(INSERT_START);
    }

    @Override
    public Integer getRecordCount() {
        return getAttribute(RECORD_COUNT);
    }

    @Override
    public String getMainClass() {
        return getConfig(MAIN_CLASS);
    }

    @Override
    public List<String> getClasspath() {
        return getConfig(CLASSPATH);
    }

    @Override
    public void kill() {
        super.kill();
    }

    @Override
    public Class<? extends YCSBEntityDriver> getDriverInterface() {
        return YCSBEntityDriver.class;
    }

    @Override
    public YCSBEntityDriver getDriver() {
        return (YCSBEntityDriver) super.getDriver();
    }

    @Override
    public void connectSensors()
    {
        connectedSensors = true;
        connectServiceUpIsRunning();

    }
}
