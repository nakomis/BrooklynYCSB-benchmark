package ycsb;


import brooklyn.entity.java.VanillaJavaAppImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class YCSBEntityImpl extends VanillaJavaAppImpl implements YCSBEntity {

    private static final Logger log = LoggerFactory.getLogger(YCSBEntityImpl.class);

    @Override
    public void init() {
        super.init();

        if (getConfig(YCSBEntity.TIMESERIES_GRANULARITY) != null)
            setConfig(YCSBEntity.TIMESERIES, true);
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
    public void fetchOutputs(String localpath, String workloadname) {
        log.info("Fetching output files from {} to {} on local machine", getId(), localpath);
        getDriver().fetchOutputs(localpath, workloadname);
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
    public void connectSensors() {
        connectedSensors = true;
        connectServiceUpIsRunning();

    }
}
