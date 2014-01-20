package ycsb;


import brooklyn.entity.java.VanillaJavaAppImpl;
import brooklyn.util.text.Identifiers;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class YCSBEntityImpl extends VanillaJavaAppImpl implements YCSBEntity {

    private static final Logger log = LoggerFactory.getLogger(YCSBEntityImpl.class);
    private final List<Integer> outputLoadIds = Lists.newArrayList();
    private final List<Integer> outputTransactionIds = Lists.newArrayList();

    @Override
    public void init() {
        super.init();


        if (getConfig(YCSBEntity.TIMESERIES_GRANULARITY) != null)
            setConfig(YCSBEntity.TIMESERIES, true);
    }

    public void loadWorkloadEffector(String workload) {

        int id = Identifiers.randomInt();

        log.info("Loading wokload {} on YCSB Entity with id: {}", workload, getId());
        YCSBEntityDriver driver = getDriver();

        //add the load file id to the outputIds List
        outputLoadIds.add(id);
        driver.loadWorkload(workload, id);

    }

    public void runWorkloadEffector(String workload) {
        int id = Identifiers.randomInt();

        log.info("Running wokload {} on YCSB Entity with id: {}", workload, getId());
        YCSBEntityDriver driver = getDriver();
        outputTransactionIds.add(id);
        driver.runWorkload(workload, id);
    }

    @Override
    public void fetchOutputs(String localpath) {
        log.info("Fetching output files from {} to {} on local machine", getId(), localpath);
        getDriver().fetchOutputs(localpath, outputLoadIds, outputTransactionIds);
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
