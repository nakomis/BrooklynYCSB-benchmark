package ycsb;


import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import brooklyn.entity.java.VanillaJavaAppImpl;


public class YCSBEntityImpl extends VanillaJavaAppImpl implements YCSBEntity {

    private static final Logger log = LoggerFactory.getLogger(YCSBEntityImpl.class);
    private final List<Integer> outputLoadIds = Lists.newArrayList();
    private final List<Integer> outputTransactionIds = Lists.newArrayList();

    @Override
    public void init() {
        super.init();
    }


    public void runWorkloadEffector(String workload) {

        if (Optional.fromNullable(getConfig(DB_HOSTNAMES)).isPresent()) {
            YCSBEntityDriver driver = getDriver();
            driver.runWorkload(workload);
        } else {
            throw new IllegalArgumentException("DB Hostnames to benchmark are not ready");
        }
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
