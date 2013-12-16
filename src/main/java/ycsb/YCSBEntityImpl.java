package ycsb;


import brooklyn.entity.java.VanillaJavaAppImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class YCSBEntityImpl extends VanillaJavaAppImpl implements YCSBEntity {

    private static final Logger log = LoggerFactory.getLogger(YCSBEntity.class);


    @Override
    public void loadWorkloadAEffector() {
        YCSBEntityDriver driver = getDriver();
        driver.loadWorkloadA();

    }

    @Override
    public void runWorkloadAEffector() {
        YCSBEntityDriver myDriver = (YCSBEntityDriver) getDriver();
    }


    @Override
    public Integer getInsertStart()
    {
        return getConfig(INSERT_START);
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
    public YCSBEntityDriver getDriver()
    {
        return (YCSBEntityDriver) super.getDriver();
    }
}
