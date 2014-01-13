package ycsb;

import brooklyn.entity.java.VanillaJavaAppImpl;
import brooklyn.entity.java.VanillaJavaAppSshDriver;
import brooklyn.entity.software.SshEffectorTasks;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.util.MutableMap;
import brooklyn.util.task.DynamicTasks;
import brooklyn.util.task.system.ProcessTaskWrapper;
import brooklyn.util.text.Strings;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class YCSBEntitySshDriver extends VanillaJavaAppSshDriver implements YCSBEntityDriver {


    public YCSBEntitySshDriver(VanillaJavaAppImpl entity, SshMachineLocation machine) {
        super(entity, machine);
    }

    @Override
    public VanillaJavaAppImpl getEntity() {
        return super.getEntity();
    }

    @Override
    protected String getLogFileLocation() {
        return super.getLogFileLocation();
    }

    @Override
    public void install() {
        super.install();
    }

    @Override
    public void customize() {
        super.customize();
    }

    @Override
    public void launch() {


        newScript(LAUNCHING).
//                failOnNonZeroResultCode().
        body.append("pwd").
                execute();
    }

    @Override
    public String getArgs() {
        return super.getArgs();
    }

    public Integer getInsertStart() {
        return entity.getAttribute(YCSBEntity.INSERT_START);
    }

    public Integer getInsertCount()
    {
        return entity.getAttribute(YCSBEntity.INSERT_COUNT);
    }

    public Integer getRecordCount() {
        return entity.getAttribute(YCSBEntity.RECORD_COUNT);
    }

    public Integer getOperationsCount()
    {
        return entity.getAttribute(YCSBEntity.OPERATIONS_COUNT);
    }

    public String getHostnames() {
        List<String> hostnameslist = entity.getConfig(YCSBEntity.HOSTNAMES);

        //remove port from the hostname

        return Strings.join(Lists.newArrayList(Iterables.transform(hostnameslist, new Function<String, String>() {

            @Nullable
            @Override
            public String apply(@Nullable String s) {


                int portIndex = s.indexOf(":");
                return s.substring(0, portIndex);
            }
        })), ",");
    }

    @Override
    public boolean isRunning() {
        return true;
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Override
    public void kill() {
        super.kill();
    }

    @Override
    protected Map getCustomJavaSystemProperties() {
        return super.getCustomJavaSystemProperties();
    }

    public void loadWorkload(String workload) {


    log.info("loading script: {}" , getLoadCmd(workload));
        newScript(MutableMap.of(), LAUNCHING)
                .failOnNonZeroResultCode()
                .body.append(getLoadCmd(workload))
                .execute();
    }

    public void runWorkload(String workload) {


    log.info("running script: {}" , getRunCmd(workload));
        newScript(MutableMap.of(), LAUNCHING)
                .failOnNonZeroResultCode()
                .body.append(getRunCmd(workload))
                .execute();

    }

    public String getLoadCmd(String workload) {


        String clazz = getEntity().getMainClass();
        //String args = getArgs();
        String insertStart = Integer.toString(getInsertStart());
        String insertCount = Integer.toString(getInsertCount());
        String hostnames = getHostnames();

        String recordcount = Integer.toString(getRecordCount());


        String loadcmd = String.format("java -cp \"lib/*\" %s " +
                " -db com.yahoo.ycsb.db.CassandraClient10 -load" +
                " -P lib/%s -p insertstart=%s -p insertcount=%s -s -p recordcount=%s -threads 200 " +
                getTimeseries() +
                " -p hosts=%s > load.dat"
                , clazz, workload, insertStart, insertCount, recordcount, hostnames);

        return loadcmd;
    }

    public String getRunCmd(String workload) {
        String clazz = getEntity().getMainClass();
        //String args = getArgs();
        String hostnames = getHostnames();
        String operationsCount = Integer.toString(getOperationsCount());

        return String.format("java -cp \"lib/*\" %s " +
                " -db com.yahoo.ycsb.db.CassandraClient10 -t " +
                " -P lib/%s -s -threads 200" +
                " -p operationcount=%s " +
                   getTimeseries() +
                " -p hosts=%s > transactions.dat"
                , clazz, workload, operationsCount, hostnames);
    }

    public void fetchOutputs(String localpath)
    {
        log.info("Copying files to {}" , localpath);
        getMachine().copyFrom(getRunDir() + "/load.dat",localpath +"/load" + entity.getId() + ".dat");
       getMachine().copyFrom(getRunDir() + "/transactions.dat",localpath +"/transactions" + entity.getId() + ".dat");

    }

    public String getTimeseries()
    {
        StringBuffer timeseries;
        if (entity.getConfig(YCSBEntity.TIMESERIES_GRANULARITY) instanceof Integer || Boolean.TRUE.equals(entity.getConfig(YCSBEntity.TIMESERIES)))
        {
            timeseries = new StringBuffer("-p measurementtype=timeseries");
            if (entity.getConfig(YCSBEntity.TIMESERIES_GRANULARITY) instanceof Integer)
                timeseries.append(" -p timeseries.granularity=" + entity.getConfig(YCSBEntity.TIMESERIES_GRANULARITY).toString());

            return timeseries.toString();
        }

        else
            return "";
    }

}
