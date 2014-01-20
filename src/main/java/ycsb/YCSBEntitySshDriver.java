package ycsb;

import brooklyn.entity.java.VanillaJavaAppImpl;
import brooklyn.entity.java.VanillaJavaAppSshDriver;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.util.text.Strings;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

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

    public Integer getInsertCount() {
        return entity.getAttribute(YCSBEntity.INSERT_COUNT);
    }

    public Integer getRecordCount() {
        return entity.getAttribute(YCSBEntity.RECORD_COUNT);
    }

    public Integer getOperationsCount() {
        return entity.getAttribute(YCSBEntity.OPERATIONS_COUNT);
    }

    public String getHostnames() {
        List<String> hostnameslist = entity.getConfig(YCSBEntity.HOSTNAMES);

        //remove port section from the hostname

        return Strings.join(Lists.newArrayList(Iterables.transform(hostnameslist, new Function<String, String>() {

            @Nullable
            @Override
            public String apply(@Nullable String s) {

                if (s.contains(":")) {
                    int portIndex = s.indexOf(":");
                    return s.substring(0, portIndex);
                } else
                    return s;
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

    public void loadWorkload(String workload, int id) {

        //copy the workload file to the YCSBClient
        String toinstall = "classpath://" + workload;
        int result = install(toinstall, getRunDir() + "/" + "lib" + "/", 50);
        if (result != 0)
            throw new IllegalStateException(format("unable to install workload: %s", workload));

        log.info("loading script: {}", getLoadCmd(workload, id));
        newScript(ImmutableMap.of(), LAUNCHING)
                .failOnNonZeroResultCode()
                .body.append(getLoadCmd(workload, id))
                .execute();

    }

    public void runWorkload(String workload, int id) {


        //copy the workload file to the YCSBClient
        String toinstall = "classpath://" + workload;
        int result = install(toinstall, getRunDir() + "/" + "lib" + "/", 50);
        if (result != 0)
            throw new IllegalStateException(format("unable to install workload: %s", workload));

        log.info("running script: {}", getRunCmd(workload, id));
        newScript(ImmutableMap.of(), LAUNCHING)
                .failOnNonZeroResultCode()
                .body.append(getRunCmd(workload, id))
                .execute();

    }

    private String getLoadCmd(String workload, int id) {


        String clazz = getEntity().getMainClass();
        String insertStart = Integer.toString(getInsertStart());
        String insertCount = Integer.toString(getInsertCount());
        String hostnames = getHostnames();
        String operationsCount = Integer.toString(getOperationsCount());

        String recordcount = Integer.toString(getRecordCount());


        String loadcmd = String.format("java -cp \"lib/*\" %s " +
                " -db " + getDB() + " -load -P lib/" +
                workload + " -p insertstart=%s -p insertcount=%s -s -p recordcount=%s -threads 200 " +
                getTimeseries() +
                " -p operationcount=%s -p hosts=%s > load-" + id + ".dat"
                , clazz, insertStart, insertCount, recordcount, operationsCount, hostnames);

        return loadcmd;
    }

    private String getRunCmd(String workload, int id) {



        String clazz = getEntity().getMainClass();

        String hostnames = getHostnames();
        String operationsCount = Integer.toString(getOperationsCount());


        return String.format("java -cp \"lib/*\" %s " +
                " -db " + getDB() + " -t " +
                "-P lib/" + workload + " -s -threads 200" +
                " -p operationcount=%s " +
                getTimeseries() +
                " -p hosts=%s > transactions-" + id + ".dat"
                , clazz, operationsCount, hostnames);
    }

    private String getDB() {
        return entity.getAttribute(YCSBEntity.DB_TO_BENCHMARK);
    }

    public void fetchOutputs(String localpath, List<Integer> loadIds, List<Integer> transactionIds) {
        log.info("Copying files to {}", localpath);

        if (!loadIds.isEmpty()) {
            for (Integer i : loadIds)
                getMachine().copyFrom(getRunDir() + "/load-" + i + ".dat", localpath + "/load-" + i + "-" + entity.getId() + ".dat");

        }


        if (!transactionIds.isEmpty()) {
            for (Integer i : transactionIds)
                getMachine().copyFrom(getRunDir() + "/transactions-" + i + ".dat", localpath + "/transactions-" + i + "-" + entity.getId() + ".dat");

        }


    }

    public String getTimeseries() {
        StringBuffer timeseries;
        if (entity.getConfig(YCSBEntity.TIMESERIES_GRANULARITY) instanceof Integer || Boolean.TRUE.equals(entity.getConfig(YCSBEntity.TIMESERIES))) {
            timeseries = new StringBuffer("-p measurementtype=timeseries");
            if (entity.getConfig(YCSBEntity.TIMESERIES_GRANULARITY) instanceof Integer)
                timeseries.append(" -p timeseries.granularity=" + entity.getConfig(YCSBEntity.TIMESERIES_GRANULARITY).toString());

            return timeseries.toString();
        } else
            return "";
    }




}
