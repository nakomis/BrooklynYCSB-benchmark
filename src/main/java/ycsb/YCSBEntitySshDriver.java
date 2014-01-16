package ycsb;

import brooklyn.entity.java.VanillaJavaAppImpl;
import brooklyn.entity.java.VanillaJavaAppSshDriver;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.util.exceptions.Exceptions;
import brooklyn.util.javalang.StackTraceSimplifier;
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

    public void loadWorkload(String workload) {


        log.info("loading script: {}", getLoadCmd(workload));
        newScript(ImmutableMap.of(), LAUNCHING)
                .failOnNonZeroResultCode()
                .body.append(getLoadCmd(workload))
                .execute();

    }

    public void runWorkload(String workload) {

        log.info("running script: {}", getRunCmd(workload));
        newScript(ImmutableMap.of(), LAUNCHING)
                .failOnNonZeroResultCode()
                .body.append(getRunCmd(workload))
                .execute();

    }

    private String getLoadCmd(String workload) {


        String toinstall = "classpath://" + workload;
        int result = install(toinstall, getRunDir() + "/" + "lib" + "/", 50);
        if (result != 0)
            throw new IllegalStateException(format("unable to install workload: %s", workload));

        String clazz = getEntity().getMainClass();
        //String args = getArgs();
        String insertStart = Integer.toString(getInsertStart());
        String insertCount = Integer.toString(getInsertCount());
        String hostnames = getHostnames();
        String operationsCount = Integer.toString(getOperationsCount());

        String recordcount = Integer.toString(getRecordCount());


        String loadcmd = String.format("java -cp \"lib/*\" %s " +
                " -db " + getDB() + " -load -P lib/" +
                workload + " -p insertstart=%s -p insertcount=%s -s -p recordcount=%s -threads 200 " +
                getTimeseries() +
                " -p operationcount=%s -p hosts=%s > load.dat"
                , clazz, insertStart, insertCount, recordcount, operationsCount, hostnames);

        return loadcmd;
    }

    private String getRunCmd(String workload) {

        String toinstall = "classpath://" + workload;
        int result = install(toinstall, getRunDir() + "/" + "lib" + "/", 50);
        if (result != 0)
            throw new IllegalStateException(format("unable to install workload: %s", workload));

        String clazz = getEntity().getMainClass();

        String hostnames = getHostnames();
        String operationsCount = Integer.toString(getOperationsCount());


        return String.format("java -cp \"lib/*\" %s " +
                " -db " + getDB() + " -t " +
                "-P lib/" + workload + " -s -threads 200" +
                " -p operationcount=%s " +
                getTimeseries() +
                " -p hosts=%s > transactions.dat"
                , clazz, operationsCount, hostnames);
    }

    private String getDB() {
        return entity.getAttribute(YCSBEntity.DB_TO_BENCHMARK);
    }

    public void fetchOutputs(String localpath, String workload) {
        log.info("Copying files to {}", localpath);
        getMachine().copyFrom(getRunDir() + "/load.dat", localpath + "/load-" + workload + "-" + entity.getId() + ".dat");
        getMachine().copyFrom(getRunDir() + "/transactions.dat", localpath + "/transactions-" + workload + "-" + entity.getId() + ".dat");

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

    private int install(String urlToInstall, String target, int numAttempts) {
        Exception lastError = null;
        int retriesRemaining = numAttempts;
        int attemptNum = 0;
        do {
            attemptNum++;
            try {
                return getMachine().installTo(resource, urlToInstall, target);
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                lastError = e;
                String stack = StackTraceSimplifier.toString(e);
                if (stack.contains("net.schmizz.sshj.sftp.RemoteFile.write")) {
                    log.warn("Failed to transfer " + urlToInstall + " to " + getMachine() + ", retryable error, attempt " + attemptNum + "/" + numAttempts + ": " + e);
                    continue;
                }
                log.warn("Failed to transfer " + urlToInstall + " to " + getMachine() + ", not a retryable error so failing: " + e);
                throw Exceptions.propagate(e);
            }
        } while (--retriesRemaining > 0);
        throw Exceptions.propagate(lastError);
    }


}
