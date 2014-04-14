package ycsb;

import static java.lang.String.format;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import brooklyn.entity.java.VanillaJavaAppImpl;
import brooklyn.entity.java.VanillaJavaAppSshDriver;
import brooklyn.entity.software.SshEffectorTasks;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.util.os.Os;
import brooklyn.util.task.DynamicTasks;
import brooklyn.util.text.Strings;

public class YCSBEntitySshDriver extends VanillaJavaAppSshDriver implements YCSBEntityDriver {


    public YCSBEntitySshDriver(VanillaJavaAppImpl entity, SshMachineLocation machine) {
        super(entity, machine);
    }

    @Override
    public YCSBEntityImpl getEntity() {
        return (YCSBEntityImpl) entity;
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
        newScript(LAUNCHING)
                .body.append("pwd")
                .execute();
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

    public void runWorkload(String workload) {

        //copy the workload file to the YCSBClient
        String localWorloadFile = "classpath://" + workload;

        try {
            InputStream workloadStream = new FileInputStream(localWorloadFile);
            String workloadPath = Os.mergePaths(getRunDir(), "lib", workload);

            DynamicTasks.queueIfPossible(SshEffectorTasks.put(workloadPath)
                    .contents(workloadStream)
                    .summary("copying the workload file across to the client machine")
                    .machine(getMachine())
                    .newTask());

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        log.info("loading script with workload: {}", workload);
        newScript("Loading the workload")
                .failOnNonZeroResultCode()
                .body.append(getLoadCmd(workload))
                .execute();


        log.info("running the transactions on workload: {}", workload);
        newScript("Running the workload")
                .failOnNonZeroResultCode()
                .body.append(getRunCmd(workload))
                .execute();

    }

    private String getLoadCmd(String workload) {

        String coreWorkloadClass = getEntity().getMainClass();
        String insertStart = Integer.toString(getInsertStart());
        String insertCount = Integer.toString(getInsertCount());
        String hostnames = getHostnames();
        String operationsCount = Integer.toString(getOperationsCount());

        String recordcount = Integer.toString(getRecordCount());


        String loadcmd = String.format("java -cp \"lib/*\" %s " +
                " -db " + getDB() + " -load -P lib/" +
                workload + " -p insertstart=%s -p insertcount=%s -s -p recordcount=%s -threads 500 " +
                " -p operationcount=%s -p hosts=%s | tee load-" + workload + ".dat"
                , coreWorkloadClass, insertStart, insertCount, recordcount, operationsCount, hostnames);

        return loadcmd;
    }

    private String getRunCmd(String workload) {

        String coreWorkloadClass = getEntity().getMainClass();

        String hostnames = getHostnames();
        String operationsCount = Integer.toString(getOperationsCount());


        return String.format("java -cp \"lib/*\" %s " +
                " -db " + getDB() + " -t " +
                "-P lib/" + workload + " -s -threads 500" +
                " -p operationcount=%s " +
                " -p hosts=%s | tee transactions-" + workload + ".dat"
                , coreWorkloadClass, operationsCount, hostnames);
    }

    private String getDB() {
        return entity.getConfig(YCSBEntity.DB_TO_BENCHMARK);
    }

    public void fetchOutputs(String workload) {

        String localOutPutPath = entity.getConfig(YCSBEntity.LOCAL_OUTPUT_PATH);
        log.info("Copying load and run output files to {} for workload: {}", localOutPutPath, workload);

        DynamicTasks.queueIfPossible(SshEffectorTasks.fetch(format("load-%s.dat", workload)).machine(getMachine()).newTask());
        DynamicTasks.queueIfPossible(SshEffectorTasks.fetch(format("transactions-%s.dat", workload)).machine(getMachine()).newTask());
    }


}
