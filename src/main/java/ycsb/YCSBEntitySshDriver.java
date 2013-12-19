package ycsb;

import brooklyn.entity.java.VanillaJavaAppImpl;
import brooklyn.entity.java.VanillaJavaAppSshDriver;
import brooklyn.entity.software.SshEffectorTasks;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.util.task.DynamicTasks;
import brooklyn.util.task.system.ProcessTaskWrapper;
import brooklyn.util.text.Strings;
import com.google.common.base.Function;
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

    public Integer getRecordCount() {
        return entity.getAttribute(YCSBEntity.RECORD_COUNT);
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

    public ProcessTaskWrapper<Integer> loadWorkload(String workload) {

        return DynamicTasks.queue(SshEffectorTasks.ssh(
                "cd " + getRunDir(),
                getLoadCmd(workload)));
//        newScript(LAUNCHING).
//                body.append(
//                format(getLoadCmd(workload))
//        ).execute();
    }

    public ProcessTaskWrapper<Integer> runWorkload(String workload) {

        return DynamicTasks.queue(SshEffectorTasks.ssh(
                "cd " + getRunDir(),
                getRunCmd(workload)));
//        newScript(LAUNCHING).
//                body.append(
//                format(getRunCmd(workload))
//        ).execute();
    }

    public String getLoadCmd(String workload) {
        String clazz = getEntity().getMainClass();
        String args = getArgs();
        String insertStart = Integer.toString(getInsertStart());
        String hostnames = getHostnames();
        String recordcount = Integer.toString(getRecordCount());

        return format("java $JAVA_OPTS -cp \"lib/*\" %s %s " +
                " -db com.yahoo.ycsb.db.CassandraClient10 -load" +
                " -P lib/" + workload + " -p insertstart=%s -s -p recordcount=%s -threads 10 " +
                "-p hosts=%s > load.dat"
                , clazz, args, insertStart, recordcount, hostnames);
    }

    public String getRunCmd(String workload) {
        String clazz = getEntity().getMainClass();
        String args = getArgs();
        String insertStart = Integer.toString(getInsertStart());
        String hostnames = getHostnames();

        return format("java $JAVA_OPTS -cp \"lib/*\" %s %s " +
                "-db com.yahoo.ycsb.db.CassandraClient10 -run" +
                "-P lib/" + workload + " -p insertstart=%s -p hosts=%s" +
                " >> %s/console 2>&1 </dev/null &", clazz, args, insertStart, hostnames, getRunDir());
    }

}
