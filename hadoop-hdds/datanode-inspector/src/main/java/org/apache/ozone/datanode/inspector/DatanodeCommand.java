package org.apache.ozone.datanode.inspector;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelShell;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@CommandLine.Command(
    name = "datanode-inspector",
    subcommands = {
        InspectContainers.class,
        ListContainers.class,
    },
    mixinStandardHelpOptions = true
)
public class DatanodeCommand {

    @Option(names = {"--address", "-dn"},
        description = "Datanode address")
    List<String> datanodes;
    
    @Option(names = {"--datanodeUUID", "-dnUUID"})
    List<String> datanodeUUIDs;

    @Option(names = {"--dnSshUser", "-user"},
        description = "Datanode SSH user")
    String datanodeSshUser;

    @Option(names = {"--dnSshPassword", "-pwd"},
        description = "Datanode SSH password")
    String datanodeSshPassword;

    @Option(names = {"--dnSshPrivateKey", "-pk"},
        description = "Absolute path to the datanode SSH Private Key")
    String datanodeSshPrivateKey;

    @Option(names = {"--datanodeOzoneCmdOpts", "-opts"},
        description = "Datanode 'ozone' command options ('OZONE_OPTS' env variable value)")
    String datanodeOzoneCommandOpts;

    @Option(names = {"--datanodeOzoneBinary", "-oBin"},
        description = "'ozone' command binary on datanode")
    String datanodeOzoneBinary;

    @Option(names = {"--datanodeConfig", "-cfg"},
        description = "Configuration file on datanode to run 'ozone' sub-command")
    String datanodeConf;

    @Option(names = {"--stdout-enabled", "--print-stdout", "--debug"},
        description = "Enable stdout to show remote commands call output",
        type = Boolean.class)
    boolean stdOutEnabled;

    public ChannelShell channel;

    public Channel getChannel(String host){
        if(channel == null || !channel.isConnected()){
            try{
                channel = (ChannelShell) createSession(host).openChannel("shell");
                channel.connect();
            } catch(Exception e) {
                System.out.println("Error while opening channel: "+ e);
            }
        }
        return channel;
    }

    private Session createSession(String host) throws JSchException {
        String username = datanodeSshUser != null ? datanodeSshUser :
            System.getProperty("datanode.ssh.user", System.getenv("DATANODE_SSH_USER"));
        String password = datanodeSshPassword != null ? datanodeSshPassword :
            System.getProperty("datanode.ssh.password", System.getenv("DATANODE_SSH_PASSWORD"));
        JSch jSch = new JSch();
        String sshPrivateKeyPath = "";
        if (datanodeSshPrivateKey != null) {
            sshPrivateKeyPath = datanodeSshPrivateKey;
        } else {
            String userHomeDir = System.getProperty("user.home");
            sshPrivateKeyPath = userHomeDir + "/.ssh/id_rsa";
        }
        if (Files.exists(Paths.get(sshPrivateKeyPath))) {
            jSch.addIdentity(sshPrivateKeyPath);
        }
        Session session = jSch.getSession(username, host);
        if (password != null) {
            session.setPassword(password);
        }
        session.setConfig("PreferredAuthentications",
            "publickey,keyboard-interactive,password");
        session.setConfig("StrictHostKeyChecking", "no");
        session.setConfig("UsePrivilegeSeparation", "no");
        session.setConfig("PermitUserEnvironment", "yes");
        session.connect();
        return session;
    }

    public void sendCommands(Channel channel, List<String> commands){
        try{
            PrintStream out = new PrintStream(channel.getOutputStream());
            for(String command : commands){
                out.println(command);
            }
            out.println("exit");
            out.flush();
        }catch(Exception e){
            System.out.println("Error while sending commands: "+ e);
        }
    }
    public String readChannelOutput(Channel channel){
        byte[] buffer = new byte[1024];
        StringBuilder result = new StringBuilder();
        try {
            InputStream in = channel.getInputStream();
            String line = "";
            while (true){
                while (in.available() > 0) {
                    int i = in.read(buffer, 0, 1024);
                    if (i < 0) {
                        break;
                    }
                    line = new String(buffer, 0, i);
                    result.append(line);
                }
                if(line.contains("logout")){
                    break;
                }
                if (channel.isClosed()){
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (Exception ee){}
            }
        } catch (Exception e){
            System.out.println("Error while reading channel output: "+ e);
        }
        return result.toString();
    }

    public List<DatanodeWithAttributes> getAllNodes(ScmClient scmClient)
        throws IOException {
        List<HddsProtos.Node> nodes = scmClient.queryNode(null,
            null, HddsProtos.QueryScope.CLUSTER, "");

        return nodes.stream()
            .map(p -> new DatanodeWithAttributes(
                DatanodeDetails.getFromProtoBuf(p.getNodeID()),
                p.getNodeOperationalStates(0), p.getNodeStates(0)))
            .sorted(Comparator.comparing(o -> o.healthState))
            .collect(Collectors.toList());
    }

    public String executeCommands(String host, List commands) throws JSchException {
        try{
            Channel channel=getChannel(host);
            sendCommands(channel, commands);
            String commandExecutionResult = readChannelOutput(channel);
            if (stdOutEnabled) {
                System.out.println(commandExecutionResult);
            }
            return commandExecutionResult;
        } catch (Exception e){
            System.out.println("An error occurred during executeCommands: "+e);
        } finally {
            channel.disconnect();
            channel.getSession().disconnect();
        }
        return null;
    }

    public StringBuilder buildDebugCommand() {
        return new Debug.Builder()
            .withOzoneOpts(datanodeOzoneCommandOpts)
            .withOzoneBinary(datanodeOzoneBinary)
            .withConfFile(datanodeConf)
            .build().stringBuilder();
    }

    public static class Debug {

        private final String ozoneOpts;

        private final String ozoneBinary;

        private final String confFile;

        private final String debugSubcommand;

        public Debug(String ozoneOpts, String ozoneBinary, String confFile, String debugSubcommand) {
            this.ozoneOpts = ozoneOpts;
            this.ozoneBinary = ozoneBinary;
            this.confFile = confFile;
            this.debugSubcommand = debugSubcommand;
        }

        public StringBuilder stringBuilder() {
            StringBuilder commandStringBuilder = new StringBuilder();
            if (ozoneOpts != null) {
                commandStringBuilder.append("OZONE_OPTS=\"").append(ozoneOpts).append("\" ");
            }
            if (ozoneBinary != null) {
                commandStringBuilder.append(ozoneBinary);
            } else {
                commandStringBuilder.append("ozone");
            }
            commandStringBuilder.append(" debug ");
            if (confFile != null) {
                commandStringBuilder.append("-conf=").append(confFile).append(" ");
            }
            if (debugSubcommand != null) {
                commandStringBuilder.append(debugSubcommand);
            }
            return commandStringBuilder;
        }

        public static class Builder {

            private String ozoneOpts;

            private String ozoneBinary;

            private String confFile;

            private String debugSubcommand;

            public Builder withOzoneOpts(String ozoneOpts) {
                this.ozoneOpts = ozoneOpts;
                return this;
            }

            public Builder withOzoneBinary(String ozoneBinary) {
                this.ozoneBinary = ozoneBinary;
                return this;
            }

            public Builder withConfFile(String confFile) {
                this.confFile = confFile;
                return this;
            }

            public Builder withDebugSubcommand(String debugSubcommand) {
                this.debugSubcommand = debugSubcommand;
                return this;
            }

            public Debug build() {
                return new Debug(ozoneOpts, ozoneBinary, confFile, debugSubcommand);
            }

        }

    }

    public static class DatanodeWithAttributes {
        private DatanodeDetails datanodeDetails;
        private HddsProtos.NodeOperationalState operationalState;
        private HddsProtos.NodeState healthState;

        DatanodeWithAttributes(DatanodeDetails dn,
                               HddsProtos.NodeOperationalState opState,
                               HddsProtos.NodeState healthState) {
            this.datanodeDetails = dn;
            this.operationalState = opState;
            this.healthState = healthState;
        }

        public DatanodeDetails getDatanodeDetails() {
            return datanodeDetails;
        }

        public HddsProtos.NodeOperationalState getOpState() {
            return operationalState;
        }

        public HddsProtos.NodeState getHealthState() {
            return healthState;
        }
    }

}
