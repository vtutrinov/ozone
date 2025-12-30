package org.apache.ozone;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.implementation.MethodDelegation;
import static net.bytebuddy.matcher.ElementMatchers.*;

import java.lang.instrument.Instrumentation;

// Lightweight Java agent that instruments Hadoop's
// org.apache.hadoop.security.UserGroupInformation#doAs(...) methods
// and delegates to a DoAsInterceptor which performs an OAuth check
public class SecurityAuthAgent {

    // Called when the JVM starts with -javaagent:... option
    public static void premain(String agentArgs, Instrumentation inst) {
        installAgent(inst);
    }

    // Called when the agent is attached at runtime
    public static void agentmain(String agentArgs, Instrumentation inst) {
        installAgent(inst);
    }

    private static void installAgent(Instrumentation inst) {
      try {
        new AgentBuilder.Default()
            .ignore(nameStartsWith("net.bytebuddy."))
            .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
//            .with(AgentBuilder.Listener.StreamWriting.toSystemOut())
            .ignore(none())
            .type(named("org.apache.hadoop.security.UserGroupInformation"))
            .transform((builder, typeDescription, classLoader, module,
                        protectionDomain) ->
                builder.method(named("doAs").and(takesArguments(1)))
                    .intercept(MethodDelegation.to(DoAsInterceptor.class))
            ).installOn(inst);
        System.out.println("Installed Hadoop security auth agent");
      } catch (Throwable t) {
        t.printStackTrace();
        System.out.println("Failed to install agent!");
      }
    }

}
