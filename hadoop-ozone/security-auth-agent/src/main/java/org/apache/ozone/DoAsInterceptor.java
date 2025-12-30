package org.apache.ozone;

import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.This;

import java.lang.reflect.Method;
import java.security.PrivilegedAction;
import java.util.concurrent.Callable;

// Intercepts calls to UserGroupInformation#doAs(PrivilegedAction) and
// doAs(PrivilegedExceptionAction) and performs an OAuth authentication check
public class DoAsInterceptor {

    // For PrivilegedAction and PrivilegedExceptionAction variants
    public static Object intercept(@This(optional = true) Object thiz,
                                   @AllArguments Object[] args,
                                   @Origin Method method,
                                   @SuperCall Callable<Object> zuper) throws Exception {

        // Extract the current UGI (UserGroupInformation) principal name if possible
        String ugiName = thiz.toString();

        // Perform an OAuth check before allowing the action to proceed
        System.out.println("Start to check oauth token");
        OAuthChecker.checkTokenForUser(ugiName);

        // Proceed with original method
        try {
            System.out.println("Call real method of hadoop kerberos");
            return zuper.call();
        } catch (Exception e) {
            throw e;
        }
    }
}

