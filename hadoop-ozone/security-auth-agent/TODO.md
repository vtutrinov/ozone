1. Make a java-agent that catch kerberos related action on JVM and replace
it with oauth one. For example, when a kerberos login action is called, the agent will call the oauth login action 
instead and return the result to caller.
2. The agent will be used in Ozone client and server side (trough redefining OZONE_*OPTS), so that both of them can 
use oauth to authenticate with each other. The agent will be added to Ozone startup script, so that it can be easily 
used in different environment, such as local, k8s and docker.
3. Also take a look to delegation token functionality and try to fix it too
4. Ranger plugin and ranger server also should support oauth authentication (through our implemented agent,
that will be user by -javaagent JVM startup option)
5. OAuth credentials (login/password to get oauth-token and refresh token too) should be in ${HOME}/.hadoop-auth/config
file by default in a following format:
```toml
[auth]
server_url=https://oauth.server.address/token

[cred]
login=testuser
password=my-secret-password

```
If the `[cred]` section is empty that the url to the authentication form should be printed to the client console and after
authentication is performed the token and refresh token of oauth should be placed to the user current session. Also (configurable)
the QR-code would be printed to authenticate the session through mobile device.
The section above describes the default way to provide auth data (agent option: --auth-data-provider=default), but the following
providers should be implemented too:
* `FileAuthDataProvider` - read auth data from file (configurable path), configurable through agent option:
`--auth-data-provider=file`, the file path should be provided through agent option: `--auth-data-file-path=/path/to/auth/data/file`
* `EnvAuthDataProvider` - read auth data from environment variables, configurable through agent option: `--auth-data-provider=env`, 
the environment variables are: AUTH_SERVER_URL, AUTH_LOGIN, AUTH_PASSWORD
* `JksAuthDataProvider` - read auth data from JKS keystore, configurable through agent option: `--auth-data-provider=jks`, 
the keystore path should be provided through agent option: `--auth-data-jks-path=/path/to/jks/keystore`,
the keystore password should be provided through agent option: `--auth-data-jks-password=keystore-password`,
the alias of the key should be provided through agent option: `--auth-data-jks-alias=key-alias`
* Option to write user specific auth provider, implementing the certain Java interface and provide its name through default method, the provided
name can be past to JVM arg for java agent to determine the way of getting user credentials. e.g. `--auth-data-provider=vault` (as all the providers above, e.g., env, default, jks, etc.)
So, an each provider should expose its name through a method, e.g., `getName()`, and the agent should use this method
to determine which provider to use based on the JVM arg passed to it. For example, if the user passes `--auth-data-provider=env`,
the agent should look for a provider that returns `env` from its `getName()` method and use it to get the auth data.
