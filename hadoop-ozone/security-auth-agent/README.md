# security-auth-agent

A JVM Java agent that intercepts Hadoop's Kerberos-based authentication
and substitutes OAuth 2.0 / OIDC (Keycloak-tested). Loaded via
`-javaagent:` at JVM start, it works for every Ozone process and the
client CLI without changes to Ozone source.

The agent uses ByteBuddy retransformation. The shaded JAR is built with
`Premain-Class`/`Agent-Class` manifest entries and a `Boot-Class-Path`
pointing at itself, so its classes are visible to every classloader.

## Module layout

```
src/main/java/org/apache/ozone
├── SecurityAuthAgent.java           premain entry point + ByteBuddy wiring
├── config/
│   ├── AgentArgsParser.java         parses -javaagent:...=k=v,k=v
│   └── AgentConfig.java             parsed config (POJO)
├── provider/
│   ├── AuthDataProvider.java        SPI for credential sources
│   ├── DefaultAuthDataProvider.java reads ~/.hadoop-auth/config (TOML-ish)
│   ├── EnvAuthDataProvider.java     AUTH_SERVER_URL/AUTH_LOGIN/AUTH_PASSWORD
│   ├── FileAuthDataProvider.java    --auth-data-file-path
│   ├── JksAuthDataProvider.java     JKS keystore (path/password/alias)
│   ├── InteractiveAuthDataProvider.java device flow (no static creds)
│   └── JksUtil.java
├── oauth/
│   ├── OAuthClient.java             HTTP client: password grant, refresh
│   ├── OAuthToken.java              {access, refresh, expiresAt}
│   ├── OAuthTokenManager.java       cache + lifecycle + fallback token
│   ├── OAuthSaslClient.java         SASL client (PLAIN-over-Bearer)
│   ├── OAuthSaslServer.java         SASL server (validates Bearer)
│   ├── OAuthCredentialsIdentifier.java Hadoop TokenIdentifier (kind=OAUTH-CREDS)
│   ├── OAuthTokenRenewer.java       Hadoop TokenRenewer (uses refresh_token)
│   ├── OAuthCredentialsCodec.java   encode/decode identifier+password bytes
│   ├── TokenValidator.java          interface
│   ├── NoneTokenValidator.java      decode-only (JWT payload)
│   ├── JwtTokenValidator.java       signature verify via JWKS
│   ├── IntrospectTokenValidator.java RFC 7662 introspection endpoint
│   └── SimpleJsonParser.java
├── interactive/
│   ├── InteractiveAuthFlow.java     RFC 8628 device authorization grant
│   └── QrCodeGenerator.java         optional ASCII QR for verification URI
└── interceptor/
    ├── LoginInterceptor.java        UGI.loginUserFromKeytab(*)
    ├── DoAsInterceptor.java         UGI.doAs(...)
    ├── UgiToStringInterceptor.java  UGI.toString() — show auth:OAUTH
    ├── ConnectionContextInterceptor.java Client$ConnectionId.getTicket()
    ├── SaslClientInterceptor.java   SaslRpcClient.createSaslClient(...)
    ├── SaslServerInterceptor.java   SaslRpcServer.create(...)
    ├── HttpAuthInterceptor.java     KerberosAuthenticationHandler.init()
    ├── HttpAuthTypeInterceptor.java KerberosAuthenticationHandler.getType()
    ├── HttpOidcInterceptor.java     KerberosAuthenticationHandler.authenticate()
    ├── KerberosAuthenticatorInterceptor.java client-side HTTP auth
    └── SimpleConfigParser.java

src/main/resources/META-INF/services
├── org.apache.ozone.provider.AuthDataProvider
├── org.apache.hadoop.security.token.TokenIdentifier
└── org.apache.hadoop.security.token.TokenRenewer
```

## Agent arguments

Comma-separated `key=value` pairs after the JAR path. Bare keys are
treated as boolean flags (set to true).

| Key | Default | Notes |
|---|---|---|
| `auth-data-provider` | `default` | SPI name: `default`, `env`, `file`, `jks`, `interactive`, or custom |
| `auth-data-file-path` | – | path for `file` provider |
| `auth-data-jks-path` | – | path for `jks` provider |
| `auth-data-jks-password` | – | password for `jks` provider |
| `auth-data-jks-alias` | – | alias for `jks` provider |
| `auth-token-validation` | `none` | server-side validator: `none`, `jwt`, `introspect` |
| `auth-qr` | `false` | print ASCII QR for device-flow URI |
| `auth-bundle-creds` | `false` | bundle OAuth token into UGI credentials (YARN propagation) |
| `auth-offline-access` | `false` | request `scope=openid offline_access` in device flow |

Example:

```
-javaagent:/opt/hadoop/share/ozone/lib/security-auth-agent-2.2.0-SNAPSHOT.jar=auth-data-provider=env
```

## What the agent intercepts (and why)

| Class / method | Replacement | Purpose |
|---|---|---|
| `UserGroupInformation.loginUserFromKeytab(p, k)` | obtain OAuth token via provider; build a `RemoteUser` for the principal and `setLoginUser(...)` | Replaces service login; keytab files are not read |
| `UserGroupInformation.loginUserFromKeytabAndReturnUGI` | same | Some Hadoop paths return UGI rather than mutate static state |
| `UserGroupInformation.doAs(...)` | populate `OAuthTokenManager.CURRENT_ACCESS_TOKEN` ThreadLocal; capture `ugiClass` for later reflection | Carries the user's access token to the SASL handshake on the same thread |
| `UserGroupInformation.toString()` | append `auth:OAUTH` instead of `auth:KERBEROS` when the token came from us | Diagnostics — make log lines truthful |
| `SaslRpcClient.createSaslClient(...)` | return `OAuthSaslClient` that sends the current ThreadLocal access token | Outbound RPC carries Bearer over PLAIN-SASL |
| `SaslRpcServer.create(...)` | return `OAuthSaslServer` that validates the Bearer and pins the SASL principal to the JWT username | Inbound RPC verifies Bearer; rejects spoofed usernames |
| `Client$ConnectionId.getTicket()` | when the original ticket is the OS user but an OAuth UGI is cached, substitute the OAuth UGI | Stops the connection-context user from disagreeing with the SASL principal (`auth:PROXY` failures) |
| `KerberosAuthenticationHandler.init(props)` | no-op (don't load HTTP keytab) | Services start without `HTTP/...keytab` files |
| `KerberosAuthenticationHandler.getType()` | return `"oauth"` | Stops `AuthenticationFilter` from emitting `auth:KERBEROS` cookies |
| `KerberosAuthenticationHandler.authenticate(req, resp)` | if `Authorization: Bearer ...` → validate; else redirect browser to Keycloak `/auth` (OIDC code flow) | Service-to-service Bearer + interactive UI login |
| `KerberosAuthenticator.authenticate(url, token)` | send `Authorization: Bearer <current token>` to the remote service | Outgoing HTTP from server-side clients (Recon → OM snapshot, etc.) |

Two regex matchers exist (`org\.apache\.hadoop\.security_?\.SaslRpcClient`,
`org\.apache\.hadoop\.ipc_?\.Client\$ConnectionId`) so the agent matches
both standard Hadoop and Ozone's relocated `*_` copies.

## Authentication flows

### Service startup (SCM / OM / DN / S3G / Recon)

1. JVM starts with `-javaagent` and provider credentials in env (or JKS).
2. Hadoop calls `loginUserFromKeytab(service/host@REALM, /etc/.../keytab)`.
3. `LoginInterceptor` ignores the keytab path, asks `OAuthTokenManager`
   for a token using the configured provider.
4. The token's access token is stored as the JVM-wide
   `fallbackAccessToken`; a `RemoteUser(servicePrincipal, KERBEROS)` is
   installed as the login UGI so authorization checks see the full
   principal name (e.g. `om/om@EXAMPLE.COM`).

### Client CLI (`ozone sh ...`, `hdfs dfs ...`)

1. Agent loads. No static creds → `interactive` provider triggers.
2. `InteractiveAuthFlow.execute(serverUrl)`:
   - loads `~/.hadoop-auth/session` if present; if expired and a refresh
     token is on disk, refreshes silently;
   - otherwise runs RFC 8628 device authorization grant — prints the
     verification URI (and optional QR) to stderr, polls for completion.
3. On success, the access token is cached and the login UGI is replaced
   with a `RemoteUser` for the JWT `preferred_username`. The session is
   persisted to `~/.hadoop-auth/session`.

### Inbound RPC (server side)

`OAuthSaslServer` reads the Bearer token, runs the configured
`TokenValidator`, and rejects the handshake unless the JWT username
matches the SASL principal's primary component. This blocks the
"present any valid token, claim to be anyone" attack.

### Outbound RPC

`OAuthSaslClient` picks up `OAuthTokenManager.getCurrentAccessToken()`
— ThreadLocal first, then the JVM-wide fallback. The fallback is what
lets RM's `DelegationTokenRenewer` thread pool authenticate to OM even
though no `doAs` ever fired on those threads.

### Browser HTTP UI (SCM/OM/Recon/S3G)

`HttpOidcInterceptor` replaces SPNEGO. When the request lacks a Bearer
header and isn't a server-to-server call, it issues a 302 to Keycloak's
authorization endpoint (Authorization Code with PKCE), validates the
returned `code`, sets the Hadoop auth cookie, and lets the request
proceed.

### Server-to-server HTTP (Recon → OM, etc.)

`KerberosAuthenticatorInterceptor` rewrites outgoing HTTP requests:
instead of attempting SPNEGO, it sets
`Authorization: Bearer <currentAccessToken>` from the service's own
token.

## YARN job propagation

Default mode (`auth-bundle-creds=false`): rely on Hadoop's standard
delegation token flow. The submitter's `O3fsDtFetcher` fetches an
`OzoneToken` and adds it to the job's `Credentials`. AM/task containers
then authenticate to OM via SASL TOKEN (DIGEST-MD5) — no agent needed
in containers. RM's renewer thread pool opens a fresh `OzoneClient` to
call `renewDelegationToken` on OM; the agent's `fallbackAccessToken`
lets that connection authenticate as the RM service.

Bundled mode (`auth-bundle-creds=true`): the agent's
`OAuthCredentialsIdentifier` + `OAuthTokenRenewer` register via
`META-INF/services` so Hadoop picks them up by SPI. After acquiring a
token, the agent adds a `Token<OAUTH-CREDS>` to the current UGI's
credentials. YARN serializes it into the
`ApplicationSubmissionContext`, ships it to AM/task containers, and RM
renews it via Keycloak's `/token?grant_type=refresh_token` before
expiry. Use this mode when the user's OAuth identity must flow through
to AM/task containers (e.g., for direct user-authenticated access to
non-Ozone services in the job). Combine with `auth-offline-access=true`
for long-running jobs so the refresh token survives Keycloak SSO idle
timeout.

In bundled mode you also need to add `-javaagent` to the YARN container
JVM opts (see commented-out lines in `compose/ozone-oauth/docker-config`).

## Token validators

| Validator | Behaviour |
|---|---|
| `NoneTokenValidator` | parses JWT payload, returns `preferred_username` (no signature check) |
| `JwtTokenValidator` | fetches JWKS from issuer, verifies signature + claims |
| `IntrospectTokenValidator` | RFC 7662 `/introspect` round-trip per request |

The choice is per-service via `auth-token-validation=…`. `none` is fine
for closed networks where TLS to Keycloak is the trust boundary.

## Test cluster

`hadoop-ozone/dist/src/main/compose/ozone-oauth/` runs the secure
profile **without** a KDC: SCM/OM/DN/S3G/Recon plus a single Keycloak
container preloaded with realm `ozone`, clients `ozone-client` and
service principals, and users `testuser`, `testuser2`, `om`, `scm`,
`s3g`, `recon`, `dn` with verified email and empty `requiredActions`.
The agent is bind-mounted from `dist/target/.../share/ozone/lib/`. All
Kerberos keytab paths in `docker-config` point at files that don't
exist — if the agent fails to intercept, the cluster fails to start,
proving the interception is load-bearing.

The Robot smoketest `dist/src/main/smoketest/security/ozone-oauth.robot`
exercises the OAuth flow end-to-end.

## Known limitations / follow-ups

- YARN job duration is bounded by Keycloak's offline-token idle/max
  lifespan — set those generously for long jobs.
- The agent assumes a single OAuth realm and a single client id per
  JVM. Multi-tenant token caches are not implemented.
- HDFS NameNode integration is structurally identical (same SPI files,
  same interceptors) but unverified end-to-end.
- `OAuthTokenRenewer.cancel(...)` is a no-op; Keycloak `/logout` could
  be wired in.
