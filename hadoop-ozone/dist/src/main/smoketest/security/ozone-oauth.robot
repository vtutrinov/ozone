# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

*** Settings ***
Documentation       Test OAuth authentication via security-auth-agent.
...                 This runs in a SECURE cluster (ozone.security.enabled=true)
...                 with full Kerberos config but NO KDC. The agent intercepts
...                 all Kerberos login calls and replaces them with OAuth via
...                 Keycloak. If any test passes, it proves the agent works —
...                 without the agent, Kerberos auth would fail (no KDC).
Library             OperatingSystem
Library             String
Library             BuiltIn
Resource            ../commonlib.robot
Test Timeout        5 minutes

*** Variables ***
${KEYCLOAK_URL}     http://keycloak:8080/realms/EXAMPLE.COM/protocol/openid-connect/token
# These override the scm container's service-principal env (AUTH_LOGIN=scm)
# so that ozone CLI invocations authenticate as a user, not as the scm
# service. OZONE_AGENT_LOG_LEVEL=OFF silences the agent's stdout so the
# Robot test can assert on the bare ozone CLI output (e.g. the value
# returned by `ozone getconf confKey ...`).
${AS_TESTUSER}      OZONE_AGENT_LOG_LEVEL=OFF AUTH_LOGIN=testuser AUTH_PASSWORD=testuser

*** Keywords ***
Get OAuth Token Via Curl
    [arguments]    ${user}    ${password}
    ${output} =    Execute    curl -s -X POST ${KEYCLOAK_URL} -d "grant_type=password&client_id=ozone-client&username=${user}&password=${password}" -H "Content-Type: application/x-www-form-urlencoded"
    [return]       ${output}

Run Ozone As Testuser
    [documentation]    Run an ozone CLI command with testuser OAuth
    ...                credentials and the agent's stdout silenced.
    [arguments]    ${command}
    ${output} =    Execute    ${AS_TESTUSER} ${command}
    [return]       ${output}

*** Test Cases ***
Keycloak Is Accessible
    [documentation]    Verify Keycloak is running and the ozone realm exists.
    ...                Checks the response body rather than HTTP status
    ...                to avoid Robot's collision with curl's
    ...                \%{http_code} format string syntax.
    ${output} =    Execute    curl -s http://keycloak:8080/realms/EXAMPLE.COM
    Should Contain    ${output}    "realm":"EXAMPLE.COM"

OAuth Token Can Be Obtained From Keycloak
    [documentation]    Verify direct OAuth token acquisition works
    ${output} =    Get OAuth Token Via Curl    testuser    testuser
    Should Contain    ${output}    access_token
    Should Contain    ${output}    refresh_token

Security Is Enabled
    [documentation]    Confirm the cluster is running with security enabled
    ${value} =    Run Ozone As Testuser    ozone getconf confKey ozone.security.enabled
    Should Be Equal As Strings    ${value}    true

Kerberos Is Configured
    [documentation]    Confirm Kerberos is configured (but no KDC exists)
    ${value} =    Run Ozone As Testuser    ozone getconf confKey hadoop.security.authentication
    Should Be Equal As Strings    ${value}    kerberos

Create Volume In Secure Cluster Without KDC
    [documentation]    Create a volume — this proves the agent intercepted
    ...                the Kerberos login and used OAuth instead.
    ...                Without the agent this would fail: no KDC reachable.
    ${output} =    Run Ozone As Testuser    ozone sh volume create /oauth-vol1
    Should Not Contain    ${output}    PERMISSION_DENIED
    Should Not Contain    ${output}    GSS initiate failed
    Should Not Contain    ${output}    LoginException

Create Bucket In Secure Cluster Without KDC
    [documentation]    Create a bucket — requires working auth
    ${output} =    Run Ozone As Testuser    ozone sh bucket create /oauth-vol1/oauth-bucket1
    Should Not Contain    ${output}    PERMISSION_DENIED

Put And Get Key In Secure Cluster Without KDC
    [documentation]    Write and read back a key through the full secure data path
    Execute    echo "OAuth replaces Kerberos" > /tmp/oauth-test-input.txt
    ${output} =    Run Ozone As Testuser    ozone sh key put /oauth-vol1/oauth-bucket1/test-key /tmp/oauth-test-input.txt
    Should Not Contain    ${output}    PERMISSION_DENIED
    Run Ozone As Testuser    ozone sh key get /oauth-vol1/oauth-bucket1/test-key /tmp/oauth-test-output.txt
    ${content} =    Execute    cat /tmp/oauth-test-output.txt
    Should Contain    ${content}    OAuth replaces Kerberos

List Volumes In Secure Cluster
    [documentation]    List volumes — verifies read auth path
    ${output} =    Run Ozone As Testuser    ozone sh volume list /
    Should Contain    ${output}    oauth-vol1

Clean Up
    [documentation]    Remove test data
    Execute And Ignore Error    ${AS_TESTUSER} ozone sh key delete /oauth-vol1/oauth-bucket1/test-key
    Execute And Ignore Error    ${AS_TESTUSER} ozone sh bucket delete /oauth-vol1/oauth-bucket1
    Execute And Ignore Error    ${AS_TESTUSER} ozone sh volume delete /oauth-vol1
