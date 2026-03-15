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
Documentation       mTLS client certificate validation tests for Ozone HTTPS endpoints.
...                 Tests CertVerifyAgentFilter with allowed, revoked, and unknown client certificates
...                 against /conf and /prom endpoints of all services (OM, SCM, S3G, Recon, Datanode).
Library             OperatingSystem
Library             String
Library             BuiltIn
Resource            ../commonlib.robot
Test Timeout        5 minutes

*** Variables ***
${CERTS_DIR}              /etc/certs
${CA_CERT}                ${CERTS_DIR}/ca.crt
${ALLOWED_CERT}           ${CERTS_DIR}/client-allowed.crt
${ALLOWED_KEY}            ${CERTS_DIR}/client-allowed.key
${REVOKED_CERT}           ${CERTS_DIR}/client-revoked.crt
${REVOKED_KEY}            ${CERTS_DIR}/client-revoked.key
${UNKNOWN_CERT}           ${CERTS_DIR}/client-unknown.crt
${UNKNOWN_KEY}            ${CERTS_DIR}/client-unknown.key

${OM_URL}                 https://om1:9875
${SCM_URL}                https://scm1.org:9877
${S3G_URL}                https://s3g:9879
${RECON_URL}              https://recon:9889
${DN_URL}                 https://datanode:9883

*** Keywords ***
Wait for HTTPS endpoint
    [Documentation]     Wait until the HTTPS endpoint responds with the allowed client certificate.
    [Arguments]         ${url}
    Wait Until Keyword Succeeds    120sec    5sec    Check endpoint available    ${url}

Check endpoint available
    [Documentation]     Verify that the endpoint is reachable and returns 200 with the allowed cert.
    [Arguments]         ${url}
    ${result} =         Execute    curl --cacert ${CA_CERT} --cert ${ALLOWED_CERT} --key ${ALLOWED_KEY} --write-out '\%{http_code}' --silent --show-error --output /dev/null ${url}
    Should Be Equal As Strings    ${result}    200

mTLS request should return
    [Documentation]     Perform HTTPS request with client certificate and assert the expected HTTP status code.
    [Arguments]         ${url}    ${cert}    ${key}    ${expected_code}
    ${result} =         Execute    curl --cacert ${CA_CERT} --cert ${cert} --key ${key} --write-out '\%{http_code}' --silent --show-error --output /dev/null ${url}
    Should Be Equal As Strings    ${result}    ${expected_code}

Request without cert should be rejected
    [Documentation]     Perform HTTPS request without a client certificate — should not return 200.
    [Arguments]         ${url}
    ${rc}    ${output} =    Run And Return Rc And Output    curl --cacert ${CA_CERT} --write-out '\%{http_code}' --silent --show-error --output /dev/null ${url}
    Should Not Be Equal As Strings    ${output}    200

Verify mTLS on endpoint
    [Documentation]     Run all four certificate scenarios against a single URL.
    [Arguments]         ${url}
    Wait for HTTPS endpoint    ${url}
    mTLS request should return    ${url}    ${ALLOWED_CERT}    ${ALLOWED_KEY}    200
    mTLS request should return    ${url}    ${REVOKED_CERT}    ${REVOKED_KEY}    403
    mTLS request should return    ${url}    ${UNKNOWN_CERT}    ${UNKNOWN_KEY}    403
    Request without cert should be rejected    ${url}

*** Test Cases ***
OM /conf endpoint mTLS validation
    Verify mTLS on endpoint    ${OM_URL}/conf

OM /prom endpoint mTLS validation
    Verify mTLS on endpoint    ${OM_URL}/prom

SCM /conf endpoint mTLS validation
    Verify mTLS on endpoint    ${SCM_URL}/conf

SCM /prom endpoint mTLS validation
    Verify mTLS on endpoint    ${SCM_URL}/prom

S3G /conf endpoint mTLS validation
    Verify mTLS on endpoint    ${S3G_URL}/conf

S3G /prom endpoint mTLS validation
    Verify mTLS on endpoint    ${S3G_URL}/prom

Recon /conf endpoint mTLS validation
    Verify mTLS on endpoint    ${RECON_URL}/conf

Recon /prom endpoint mTLS validation
    Verify mTLS on endpoint    ${RECON_URL}/prom

Datanode /conf endpoint mTLS validation
    Verify mTLS on endpoint    ${DN_URL}/conf

Datanode /prom endpoint mTLS validation
    Verify mTLS on endpoint    ${DN_URL}/prom
