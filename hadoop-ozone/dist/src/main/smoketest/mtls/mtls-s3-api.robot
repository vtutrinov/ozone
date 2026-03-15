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
Documentation       Tests that S3 API works via HTTPS without client certificate (AWS Signature V4 auth),
...                 while S3G admin endpoints (/conf, /prom) are still protected by mTLS.
Library             OperatingSystem
Library             String
Library             Collections
Library             BuiltIn
Resource            ../commonlib.robot
Resource            ../s3/commonawslib.robot
Test Timeout        5 minutes

*** Variables ***
${S3G_HTTPS_URL}          https://s3g:9879
${ENDPOINT_URL}           https://s3g:9879
${CERTS_DIR}              /etc/certs
${CA_CERT}                ${CERTS_DIR}/ca.crt
${ALLOWED_CERT}           ${CERTS_DIR}/client-allowed.crt
${ALLOWED_KEY}            ${CERTS_DIR}/client-allowed.key
${MTLS_BUCKET}            mtls-test-bucket
${TEST_FILE}              /tmp/testfile.txt
${DOWNLOAD_FILE}          /tmp/downloaded.txt

*** Keywords ***
Wait for S3G HTTPS
    [Documentation]     Wait until S3G HTTPS port responds.
    Wait Until Keyword Succeeds    120sec    5sec    S3G port is open

S3G port is open
    ${rc}    ${output} =    Run And Return Rc And Output    curl --cacert ${CA_CERT} --connect-timeout 5 -s -o /dev/null -w '\%{http_code}' ${S3G_HTTPS_URL}/ || true
    Should Not Be Equal As Strings    ${output}    000    S3G not reachable yet

mTLS request should return
    [Arguments]         ${url}    ${cert}    ${key}    ${expected_code}
    ${result} =         Execute    curl --cacert ${CA_CERT} --cert ${cert} --key ${key} --write-out '\%{http_code}' --silent --show-error --output /dev/null ${url}
    Should Be Equal As Strings    ${result}    ${expected_code}

Request without cert should be rejected
    [Arguments]         ${url}
    ${rc}    ${output} =    Run And Return Rc And Output    curl --cacert ${CA_CERT} --write-out '\%{http_code}' --silent --show-error --output /dev/null ${url}
    Should Not Be Equal As Strings    ${output}    200

*** Test Cases ***
S3 API operations work without client certificate
    [Documentation]     Verify that S3 API operations (create bucket, put/get object) work
    ...                 over HTTPS without a client certificate, using AWS Signature V4 auth.
    Wait for S3G HTTPS
    Install aws cli
    Kinit test user     testuser    testuser.keytab
    Setup secure v4 headers
    Execute             aws configure set default.s3.addressing_style path
    # AWS CLI must skip server cert verification for self-signed certs
    Set Environment Variable    AWS_CA_BUNDLE    ${CA_CERT}
    # Create test file
    Execute             echo "mtls-s3-test-content" > ${TEST_FILE}
    # Create bucket
    ${result} =         Execute    aws s3api create-bucket --bucket ${MTLS_BUCKET} --endpoint-url ${S3G_HTTPS_URL}
    Log                 Create bucket result: ${result}
    Should Contain      ${result}    ${MTLS_BUCKET}
    # Put object
    ${result} =         Execute    aws s3api put-object --bucket ${MTLS_BUCKET} --key testobj --body ${TEST_FILE} --endpoint-url ${S3G_HTTPS_URL}
    Log                 Put object result: ${result}
    # Get object
    ${result} =         Execute    aws s3api get-object --bucket ${MTLS_BUCKET} --key testobj ${DOWNLOAD_FILE} --endpoint-url ${S3G_HTTPS_URL}
    Log                 Get object result: ${result}
    # Verify content matches
    ${uploaded} =       Execute    cat ${TEST_FILE}
    ${downloaded} =     Execute    cat ${DOWNLOAD_FILE}
    Should Be Equal     ${uploaded}    ${downloaded}

S3G admin /conf endpoint requires mTLS
    [Documentation]     Verify that /conf on S3G still requires a valid client certificate.
    mTLS request should return    ${S3G_HTTPS_URL}/conf    ${ALLOWED_CERT}    ${ALLOWED_KEY}    200
    Request without cert should be rejected    ${S3G_HTTPS_URL}/conf

S3G admin /prom endpoint requires mTLS
    [Documentation]     Verify that /prom on S3G still requires a valid client certificate.
    mTLS request should return    ${S3G_HTTPS_URL}/prom    ${ALLOWED_CERT}    ${ALLOWED_KEY}    200
    Request without cert should be rejected    ${S3G_HTTPS_URL}/prom
