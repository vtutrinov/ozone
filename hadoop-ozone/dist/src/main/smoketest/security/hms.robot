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
Documentation    Hive Metastore (HMS) + HiveServer2 end-to-end via
...              OAuth-only auth. test.sh drives the DDL through
...              HS2 from the host (scm doesn't ship beeline); this
...              robot only asserts the Ozone-side state — that the
...              warehouse dirs HMS produced via the OAuth-auth
...              filesystem adapter actually materialised on ofs://.
Library          OperatingSystem
Resource         ../commonlib.robot
Test Timeout     2 minute

*** Variables ***
${HMS_HOST}       hms
${HMS_PORT}       9083
${HS2_HOST}       hiveserver2
${HS2_PORT}       10000
# The following three come from test.sh via -v on the robot CLI.
${WAREHOUSE}      ofs://om/volume1/bucket1/warehouse
${TEST_DB}        oauthhive_smoketest
${TEST_TABLE}     greetings

*** Keywords ***
Ozone Path Exists
    [arguments]    ${path}
    # OZONE_AGENT_LOG_LEVEL=OFF keeps the agent's banner out of
    # stdout so the captured output is just OK / MISSING.
    ${result} =    Execute    OZONE_AGENT_LOG_LEVEL=OFF ozone fs -test -d ${path} && echo OK || echo MISSING
    [return]       ${result}

*** Test Cases ***

HMS Thrift Port Is Up
    [documentation]    Verify HMS is listening on its thrift port.
    ...                Different ncat builds print either "succeeded"
    ...                or "Connected to" on success; we match the
    ...                common substring.
    ${output} =    Execute    nc -zv ${HMS_HOST} ${HMS_PORT} 2>&1
    Should Contain    ${output}    Connected to

HiveServer2 Is Up
    [documentation]    HS2 thrift JDBC port is reachable. (The agent
    ...                presence is implicitly verified by the
    ...                Database Dir / Table Dir tests below — they
    ...                only succeed when the agent has replaced
    ...                HMS's keytab login with an OAuth identity
    ...                that can authenticate to OM.)
    ${output} =    Execute    nc -zv ${HS2_HOST} ${HS2_PORT} 2>&1
    Should Contain    ${output}    Connected to

Database Dir Materialised On Ozone
    [documentation]    test.sh issued CREATE DATABASE … LOCATION
    ...                'ofs://…' through HS2. Verify the dir exists.
    ${output} =    Ozone Path Exists    ${WAREHOUSE}/${TEST_DB}.db
    Should Be Equal    ${output}    OK

Table Dir Materialised On Ozone
    [documentation]    test.sh issued CREATE TABLE inside the DB.
    ...                Verify the table dir exists under the DB dir.
    ${output} =    Ozone Path Exists    ${WAREHOUSE}/${TEST_DB}.db/${TEST_TABLE}
    Should Be Equal    ${output}    OK
