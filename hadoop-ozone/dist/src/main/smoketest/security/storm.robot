# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

*** Settings ***
Documentation    Apache Storm cluster end-to-end via OAuth-only
...              auth. test.sh brings up Nimbus + Supervisor + a
...              ZooKeeper for coordination, asserts the agent
...              loaded on both daemon JVMs, and writes a probe
...              key on ofs:// from inside the Nimbus container.
...              This robot verifies the Ozone-side state.
Library          OperatingSystem
Resource         ../commonlib.robot
Test Timeout     2 minute

*** Variables ***
${NIMBUS_HOST}    nimbus
${NIMBUS_PORT}    6627
${UI_HOST}        storm-ui
${UI_PORT}        8080
${WORKSPACE}      ofs://om/volume1/bucket1/storm-test

*** Test Cases ***

Nimbus Thrift Port Is Up
    [documentation]    Nimbus's thrift port (6627) is what
    ...                topology submission goes through.
    ${output} =    Execute    nc -zv ${NIMBUS_HOST} ${NIMBUS_PORT} 2>&1
    Should Contain    ${output}    Connected to

Storm UI Is Up
    [documentation]    Storm's web UI is a proxy for "Nimbus +
    ...                ZooKeeper are talking to each other".
    ${output} =    Execute    nc -zv ${UI_HOST} ${UI_PORT} 2>&1
    Should Contain    ${output}    Connected to

Probe Key Written On Ozone
    [documentation]    test.sh wrote a probe key from the Nimbus
    ...                container via `ozone fs -put`. Verify
    ...                the key materialised on ofs://.
    ${output} =    Execute    OZONE_AGENT_LOG_LEVEL=OFF ozone fs -ls ${WORKSPACE}/probe.txt 2>&1 | grep -c "probe.txt" || true
    Should Not Be Equal As Integers    ${output}    0

Probe Key Owner Is Storm
    [documentation]    The write inside the Nimbus JVM went
    ...                through the OAuth-authenticated UGI, so
    ...                the file owner on Ozone must be "storm",
    ...                not any local OS user.
    ${output} =    Execute    OZONE_AGENT_LOG_LEVEL=OFF ozone fs -ls ${WORKSPACE}/probe.txt 2>&1 | grep "probe.txt" | head -1 | awk '{print $3}'
    Should Be Equal    ${output}    storm
