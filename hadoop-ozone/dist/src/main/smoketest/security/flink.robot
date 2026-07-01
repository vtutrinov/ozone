# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

*** Settings ***
Documentation    Apache Flink session cluster end-to-end via
...              OAuth-only auth. test.sh stages input on ofs://,
...              runs batch WordCount via `flink run`, and lets
...              this robot assert the Ozone-side state.
Library          OperatingSystem
Resource         ../commonlib.robot
Test Timeout     2 minute

*** Variables ***
${JM_HOST}        jobmanager
${JM_REST}        8081
${WORKSPACE}      ofs://om/volume1/bucket1/flink-test

*** Test Cases ***

Flink JobManager REST Port Is Up
    [documentation]    JobManager REST + Web UI port reachable
    ...                inside the compose network.
    ${output} =    Execute    nc -zv ${JM_HOST} ${JM_REST} 2>&1
    Should Contain    ${output}    Connected to

WordCount Output Files On Ozone
    [documentation]    Flink WordCount writes one or more part
    ...                files under the output dir on Ozone. The
    ...                default DataSink names them "1", "2", ...
    ...                (one per parallel sink instance); at least
    ...                one must exist.
    ${output} =    Execute    OZONE_AGENT_LOG_LEVEL=OFF ozone fs -ls ${WORKSPACE}/wc-output/ 2>&1 | grep -v '^Found' | grep -c "wc-output/" || true
    Should Not Be Equal As Integers    ${output}    0

Data File Owner Is Flink
    [documentation]    Flink TaskManager runs as OAuth identity
    ...                "flink"; the Ozone file owner must match.
    ${output} =    Execute    OZONE_AGENT_LOG_LEVEL=OFF ozone fs -ls ${WORKSPACE}/wc-output/ 2>&1 | grep -v '^Found' | grep "wc-output/" | head -1 | awk '{print $3}'
    Should Be Equal    ${output}    flink

WordCount Counted The Words Correctly
    [documentation]    Cat the WC output and confirm "ozone 3"
    ...                (the staged input has 3 occurrences of
    ...                "ozone"). Sanity-check that the job
    ...                actually processed the data.
    ${output} =    Execute    OZONE_AGENT_LOG_LEVEL=OFF ozone fs -cat ${WORKSPACE}/wc-output/* 2>&1 | grep -E "^ozone " | awk '{print $2}'
    Should Be Equal As Integers    ${output}    3
