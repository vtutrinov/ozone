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
Documentation    Apache Spark Standalone end-to-end via OAuth-only
...              auth. test.sh runs SparkPi + a spark-sql DataFrame
...              round-trip from spark-master; this robot only
...              asserts the resulting state on Ozone.
Library          OperatingSystem
Resource         ../commonlib.robot
Test Timeout     2 minute

*** Variables ***
${MASTER_HOST}    spark-master
${MASTER_PORT}    7077
${UI_PORT}        8081
${WORKSPACE}      ofs://om/volume1/bucket1/spark-test

*** Test Cases ***

Spark Master Port Is Up
    [documentation]    Standalone master RPC port reachable.
    ${output} =    Execute    nc -zv ${MASTER_HOST} ${MASTER_PORT} 2>&1
    Should Contain    ${output}    Connected to

Spark Master UI Is Up
    [documentation]    Standalone master web UI reachable.
    ${output} =    Execute    nc -zv ${MASTER_HOST} ${UI_PORT} 2>&1
    Should Contain    ${output}    Connected to

DataFrame Parquet Files On Ozone
    [documentation]    test.sh ran INSERT through spark-sql; Parquet
    ...                writer should have produced at least one
    ...                part file under the table dir.
    ${output} =    Execute    OZONE_AGENT_LOG_LEVEL=OFF ozone fs -ls ${WORKSPACE}/spark_oauth/ 2>&1 | grep -v staging | grep -c "part-" || true
    Should Not Be Equal As Integers    ${output}    0

Data File Owner Is Spark
    [documentation]    Spark driver runs as the OAuth identity
    ...                "spark"; the Ozone file owner must match.
    ${output} =    Execute    OZONE_AGENT_LOG_LEVEL=OFF ozone fs -ls ${WORKSPACE}/spark_oauth/ 2>&1 | grep -v staging | grep "part-" | head -1 | awk '{print $3}'
    Should Be Equal    ${output}    spark
