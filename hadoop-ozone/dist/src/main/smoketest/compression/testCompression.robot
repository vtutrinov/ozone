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
Library             OperatingSystem
Library             String
Library             Collections
Library             Process
Resource            ../commonlib.robot
Resource            ../ozone-lib/shell.robot
Resource            ../s3/commonawslib.robot

*** Variables ***
${ENDPOINT_URL}       http://s3g:9878
${TEST_VOLUME}            volume1
${TEST_BUCKET}            bucket
${S3_BUCKET}             s3bucket
${COUNT}                  1
@{COMPRESSION_TYPES}      gzip  snappy    bzip2

*** Test Cases ***
Test Compress Ratis
    ${ext}    Set Variable    .txt
    FOR  ${compression}  IN   @{COMPRESSION_TYPES}
        Setup Ozone Test Environment      2    ${compression}    RATIS    THREE    ${ext}
        Check Key Compressed              2    ${compression}    ${ext}
        Cleanup Ozone Test Environment    2    ${ext}
    END

Test Compress EC
    ${ext}    Set Variable    .txt
    FOR  ${compression}  IN   @{COMPRESSION_TYPES}
        Setup Ozone Test Environment      2    ${compression}    EC    rs-3-2-1024k    ${ext}
        Check Key Compressed              2    ${compression}    ${ext}
        Cleanup Ozone Test Environment    2    ${ext}
    END

Test Unknown Extension is not compressed
    ${ext}    Set Variable    .app
    FOR  ${compression}  IN   @{COMPRESSION_TYPES}
        Setup Ozone Test Environment      2    ${compression}    RATIS    THREE    ${ext}
        Check Key Not Compressed          2    ${compression}    ${ext}
        Cleanup Ozone Test Environment    2    ${ext}
    END

Put object to s3 compressed bucket
    ${ext}              Set Variable    .txt
                        Setup Ozone Test Environment           2    gzip    RATIS    THREE    ${ext}
                        Setup s3 env
                        Create S3 Bucket                       gzip    RATIS    THREE

                        Execute                                echo "Randomtext123" > /tmp/testfile
    ${result} =         Execute AWSS3ApiCli and ignore error   put-object --bucket ${S3_BUCKET} --key ${PREFIX}/putobject/key=value/f1 --body /tmp/testfile
                        Should contain                         ${result}         Compressed bucket are not supported in S3
                        Execute And Ignore Error               ozone sh bucket delete /s3v/${S3_BUCKET}
                        Cleanup Ozone Test Environment         2    ${ext}

Put object to s3 linked bucket
    ${ext}              Set Variable    .txt
                        Setup Ozone Test Environment      2    gzip    RATIS    THREE    ${ext}
                        Setup s3 env
                        Link S3 Bucket

                        Execute                               echo "Randomtext123" > /tmp/testfile
    ${result} =         Execute AWSS3ApiCli and ignore error  put-object --bucket ${S3_BUCKET} --key ${PREFIX}/putobject/key=value/f1 --body /tmp/testfile
                        Should contain                        ${result}         Compressed bucket are not supported in S3
    ${result} =         Execute AWSS3ApiCli and ignore error  get-object --bucket ${S3_BUCKET} --key key1.txt /tmp/testfile.result
                        Should contain                        ${result}         Compressed bucket are not supported in S3
                        Execute And Ignore Error              ozone sh bucket delete /s3v/${S3_BUCKET}
                        Cleanup Ozone Test Environment        2    ${ext}

*** Keywords ***
Setup Ozone Test Environment
    [Arguments]    ${num_keys}    ${compresionType}  ${TYPE}  ${REPLICATION}    ${extension}
    Log    Setting up Ozone test environment... ${compresionType}
    Execute And Ignore Error    ozone sh volume create ${TEST_VOLUME}
    Execute And Ignore Error    ozone sh bucket create --type=${TYPE} --replication=${REPLICATION} --compression=${compresionType} ${TEST_VOLUME}/${TEST_BUCKET}
    ${keys}=    Evaluate    ${num_keys} + 1
    FOR    ${i}    IN RANGE    1    ${keys}
        ${key}    Set Variable    key${i}${extension}
        ${file}   Set Variable    random_file_${i}
        Execute    dd if=/dev/urandom of=${file} bs=1M count=${COUNT}
        Execute    ozone sh key put ${TEST_VOLUME}/${TEST_BUCKET}/${key} ${file}
    END

Setup v2 headers
    Set Environment Variable   AWS_ACCESS_KEY_ID       ANYID
    Set Environment Variable   AWS_SECRET_ACCESS_KEY   ANYKEY

Create S3 Bucket
    [Arguments]     ${compresionType}  ${TYPE}  ${REPLICATION}
    Execute And Ignore Error    ozone sh bucket create --type=${TYPE} --replication=${REPLICATION} --compression=${compresionType} /s3v/${S3_BUCKET}

Link S3 Bucket
    Execute            ozone sh bucket link /${TEST_VOLUME}/${TEST_BUCKET} /s3v/${S3_BUCKET}

Setup s3 env
    Generate random prefix
    Install aws cli
    Setup v2 headers

Cleanup Ozone Test Environment
    [Arguments]    ${num_keys}    ${extension}
    Log    Cleaning up Ozone test environment...
    ${keys}=    Evaluate    ${num_keys} + 1
    FOR    ${i}    IN RANGE    1    ${keys}
        Execute And Ignore Error    ozone fs -rm -skipTrash /${TEST_VOLUME}/${TEST_BUCKET}/key${i}${extension}
    END
    Execute And Ignore Error    ozone sh bucket delete /${TEST_VOLUME}/${TEST_BUCKET}
    Execute And Ignore Error    ozone sh volume delete /${TEST_VOLUME}


Check Key Compressed
    [Arguments]    ${num_keys}    ${compresionType}    ${extension}
    Log    Checking compresison type for key
    ${keys}=    Evaluate    ${num_keys} + 1
    FOR    ${i}    IN RANGE    1    ${keys}
        ${key}    Set Variable    key${i}
        ${result}=  Execute    ozone sh key info ${TEST_VOLUME}/${TEST_BUCKET}/${key}${extension}
        Should Contain    ${result}    ${compresionType}
    END

Check Key Not Compressed
    [Arguments]    ${num_keys}    ${compresionType}  ${extension}
    Log    Checking compresison type for key
    ${keys}=    Evaluate    ${num_keys} + 1
    FOR    ${i}    IN RANGE    1    ${keys}
        ${key}    Set Variable    key${i}
        ${result}=  Execute    ozone sh key info ${TEST_VOLUME}/${TEST_BUCKET}/${key}${extension}
        Should Not Contain    ${result}    ${compresionType}
    END
