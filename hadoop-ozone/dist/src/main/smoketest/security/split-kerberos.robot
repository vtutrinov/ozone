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
Documentation       Verify the split-Kerberos deployment mode end-to-end:
...                 - external clients (ofs, `ozone s3 getsecret`) must
...                   present a Kerberos TGT against OM's external port;
...                 - the AWS S3 Sig V4 flow (key issued via Kerberos, then
...                   used over plain HTTPS to S3G, forwarded to OM via the
...                   SIMPLE-auth service-RPC port) must validate by HMAC on
...                   the OM side with no Kerberos involved after the secret
...                   was obtained;
...                 - inter-service traffic (S3G -> OM, DN -> SCM) runs over
...                   SIMPLE auth.
...                 Run from the `httpfs` container (or any non-daemon
...                 container) so the test exercises the same Kerberos path a
...                 real external user would take, against a cluster started
...                 by compose/ozonesecure-split.
Library             OperatingSystem
Library             String
Library             BuiltIn
Resource            ../commonlib.robot
Resource            ../s3/commonawslib.robot
Test Timeout        5 minutes

*** Variables ***
${SPLIT_KRB_USER}                 testuser
${SPLIT_KRB_KEYTAB}          testuser.keytab
${SPLIT_BUCKET_PREFIX}      splitkrb
${SPLIT_VOLUME_PREFIX}      splitofs
${LOCAL_PAYLOAD}            /tmp/split-krb-payload.txt
${DOWNLOADED_PAYLOAD}       /tmp/split-krb-payload.downloaded
${OFS_LOCAL_PAYLOAD}        /tmp/split-krb-ofs-payload.txt
${OFS_DOWNLOADED_PAYLOAD}   /tmp/split-krb-ofs-payload.downloaded
${PAYLOAD_CONTENT}          split-kerberos hello — sigv4 + HMAC on OM
${OFS_PAYLOAD_CONTENT}      split-kerberos hello - ofs over Kerberos on OM external port

*** Keywords ***
Clean ticket cache
    Execute and Ignore Error    kdestroy

Authenticated ofs ls succeeds
    Run Keyword     Kinit test user     ${SPLIT_KRB_USER}     ${SPLIT_KRB_KEYTAB}
    ${out}=     Execute     ozone fs -ls ofs://om/
    Should not contain      ${out}      AccessControlException

Get S3 secret with TGT
    Run Keyword     Kinit test user     ${SPLIT_KRB_USER}     ${SPLIT_KRB_KEYTAB}
    Execute and Ignore Error    ozone s3 revokesecret -y -u ${SPLIT_KRB_USER}
    ${out}=     Execute     ozone s3 getsecret -u ${SPLIT_KRB_USER}
    Should contain      ${out}      awsAccessKey
    Should contain      ${out}      awsSecret

ofs ls without TGT is rejected
    Clean ticket cache
    ${rc}   ${out}=     Run And Return Rc And Output    ozone fs -ls ofs://om/
    Should Not Be Equal As Integers     ${rc}   0
    Should Match Regexp    ${out}      AccessControlException|Client cannot authenticate

getsecret without TGT is rejected
    Clean ticket cache
    ${rc}   ${out}=     Run And Return Rc And Output    ozone s3 getsecret -u ${SPLIT_KRB_USER}
    Should Not Be Equal As Integers     ${rc}   0
    Should Match Regexp    ${out}      AccessControlException|Client cannot authenticate

Provision AWS credentials with TGT then drop TGT
    Run Keyword     Kinit test user     ${SPLIT_KRB_USER}     ${SPLIT_KRB_KEYTAB}
    # Make sure we start from a clean secret state.
    Execute and Ignore Error    ozone s3 revokesecret -y
    ${result}=      Execute     ozone s3 getsecret
    ${accessKey}=   Get Regexp Matches      ${result}     (?<=awsAccessKey=).*
    ${secret}=      Get Regexp Matches      ${result}     (?<=awsSecret=).*
    Set Suite Variable      ${ACCESS_KEY}       ${accessKey[0].strip()}
    Set Suite Variable      ${SECRET_KEY}       ${secret[0].strip()}
    Execute         aws configure set default.s3.signature_version s3v4
    Execute         aws configure set aws_access_key_id ${ACCESS_KEY}
    Execute         aws configure set aws_secret_access_key ${SECRET_KEY}
    Execute         aws configure set region us-west-1
    Execute         aws configure set default.s3.addressing_style path
    # Drop the TGT to prove subsequent S3 ops don't need it.
    Clean ticket cache

Exercise AWS S3 flow under sigv4 with no TGT
    [Documentation]    The whole point of split-Kerberos: a human authenticates
    ...                ONCE via Kerberos to fetch an AWS key, then drops the
    ...                TGT and uses Sig V4 alone. S3G receives the signed HTTP
    ...                request, forwards to OM over the SIMPLE service-RPC
    ...                port, and OM validates the signature by HMAC against the
    ...                stored S3 secret — no Kerberos on the wire after the
    ...                initial getsecret.
    Install aws cli
    Provision AWS credentials with TGT then drop TGT
    # Sanity: confirm no TGT.
    ${klist_rc}     ${klist_out}=   Run And Return Rc And Output     klist -s
    Should Not Be Equal As Integers     ${klist_rc}     0
    # Use a unique bucket name so re-runs don't collide.
    ${suffix}=          Generate Random String     8       [LOWER]
    ${bucket}=          Set Variable               ${SPLIT_BUCKET_PREFIX}-${suffix}
    Set Test Variable   ${bucket}
    # ---- s3api create-bucket ----
    Execute             aws s3api --endpoint-url http://s3g:9878 create-bucket --bucket ${bucket}
    # ---- s3 cp (PutObject via sigv4) ----
    Create File         ${LOCAL_PAYLOAD}    ${PAYLOAD_CONTENT}
    Execute             aws s3 --endpoint-url http://s3g:9878 cp ${LOCAL_PAYLOAD} s3://${bucket}/payload.txt
    # ---- s3 ls (ListObjects) ----
    ${list}=            Execute     aws s3 --endpoint-url http://s3g:9878 ls s3://${bucket}/
    Should Contain      ${list}     payload.txt
    # ---- s3 cp (GetObject + byte-for-byte verify) ----
    Remove File         ${DOWNLOADED_PAYLOAD}
    Execute             aws s3 --endpoint-url http://s3g:9878 cp s3://${bucket}/payload.txt ${DOWNLOADED_PAYLOAD}
    ${downloaded}=      Get File                   ${DOWNLOADED_PAYLOAD}
    Should Be Equal     ${downloaded.strip()}      ${PAYLOAD_CONTENT}
    # ---- cleanup ----
    Execute             aws s3 --endpoint-url http://s3g:9878 rm s3://${bucket}/payload.txt
    Execute             aws s3api --endpoint-url http://s3g:9878 delete-bucket --bucket ${bucket}
    [Teardown]          Run Keywords
    ...                 Remove File    ${LOCAL_PAYLOAD}    AND
    ...                 Remove File    ${DOWNLOADED_PAYLOAD}

AWS S3 sigv4 with a forged secret is rejected
    [Documentation]    Sanity: if the access key is valid but the secret is
    ...                wrong, OM's HMAC validation must reject the signature.
    Install aws cli
    Provision AWS credentials with TGT then drop TGT
    # Forge the secret. AWS CLI will sign with the wrong HMAC; OM must reject.
    Execute             aws configure set aws_secret_access_key forged-secret
    ${rc}   ${out}=     Run And Return Rc And Output    aws s3api --endpoint-url http://s3g:9878 list-buckets
    Should Not Be Equal As Integers     ${rc}   0
    Should Match Regexp     ${out}      Forbidden|SignatureDoesNotMatch|AccessDenied|Access Denied|InvalidAccessKeyId

Exercise ofs write and read flow under Kerberos
    [Documentation]    Symmetric to the AWS S3 case but for the ofs path: an
    ...                external client with a TGT must be able to create a
    ...                volume + bucket, put a key, list, read, and download it
    ...                byte-for-byte intact. This exercises OM key allocation
    ...                (allocateBlock → SCM over the SIMPLE service-RPC port)
    ...                and the Datanode data plane in the split-Kerberos mode.
    Run Keyword         Kinit test user     ${SPLIT_KRB_USER}     ${SPLIT_KRB_KEYTAB}
    # Sanity: confirm a TGT is present before any ofs op.
    ${klist_rc}     ${klist_out}=   Run And Return Rc And Output     klist -s
    Should Be Equal As Integers     ${klist_rc}     0
    # Use unique names so re-runs don't collide.
    ${suffix}=          Generate Random String     8       [LOWER]
    ${volume}=          Set Variable               ${SPLIT_VOLUME_PREFIX}-${suffix}
    ${bucket}=          Set Variable               buk
    Set Test Variable   ${volume}
    Set Test Variable   ${bucket}
    # ---- volume + bucket (OM control plane) ----
    Execute             ozone sh volume create /${volume}
    Execute             ozone sh bucket create /${volume}/${bucket}
    # ---- ofs -put (PutKey via OM, blocks allocated from SCM, data to DN) ----
    Create File         ${OFS_LOCAL_PAYLOAD}    ${OFS_PAYLOAD_CONTENT}
    Execute             ozone fs -put ${OFS_LOCAL_PAYLOAD} ofs://om/${volume}/${bucket}/payload.txt
    # ---- ofs -ls (verify present) ----
    ${list}=            Execute     ozone fs -ls ofs://om/${volume}/${bucket}/
    Should Contain      ${list}     payload.txt
    # ---- ofs -cat (read content directly) ----
    ${content}=         Execute     ozone fs -cat ofs://om/${volume}/${bucket}/payload.txt
    Should Be Equal     ${content.strip()}     ${OFS_PAYLOAD_CONTENT}
    # ---- ofs -get (full GetKey + byte-for-byte verify) ----
    Remove File         ${OFS_DOWNLOADED_PAYLOAD}
    Execute             ozone fs -get ofs://om/${volume}/${bucket}/payload.txt ${OFS_DOWNLOADED_PAYLOAD}
    ${downloaded}=      Get File                   ${OFS_DOWNLOADED_PAYLOAD}
    Should Be Equal     ${downloaded.strip()}      ${OFS_PAYLOAD_CONTENT}
    # ---- cleanup ----
    Execute             ozone fs -rm -skipTrash ofs://om/${volume}/${bucket}/payload.txt
    Execute             ozone sh bucket delete /${volume}/${bucket}
    Execute             ozone sh volume delete /${volume}
    [Teardown]          Run Keywords
    ...                 Remove File    ${OFS_LOCAL_PAYLOAD}    AND
    ...                 Remove File    ${OFS_DOWNLOADED_PAYLOAD}

ofs write without TGT is rejected
    [Documentation]    The negative twin of the write+read case: without a
    ...                Kerberos ticket the same put must be rejected by OM
    ...                with AccessControlException.
    Clean ticket cache
    Create File         ${OFS_LOCAL_PAYLOAD}    ${OFS_PAYLOAD_CONTENT}
    ${rc}   ${out}=     Run And Return Rc And Output
    ...                 ozone fs -put ${OFS_LOCAL_PAYLOAD} ofs://om/anyvol/anybuk/anykey
    Should Not Be Equal As Integers     ${rc}   0
    Should Match Regexp     ${out}      AccessControlException|Client cannot authenticate
    [Teardown]          Remove File    ${OFS_LOCAL_PAYLOAD}

*** Test Cases ***
External ofs succeeds with TGT, fails without
    Authenticated ofs ls succeeds
    ofs ls without TGT is rejected

External ofs write + read round-trip with TGT
    Exercise ofs write and read flow under Kerberos

External ofs write without TGT is rejected
    ofs write without TGT is rejected

S3 secret round-trip
    Get S3 secret with TGT
    getsecret without TGT is rejected

External AWS S3 sigv4 succeeds without TGT
    Exercise AWS S3 flow under sigv4 with no TGT

OM rejects forged S3 signatures
    AWS S3 sigv4 with a forged secret is rejected
