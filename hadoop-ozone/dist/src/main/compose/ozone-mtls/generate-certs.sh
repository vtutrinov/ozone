#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Generates certificates for mTLS testing:
#   - Root CA (self-signed)
#   - Server certificate with SANs for all Ozone services
#   - Client certificates: allowed, revoked, unknown
#   - JKS keystores for Ozone services
#   - PKCS12 files for browser import

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CERTS_DIR="${SCRIPT_DIR}/certs"
PASSWORD="changeit"

# Clean previous certs
rm -rf "${CERTS_DIR}"
mkdir -p "${CERTS_DIR}"
cd "${CERTS_DIR}"

# ── CA infrastructure (openssl ca) ──────────────────────────────────────────

mkdir -p newcerts
touch index.txt
echo "01" > serial

cat > openssl-ca.cnf <<CAEOF
[ca]
default_ca = CA_default

[CA_default]
dir               = ${CERTS_DIR}
database          = \$dir/index.txt
new_certs_dir     = \$dir/newcerts
serial            = \$dir/serial
certificate       = \$dir/ca.crt
private_key       = \$dir/ca.key
default_md        = sha256
default_days      = 3650
policy            = policy_anything
copy_extensions   = copy

[policy_anything]
countryName            = optional
stateOrProvinceName    = optional
organizationName       = optional
organizationalUnitName = optional
commonName             = supplied
emailAddress           = optional

[req]
default_bits       = 4096
default_md         = sha256
distinguished_name = req_distinguished_name
prompt             = no

[req_distinguished_name]
CN = Ozone-Test-CA

[v3_ca]
basicConstraints       = critical, CA:TRUE
subjectKeyIdentifier   = hash
authorityKeyIdentifier = keyid:always, issuer
keyUsage               = critical, keyCertSign, cRLSign
CAEOF

# ── Root CA ─────────────────────────────────────────────────────────────────

openssl req -new -x509 -nodes \
  -keyout ca.key -out ca.crt \
  -days 3650 \
  -config openssl-ca.cnf \
  -extensions v3_ca

echo "==> Root CA created (CN=Ozone-Test-CA)"

# ── Server certificate ──────────────────────────────────────────────────────

cat > server-ext.cnf <<'SRVEOF'
[req]
default_bits       = 2048
default_md         = sha256
distinguished_name = req_dn
req_extensions     = v3_req
prompt             = no

[req_dn]
CN = ozone-server

[v3_req]
basicConstraints = CA:FALSE
keyUsage         = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName   = @alt_names

[alt_names]
DNS.1  = om1
DNS.2  = om2
DNS.3  = om3
DNS.4  = scm1.org
DNS.5  = scm2.org
DNS.6  = scm3.org
DNS.7  = datanode
DNS.8  = s3g
DNS.9  = recon
DNS.10 = localhost
IP.1   = 127.0.0.1
SRVEOF

openssl req -new -nodes \
  -keyout server.key -out server.csr \
  -config server-ext.cnf

SERIAL=$(cat "${CERTS_DIR}/serial")
openssl ca -batch -notext \
  -config "${CERTS_DIR}/openssl-ca.cnf" \
  -in "${CERTS_DIR}/server.csr" \
  -extensions v3_req \
  -extfile "${CERTS_DIR}/server-ext.cnf" \
  -days 3650
cp "${CERTS_DIR}/newcerts/${SERIAL}.pem" "${CERTS_DIR}/server.crt"

echo "==> Server certificate created (CN=ozone-server)"

# ── Helper: create client certificate ───────────────────────────────────────

create_client_cert() {
  local cert_name="$1"
  local cn="$2"

  cat > "${cert_name}-ext.cnf" <<EOF
[req]
default_bits       = 2048
default_md         = sha256
distinguished_name = req_dn
req_extensions     = v3_req
prompt             = no

[req_dn]
CN = ${cn}

[v3_req]
basicConstraints       = CA:FALSE
keyUsage               = digitalSignature
extendedKeyUsage       = clientAuth
authorityInfoAccess    = OCSP;URI:http://ocsp:2560
EOF

  openssl req -new -nodes \
    -keyout "${cert_name}.key" -out "${cert_name}.csr" \
    -config "${cert_name}-ext.cnf"

  local serial
  serial=$(cat "${CERTS_DIR}/serial")
  openssl ca -batch -notext \
    -config "${CERTS_DIR}/openssl-ca.cnf" \
    -in "${CERTS_DIR}/${cert_name}.csr" \
    -extensions v3_req \
    -extfile "${CERTS_DIR}/${cert_name}-ext.cnf" \
    -days 3650
  cp "${CERTS_DIR}/newcerts/${serial}.pem" "${CERTS_DIR}/${cert_name}.crt"

  # PKCS12 for browser import
  openssl pkcs12 -export \
    -in "${cert_name}.crt" -inkey "${cert_name}.key" -certfile ca.crt \
    -out "${cert_name}.p12" -passout "pass:${PASSWORD}" \
    -name "${cn}"

  echo "==> Client certificate created (CN=${cn})"
}

# ── Client certificates ────────────────────────────────────────────────────

create_client_cert "client-allowed" "ozone-client-allowed"
create_client_cert "client-revoked" "ozone-client-revoked"
create_client_cert "client-unknown" "ozone-client-unknown"

# ── Revoke the "revoked" client certificate ─────────────────────────────────

openssl ca -batch \
  -config "${CERTS_DIR}/openssl-ca.cnf" \
  -revoke "${CERTS_DIR}/client-revoked.crt"

echo "==> Client certificate CN=ozone-client-revoked has been revoked"

# ── JKS keystores ──────────────────────────────────────────────────────────

# server.p12 (intermediate for keytool import)
openssl pkcs12 -export \
  -in server.crt -inkey server.key -certfile ca.crt \
  -out server.p12 -passout "pass:${PASSWORD}" \
  -name "ozone-server"

keytool -importkeystore \
  -srckeystore server.p12 -srcstoretype PKCS12 -srcstorepass "${PASSWORD}" \
  -destkeystore server.jks -deststoretype JKS -deststorepass "${PASSWORD}" \
  -noprompt

# truststore.jks (CA cert)
keytool -importcert \
  -file ca.crt -alias "ozone-test-ca" \
  -keystore truststore.jks -storetype JKS -storepass "${PASSWORD}" \
  -noprompt

echo "==> JKS keystores created (server.jks, truststore.jks)"

# ── Cleanup intermediate files ──────────────────────────────────────────────

rm -f *.csr *.cnf server.p12

echo ""
echo "============================================"
echo " Certificate generation complete!"
echo " Output directory: ${CERTS_DIR}"
echo ""
echo " Browser usage:"
echo "   1. Import ca.crt as trusted CA"
echo "   2. Import client-*.p12 (password: ${PASSWORD})"
echo ""
echo " curl testing:"
echo "   curl -k --cert certs/client-allowed.crt \\"
echo "        --key certs/client-allowed.key \\"
echo "        https://localhost:9875/conf"
echo "============================================"
