
## Prerequisites

- You have a Yandex account and cloud resources are available.
- Install Yandex CLI. (https://cloud.yandex.com/en/docs/cli/operations/install-cli)
- Create Service Account (with 'admin' role) and get the JSON key file.
- Install Terraform.
- Install Ansible.
- Install `jq` and `yq` tools, and jmespath query tool (pip install jmespath)

```bash

## Steps

* create yc configuration file
```bash
yc init
```

* check available service accounts
```bash
yc iam service-account list
```
output:
```bash
+----------------------+-------+
|          ID          | NAME  |
+----------------------+-------+
| ajeuuj7kmuq0nolsdglc | admin |
+----------------------+-------+
```

* create key:
```bash
yc iam key create --service-account-name admin --output key.json
```
```bash
cat key.json
# output
{
   "id": "ajek4fcf410a9j1h415k",
   "service_account_id": "ajeuuj7kmuq0nolsdglc",
   "created_at": "2024-03-26T14:44:20.241796096Z",
   "key_algorithm": "RSA_2048",
   "public_key": "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAmdhMZoR6PFEdEUVh6jBN\n7X9s+GLNnxNBkyXskuv2INOdFFnRoZO+NHTiENlz1S0wvJdsgY+Ym+YyhpgK1QoN\nULLrUwS/iePAxaarRa2tdmH4906WHs9AccvQysOZIZwWDZYZk6YMIy38noshLzNl\nXVpaK6Y9JvIyLimTtn44EccVCLxPpZaRy67p5HAii4gvLUWgqkpcf488UU+BIb5P\nq6FYV8C89NNus6ADSWgzC4vgAe8deAN5I97sWEnViBnRCMG87W64O3U/V47o/dAK\nMHiX2nGT2OzGs4bb2MDjZFjtA9v5u9muX7fobZZH1duOpI3nW5meEMVyedoBNhTD\ngQIDAQAB\n-----END PUBLIC KEY-----\n",
   "private_key": "PLEASE DO NOT REMOVE THIS LINE! Yandex.Cloud SA Key ID \u003cajek4fcf410a9j1h415k\u003e\n-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCZ2ExmhHo8UR0R\nRWHqME3tf2z4Ys2fE0GTJeyS6/Yg050UWdGhk740dOIQ2XPVLTC8l2yBj5ib5jKG\nmArVCg1QsutTBL+J48DFpqtFra12Yfj3TpYez0Bxy9DKw5khnBYNlhmTpgwjLfye\niyEvM2VdWlorpj0m8jIuKZO2fjgRxxUIvE+llpHLrunkcCKLiC8tRaCqSlx/jzxR\nT4Ehvk+roVhXwLz0026zoANJaDMLi+AB7x14A3kj3uxYSdWIGdEIwbztbrg7dT9X\njuj90AoweJfacZPY7MazhtvYwONkWO0D2/m72a5ft+htlkfV246kjedbmZ4QxXJ5\n2gE2FMOBAgMBAAECggEAAau5crCUIkUQP4CFJnW1VvO4E3+BM63SG77A8byJzQP3\nVAIAtpRD785oKR4vdZx6x8WOOl78nY7TtCYBEhy8lciHqTxnNyBn1s2vVP+kn2FD\ngq4SD84Y6VN5FkanClgnXijn6LRYM+abNH6W0uwoOOoTCh/RULO3K0ysy8HVqo2+\nYnCjzEnfGX8OYpakgh0vrYK+TAmxeVPncfsN8/LAHuqzJLt5R2fadAkpN7pnTx4l\nXwxMB3RWlkEs3U/WJnck24Ny6A9TfbXb1/o4+gRo13DqV2SaMR3K4YgNrbFyW8/W\nzoQQrZLGcILZKbV/7nDAO0TcrnZhoXGP8qLFvHjrEQKBgQC+EHD1ZLLtsMFPkGt6\n6gPXLXQ2TW5RM5NQRqlXwKUAlSiexDpFGbkssXmR4ayMdB0m0PB6n3XIv9fCYzgL\nYEG5xwzuj/tc8akFhUQyGLOpzI59XU+1W8MqVd2YjPYnDjti2bHMpB5gOTAEL0i6\n8Czsg6o1Qt2wSyZEYzALLld3aQKBgQDPNzqhw9cut979GO5o9HFEDmnOypCB+4zd\nEQiOoJk9OT60kLITMJFzmAEgEXT1+4ddfnHLfSdBk65H57BruMco6gPIsm42f4iW\nUTvk7/EtGaOBVducluKTWOtHKe/xkMLyyvyEccH0ZekIGby63hA5Rogs/Uk/kkxn\nS7oaRQJAWQKBgBNpzn/iJzyL/1LHJ5NIb9f8tpANXKVCpVtfvFBQRwpGMf31gRYp\nyHY+MnKqIFvASHH8iXOc8gTtQ3aBd+oBjPUS1clQP5aAwIjl0c9kIoXHdQ5tB4U4\nuDiMyLOaQlI+6+Xu4nBKmyes77Cdu8oMWipWUH5cNBSbuG7nyrJ5q0lxAoGBAMWs\nYxNkVsdrgp6hhPW8krygn1E1LgvBo4xULnyZOfYMwQPRsP7bazYBgLlNzezGqUiI\nUhgT6ToGntBbdpIcsGkYbMmtZbQ5j1wOXHu2ZbVnavH7rMfBrJ1xyuiVWh9wwHiL\nKb9TIfp6qYYfv0nog0OQRGHeslyhvJ8hv4RF5rLpAoGAPIKV/uF6B+eQ93ARlJpV\nlsNl6v9Zj/v8UkF7z4JGFNu8waVMmQqxX1TtDPCTQQg6N6r+vXh/G2eEUwEAQv9x\nNr5eUv4OdkqGlOMj8KLQ4wtc5BGme4KKIKq6+gmSc76NcdfEZx+O+Pyengnr7frp\n73opyzmoQitB7pL1YAjHC+Y=\n-----END PRIVATE KEY-----\n"
}
```

* set created key as default
```bash
yc config set service-account-key key.json
```

* deploy VPN server
```bash
yc compute instance create \
  --name vpn-server \
  --zone ru-central1-a \
  --hostname openvpn \
  --memory 2 \
  --cores 2 \
  --labels service=vpn,role=server \
  --network-interface subnet-name=default-ru-central1-d,nat-ip-version=ipv4 \
  --create-boot-disk image-folder-id=standard-images,image-family=openvpn,size=10 \
  --ssh-key ~/.ssh/id_rsa.pub
  --format json
```
and configure it according to [docummentation](https://yandex.cloud/en-ru/docs/tutorials/routing/openvpn)

* to enable connection to the Internet inside VMs, you need to create NAT Gateway:

```bash
yc vpc gateway create --name nat-gateway
```
then you need to create a route-table:
```bash
yc vpc route-table create --name=nat-route-table --network-name=default --route destination=0.0.0.0/0,gateway-id=enpkq1i59uorcd2uk4rp****
# gateway-id is the id of the created NAT Gateway above
```

and finally you need to attach the route-table to the subnet:
```bash
yc vpc subnet update --name=default-ru-central1-d --route-table-id=rtb-0e1q1i59uorcd2uk4rp****
# rtb-0e1q1i59uorcd2uk4rp**** is the id of the created route-table above
```

# Deploy cluster

* cd ./ansible
* source ./venv/bin/activate

```bash
cd ./ansible
source ./venv/bin/activate
ansible-playbook ./make_ozone_cluster.yaml
```