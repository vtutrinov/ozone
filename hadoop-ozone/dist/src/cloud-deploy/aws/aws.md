# Ozone Deployment Automation To AWS


0. Assign an AdministratorAccess policy to you group or user in AWS Console
1. Configure correctly your profile to deal with AWS API

```bash
#~/.aws/config
[profile vtutrinov]
source_profile = default
aws_access_key_id = ***
aws_secret_access_key = ***
region = us-east-1
```

2. Create ec2 key-pair

```bash
aws --profile vtutrinov ec2 create-key-pair --key-name 'aws-ec2-admin-keypair' --output text > aws-ec2-admin-key.pem
chmod 400 aws-ec2-admin-key.pem
```
3. Create launch templates:

```bash
aws --profile vtutrinov ec2 create-launch-template --launch-template-name ozone-services --launch-template-data file://./ozone-services-launch-template.json
```

4. Create required ec2 instances (and network)

15 instances:

5 datanodes, 3 OMs, 3 SCMs - large machines (11 instances)
S3Gateway, Recon, Grafana, Prometheus - small machines (4 instances)

```bash
# create ec2 instances for datanodes
aws ec2 run-instances \
    --launch-template LaunchTemplateId=lt-07377af3122c729dc \
    --instance-type c5.4xlarge
    --tag-specifications "ResourceType=instance,Tags=[{Key=Type,Value=Core},{Key=Service,Value=Datanode}]"
    --block-device-mapping file://./core-services-device-mapping.json
    --count 5:5
    
# create ec2 instances for OMs
aws ec2 run-instances \
    --launch-template LaunchTemplateId=lt-07377af3122c729dc \
    --instance-type c5.4xlarge
    --tag-specifications "ResourceType=instance,Tags=[{Key=Type,Value=Core},{Key=Service,Value=OM}]"
    --block-device-mapping file://./core-services-device-mapping.json
    --count 3:3
    
# create ec2 instances for SCMs
aws ec2 run-instances \
    --launch-template LaunchTemplateId=lt-07377af3122c729dc \
    --instance-type c5.4xlarge
    --tag-specifications "ResourceType=instance,Tags=[{Key=Type,Value=Core},{Key=Service,Value=SCM}]"
    --block-device-mapping file://./core-services-device-mapping.json
    --count 3:3

```
