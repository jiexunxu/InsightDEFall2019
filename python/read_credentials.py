# Read AWS credentials from local credentials.yaml file

import csv
import yaml


def read():
    with open("credentials.yaml") as f:
        data = yaml.safe_load(f)
        key_ID = data[0]
        secret_access_key = data[1]
        db_host = data[2]
        db_password = data[3]
        instanceIds = [data[4], data[5], data[6]]
        if data[7] == "1":
            enable_ec2_control = True
        else:
            enable_ec2_control = False
    return [
        key_ID,
        secret_access_key,
        db_host,
        db_password,
        instanceIds,
        enable_ec2_control,
    ]
