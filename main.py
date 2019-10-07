# Entry point of this project

import sys
sys.path.insert(0, './flask')
sys.path.insert(0, './python')
import app
import ec2_manager
import init
import subprocess


[internal_params, bucket, connection, output_foldername, aws_key, aws_access, db_password, instanceIds, enable_ec2_control]=init.init()
if enable_ec2_control:
    ec2_manager.reset(connection, instanceIds)
subprocess.call(["python3","./flask/app.py"])
