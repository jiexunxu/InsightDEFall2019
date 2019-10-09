# Entry point of this project.
# Run the entire project by calling "python3 main.py" will start the web UI service

import sys

sys.path.insert(0, "./flask")
sys.path.insert(0, "./python")
import app
import ec2_manager
import init
import subprocess

[
    internal_params,
    bucket,
    connection,
    output_foldername,
    aws_key,
    aws_access,
    db_password,
    instanceIds,
    enable_ec2_control,
] = init.init()
# If enabled, reset all ec2 slave nodes to stop status
if enable_ec2_control:
    ec2_manager.reset(connection, instanceIds)
subprocess.call(["python3", "./flask/app.py"])
