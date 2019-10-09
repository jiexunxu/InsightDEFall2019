# start or stop ec2 slave nodes based on how many queries there are in the system, queried from the database

import boto3
import time

# Helper unction called by increment_requests(), decrement_requests() and reset()
def manage(connection, instanceIds):
    # Uses boto3 to start or stop EC2 instances on AWS
    def stop_instances():
        try:
            response = ec2.stop_instances(InstanceIds=instanceIds, DryRun=False)
            connection.cursor().execute("UPDATE usage_stats SET running_instances=0")
            connection.commit()
        except ClientError as e:
            print(e)

    def start_instances():
        try:
            response = ec2.start_instances(InstanceIds=instanceIds, DryRun=False)
            connection.cursor().execute("UPDATE usage_stats SET running_instances=3")
            connection.commit()
            waiter = ec2.get_waiter("instance_status_ok")
            waiter.wait(InstanceIds=instanceIds)
        except ClientError as e:
            print(e)

    ec2 = boto3.client("ec2")
    cursor = connection.cursor()
    # Read how many requests are still present from the database
    cursor.execute("SELECT * FROM usage_stats")
    usage_stats = cursor.fetchall()
    instances = boto3.client("ec2").describe_instance_status()
    if usage_stats[0][1] == 0 and usage_stats[0][0] > 0:
        print("===starting instances===")
        start_instances()
    elif usage_stats[0][1] > 0 and usage_stats[0][0] == 0:
        print("===stopping instances===")
        stop_instances()


# Called every time a user clicks the Submit button, if enable_ec2_control is True
def increment_requests(connection, instanceIds):
    connection.cursor().execute("UPDATE usage_stats SET requests=requests+1")
    connection.commit()
    manage(connection, instanceIds)


# Called every time a spark-submit job finishes, if enable_ec2_control is True
def decrement_requests(connection, instanceIds):
    connection.cursor().execute("UPDATE usage_stats SET requests=requests-1")
    connection.commit()
    manage(connection, instanceIds)


# Called when the project is started initially, if enable_ec2_control is True
def reset(connection, instanceIds):
    connection.cursor().execute(
        "UPDATE usage_stats SET requests=0, running_instances=3"
    )
    connection.commit()
    manage(connection, instanceIds)
