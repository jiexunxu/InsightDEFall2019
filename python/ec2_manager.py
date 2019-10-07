# start or stop ec2 slave nodes based on how many queries there are in the system, queried from the database

import boto3
import time

def manage(connection, instanceIds):
    def stop_instances():
        try:
            response=ec2.stop_instances(InstanceIds=instanceIds, DryRun=False)
            connection.cursor().execute("UPDATE usage_stats SET running_instances=0")
            connection.commit()
        except ClientError as e:
            print(e)
            
    def start_instances():
        try:
            response=ec2.start_instances(InstanceIds=instanceIds, DryRun=False)
            connection.cursor().execute("UPDATE usage_stats SET running_instances=3")
            connection.commit()
            waiter=ec2.get_waiter('instance_status_ok')
            waiter.wait(InstanceIds=instanceIds)
        except ClientError as e:
            print(e)
            
    ec2=boto3.client('ec2')
    cursor=connection.cursor()
    cursor.execute("SELECT * FROM usage_stats")
    usage_stats=cursor.fetchall()
    instances = boto3.client("ec2").describe_instance_status()
    if usage_stats[0][1]==0 and usage_stats[0][0]>0:
        print('===starting instances===')
        start_instances()        
    elif usage_stats[0][1]>0 and usage_stats[0][0]==0:
        print('===stopping instances===')
        stop_instances()

def increment_requests(connection, instanceIds):
    connection.cursor().execute("UPDATE usage_stats SET requests=requests+1")
    connection.commit()
    manage(connection, instanceIds) 

def decrement_requests(connection, instanceIds):
    connection.cursor().execute("UPDATE usage_stats SET requests=requests-1")
    connection.commit()
    manage(connection, instanceIds)

def reset(connection, instanceIds):
    connection.cursor().execute("UPDATE usage_stats SET requests=0, running_instances=3")
    connection.commit()
    manage(connection, instanceIds)
