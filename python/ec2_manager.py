# start or stop ec2 slave nodes based on how many queries there are in the system, queried from the database

import boto3

def manage(connection):
    def stop_instances():
        try:
            response=ec2.stop_instances(InstanceIds=instanceIds, DryRun=False)
            connection.cursor().execute("UPDATE usage_stats SET running_instances=3")
            connection.commit()
        except ClientError as e:
            print(e)
            
    def start_instances():
        try:
            response=ec2.start_instances(InstanceIds=instanceIds, DryRun=False)
            connection.cursor().execute("UPDATE usage_stats SET running_instances=0")
            connection.commit()
        except ClientError as e:
            print(e)
            
    instanceIds=['i-056ca5c6a60592957', 'i-08e547af1b32f71f0', 'i-0e4ff7936219479c5']    
    ec2=boto3.client('ec2')
    usage_stats=connection.cursor().execute("SELECT requests FROM usage_stats").fetchall()
    if usage_stats[1]==0 and usage_stats[0]>0:
        start_instances()
    elif usage_stats[1]>0 and usage_stats[0]==0:
        stop_instances()

def increment_requests(connection):
    connection.cursor().execute("UPDATE usage_stats SET requests=requests+1")
    connection.commit()
    manage(connection) 

def decrement_requests(connection):
    connection.cursor().execute("UPDATE usage_stats SET requests=requests-1")
    connection.commit()
    manage(connection)

def reset()
    connection.cursor().execute("UPDATE usage_stats SET requests=0, running_instances=1")
    connection.commit()
    manage()  
