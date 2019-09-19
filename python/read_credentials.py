# Read AWS credentials from local credentials.txt file
# Returns a list where 1st element is AWS key ID, 2nd element is AWS secret access key

import csv

def read():
    with open('credentials.txt') as file:
        reader=csv.reader(file, delimiter=',')
        line_count=0
        for row in reader:
            if line_count==0:
                key_ID=row[1]
            elif line_count==1:
                secret_access_key=row[1]
            elif line_count==2:
                db_host=row[1]
            elif line_count==3:
                db_password=row[1]
            line_count+=1
    return [key_ID, secret_access_key, db_host, db_password]
