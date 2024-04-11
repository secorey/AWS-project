'''
This script deploys the step function that distributes ~500 Reddit handles 
evenly to 50 Lambda workers to scrape the posts from the Reddit pages. The 
Reddit handles are gathered from RDS.
'''

import boto3
import mysql.connector
import json
import numpy as np


def split_list(lst, num_groups):
    '''
    Splits a list of items into 50 groups of mostly equal size.
    '''
    lengths = np.full(num_groups, len(lst) // num_groups)
    lengths[:len(lst) % num_groups] += 1
    return np.split(lst, np.cumsum(lengths))


rds_name = 'rds_project'
sf_name = 'sf_project'
sm_name = 'sm_project'

# Gather Reddit handles:
rds = boto3.client('rds')
db = rds.describe_db_instances()['DBInstances'][0]
ENDPOINT = db['Endpoint']['Address']
PORT = db['Endpoint']['Port']
conn =  mysql.connector.connect(host=ENDPOINT,
                                user="username",
                                passwd="password", 
                                port=PORT, 
                                database=rds_name)
cur = conn.cursor()
query = """
    SELECT reddit_handle
    FROM diversity_scores
"""
cur.execute(query)
reddit_handles = [handle[0] for handle in cur.fetchall()]
reddit_handles = split_list(reddit_handles, 50)[:-1]
reddit_handles = \
    [{'reddit_handle': list(handles)} for handles in reddit_handles]
conn.close()

# Deploy Step Function:
sfn = boto3.client('stepfunctions')
response = sfn.list_state_machines()
# Get arn for Step Function state machine:
state_machine_arn = [sm['stateMachineArn'] 
                    for sm in response['stateMachines'] 
                    if sm['name'] == sf_name][0]

response = sfn.start_execution(
    stateMachineArn=state_machine_arn,
    name=sm_name,
    input=json.dumps(reddit_handles)
)