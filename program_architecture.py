import boto3
import mysql.connector
import csv
import json

RDS_NAME = 'rds_project'


def create_rds():
    '''
    Set up RDS and adjust permissions.
    '''

    # Set up RDS:
    print('Creating RDS...')
    rds_instance_id = 'relational-db-project'
    RDS_NAME = 'rds_project'
    rds = boto3.client('rds')
    # Do nothing if identifier already exists:
    try:
        response = rds.create_db_instance(
            DBInstanceIdentifier=rds_instance_id,
            DBName=RDS_NAME,
            MasterUsername='username',
            MasterUserPassword='password',
            DBInstanceClass='db.t2.micro',
            Engine='mysql',
            AllocatedStorage=5
        )
    except rds.exceptions.DBInstanceAlreadyExistsFault:
        print(f'RDS {RDS_NAME} already exists.')
    # Wait until DB is available to continue
    rds.get_waiter('db_instance_available')\
        .wait(DBInstanceIdentifier=rds_instance_id)
    # Describe where DB is available and on what port
    db = rds.describe_db_instances()['DBInstances'][0]
    PORT = db['Endpoint']['Port']
    # Get Name of Security Group
    SGNAME = db['VpcSecurityGroups'][0]['VpcSecurityGroupId']
    # Adjust Permissions for that security group:
    print('Adjusting permissions...')
    try:
        ec2 = boto3.client('ec2')
        data = ec2.authorize_security_group_ingress(
                GroupId=SGNAME,
                IpPermissions=[
                    {'IpProtocol': 'tcp',
                    'FromPort': PORT,
                    'ToPort': PORT,
                    'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}
                ]
        )
    # If SG is already adjusted, print this out:
    except ec2.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == 'InvalidPermission.Duplicate':
            print("Permissions already adjusted.")
        else:
            print(e)
    return


def set_rds_tables():
    '''
    Create diversity_scores and posts tables in RDS. Add data from 
    diversity_table.csv to diversity_scores. If tables already exist, reset 
    them.
    '''

    print('Creating tables in RDS...')

    # Connect to RDS:
    rds = boto3.client('rds')
    db = rds.describe_db_instances()['DBInstances'][0]
    ENDPOINT = db['Endpoint']['Address']
    PORT = db['Endpoint']['Port']

    # Connect to MySQL:
    conn =  mysql.connector.connect(host=ENDPOINT,
                                    user="username",
                                    passwd="password", 
                                    port=PORT, 
                                    database=RDS_NAME)
    cur = conn.cursor()

    # Check if tables exist:
    schema = f'''
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = "{RDS_NAME}"
    '''
    cur.execute(schema)
    table_names = [table[0] for table in cur.fetchall()]
    to_reset = set(table_names.copy())
    for table in table_names:
        print(f'{table} already exists. Do you wish to reset this table? (Y/N)')
        entry = input()
        if entry == 'Y':
            print(f'Dropping {table}...')
            schema = f'''
                DROP TABLE {table};
            '''
            cur.execute(schema)
        else:
            to_reset.remove(table)

    # Create diversity table:
    table_name = 'diversity_scores'
    print(f'Setting {table_name}...')
    schema = f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            overall_rank INT,
            city VARCHAR(255),
            total_score FLOAT,
            socioeconomic_diversity INT,
            cultural_diversity INT,
            economic_diversity INT,
            household_diversity INT,
            religious_diversity INT,
            reddit_handle VARCHAR(255),
            bad_handle CHAR(10)
        )
    '''
    cur.execute(schema)
    conn.commit()
    if table_name in to_reset:
        # Add diverstiy_table.csv to diversity table:
        print(f'Adding data to {table_name}...')
        file_path = 'data/diversity_table.csv'
        with open(file_path, 'r') as f:
            csv_data = csv.reader(f)
            # Skip header row:
            next(csv_data)
            # Insert data row by row:
            insert_query = f"""
                INSERT INTO {table_name} (overall_rank, city, total_score, 
                                socioeconomic_diversity, cultural_diversity, 
                                economic_diversity, household_diversity, 
                                religious_diversity, reddit_handle, bad_handle) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, False)
            """
            cur = conn.cursor() # Trying bad handle
            data_to_insert = [tuple(row) for row in csv_data]
            cur.executemany(insert_query, data_to_insert)
            conn.commit()
            
    # Create posts table:
    print('Setting posts if table does not exist already...')
    table_name = 'posts'
    schema = f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            reddit_handle VARCHAR(255),
            timestamp DATETIME,
            title TEXT,
            body TEXT
        )
    '''
    cur.execute(schema)
    conn.commit()
    # Close mysql connection:
    conn.close()
    return


def program_lambda_function():
    '''
    Set up Lambda function and Step function.
    '''
    # Set up Lambda function:
    print('Setting up Lambda function...')
    lambda_function_name = 'project_scraping'
    aws_lambda = boto3.client('lambda')
    iam_client = boto3.client('iam')
    role = iam_client.get_role(RoleName='LabRole')
    with open('deployment_package.zip', 'rb') as f:
        lambda_zip = f.read()
    try:
        # If function hasn't yet been created, create it:
        response = aws_lambda.create_function(
            FunctionName=lambda_function_name,
            Runtime='python3.9',
            Role=role['Role']['Arn'],
            Handler='lambda_function.lambda_handler',
            Code=dict(ZipFile=lambda_zip),
            Timeout=180
        )
    except aws_lambda.exceptions.ResourceConflictException:
        # If function already exists, update it based on zip file contents:
        response = aws_lambda.update_function_code(
        FunctionName=lambda_function_name,
        ZipFile=lambda_zip
        )
    lambda_arn = response['FunctionArn']

    # Set up step functions:
    print('Setting up step function...')
    sf_name = 'sf_project'
    sfn = boto3.client('stepfunctions')
    def make_def(lambda_arn):
        definition = {
        "Comment": "My State Machine",
        "StartAt": "Map",
        "States": {
            "Map": {
            "Type": "Map",
            "End": True,
            "Iterator": {
                "StartAt": "Lambda Invoke",
                "States": {
                "Lambda Invoke": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke",
                    "OutputPath": "$.Payload",
                    "Parameters": {
                    "Payload.$": "$",
                    "FunctionName": lambda_arn
                    },
                    "Retry": [
                    {
                        "ErrorEquals": [
                        "Lambda.ServiceException",
                        "Lambda.AWSLambdaException",
                        "Lambda.SdkClientException",
                        "Lambda.TooManyRequestsException",
                        "States.TaskFailed"
                        ],
                        "IntervalSeconds": 2,
                        "MaxAttempts": 6,
                        "BackoffRate": 2
                    }
                    ],
                    "End": True
                }
                }
            }
            }
        }
        }
        return definition
    sf_def = make_def(lambda_arn)
    try:
        response = sfn.create_state_machine(
            name=sf_name,
            definition=json.dumps(sf_def),
            roleArn=role['Role']['Arn'],
            type='EXPRESS'
        )
    except sfn.exceptions.StateMachineAlreadyExists:
        response = sfn.list_state_machines()
        state_machine_arn = [sm['stateMachineArn'] 
                            for sm in response['stateMachines'] 
                            if sm['name'] == sf_name][0]
        response = sfn.update_state_machine(
            stateMachineArn=state_machine_arn,
            definition=json.dumps(sf_def),
            roleArn=role['Role']['Arn']
        )
    print('Done!')
    return


def program_all():
    '''
    Program everything
    '''
    print('Are you sure you want to program \
all levels of this architecture? Y/N')
    entry = input()
    if entry == 'Y':
        create_rds()
        set_rds_tables()
        program_lambda_function()
    return
