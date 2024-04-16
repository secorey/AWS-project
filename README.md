# AWS Project
#### Sam Corey
#### 26 May 2023

## Research Problem
Often, who we surround ourselves with informs our opinions, thoughts, and attitudes. As a result, groups of people with already similar opinions are unlikely to see much change in their beliefs. On the flip side, being in the context of others with diverse backgrounds may cause more changes in a person's beliefs.

Diversity is mostly seen as a positive thing, but it can exist in different forms, and be beneficial for different reasons. For example, a university may want its students to come from a diverse set of socioeconomic backgrounds. A basketball team wants players with a diverse set of skills so their team can be well-balanced in their offense and defense. A jury may value diverse jurors so that their opinions are the most representative of society. Furthermore, diversity is highly valued in the workplace because not only does it represent fairness in opportunity, but whatever entity holds that diversity benefits from having the perspectives of people of different backgrounds.

If we know that diversity affects opinions, and yet there are different types of diversity, how might these different types of diversity affect differences in opinions? In other words, how much does the type of diversity mediate the relationship between increases in diversity and changes in opinions?

The present study looked at different types of diversity and compared how cities with high diversity on those axes differ from cities with low diversity on those axes in the topics that their citizens discuss online. Understanding how the relationship between increases in diversity and changes of opinion is mediated by the type of diversity can help us to understand how human thinking is influenced by exposure to other groups around us in addition to understanding what types of diversity society should emphasize to best contribute to human ingenuity and progress.

## Methods
The internet has given social science researchers massive amounts of data that can be used to study human behavior. Thus, the present study examined online topics discussed by people in different communities in the United States. While online discussed topics are not a direct measure of opinions, understanding what people talk about serves as a small window into their interests and values.

To examine how the relationship between an increase in diversity and changes of opinion is mediated by the type of diversity, this study compared online posts between cities that are very diverse and cities that are not as diverse. The types of diversity this study compared were: general, socioeconomic, cultural, economic, household, and religious (data gathered from [this webite](https://wallethub.com/edu/most-diverse-cities/12690)). The cities that this study examined were 500 of the most populous cities in the United States. The online posts analyzed in this study were posts to each city's Reddit page. This study used topic models to analyze posts and compared the most frequent topics across cities.

The external validity of these methods relies on sampling a large number of posts from a large number of cities in the United States. In total, the number of posts gathered was ~250,000 posts from ~500 different web pages. Thus, a scalable solution was necessary for both the gathering of the data and the analysis.

## Running this Program
### Gather data
#### Getting the diversity table
[get_diversity_table.py](https://github.com/secorey/AWS-project/blob/main/get_diversity_table.py)
```
python3 get_diversity_table.py
```
NOTE: Must have a Google Chrome browser and ChromeDriver installed. This program was written using Chrome version 112, and thus used ChromeDriver version 112. See info on installation here: https://chromedriver.chromium.org/downloads

Running this program will result in the [data/diversity_table.csv](https://github.com/secorey/AWS-project/blob/main/data/diversity_table.csv) dataset, which holds the different diversity scores for 500 of the most populous cities in the United States. An additional column for Reddit handles was added. Note that, while most Reddit handles could be generated automatically from the city names themselves, not every Reddit handle follows this formula (especially when there are repeat city names). So, some hand-coding had to be done to this dataset for it to exist in its current form. This program only scrapes one web page, so it can be done in serial, but gathering this information is essential for the rest of the pipeline.

#### Programming all AWS architecture
[program_architecture.py](https://github.com/secorey/AWS-project/blob/main/program_architecture.py)

This program sets up RDS (```create_rds()```), creates two RDS tables ('posts' and 'diversity_scores') and adds data from diversity_table.csv to the 'diversity_scores' table in RDS (```set_rds_tables()```), and sets up the Lambda Function and Step Function (```program_lambda_function()```). Within this script, each function represents a different part of the architecture to set up, so each can be run individually. However, all architecture can be set up from scratch by running the ```program_all()``` function:
```
python3 -c "from program_architecture import program_all; program_all()"
```

##### Lambda function
[deployment_package/lambda_function.py](https://github.com/secorey/AWS-project/blob/main/deployment_package/lambda_function.py)

NOTE: The entire deployment package is too large to have on GitHub, but it includes the requests package and the mysql-connector-python package.

This function receives a list of Reddit handles from the step function and scrapes the 1,000 most recent posts from each page. All posts from all pages are held in memory before being sent together to the 'posts' table in RDS along with their respective Reddit handles. Another column is included in the 'diversity_scores' table, 'bad_handle', which is changed to 'True' if the Lambda worker attempts to scrape that Reddit page but is unable to (usually because the page does not exist). Having this feature helped to recognize the inaccurate Reddit handles so they could be changed or removed.

#### Deploying the Step Function
[scrape_reddit.py](https://github.com/secorey/AWS-project/blob/main/scrape_reddit.py)
```
python3 scrape_reddit.py
```

This script invokes a step function that invokes 50 Lambda workers. First, this script gathers the ~500 Reddit handles from the 'diversity_scores' table in RDS. Then, it passes the handles evenly to the Lambda workers.

### Generate topic models
[project_topic_models.ipynb](https://github.com/secorey/AWS-project/blob/main/project_topic_models.ipynb)
To run the topic models, I used PySpark on an EMR cluster within AWS. To create the EMR cluster, run the following command in the terminal, replacing the relevant S3 bucket name:
```
aws emr create-cluster \
    --name "Spark Cluster" \
    --release-label "emr-6.2.0" \
    --applications Name=Hadoop Name=Hive Name=JupyterEnterpriseGateway Name=JupyterHub Name=Livy Name=Pig Name=Spark Name=Tez \
    --instance-type m5.xlarge \
    --instance-count 3 \
    --use-default-roles \
    --region us-east-1 \
    --ec2-attributes '{"KeyName": "vockey"}' \
    --configurations '[{"Classification": "jupyter-s3-conf", "Properties": {"s3.persistence.enabled": "true", "s3.persistence.bucket": "project-secorey"}}]'
```
I used a Jupyter Notebook to run the topic models. To do so, once the EMR cluster has finished setting up, run the following command in the terminal, replacing the path to PEM file and relevant primary node public DNS as necessary:
```
ssh -i ~/.aws/labsuser.pem -NL 9443:localhost:9443 hadoop@ec2-54-198-253-95.compute-1.amazonaws.com
```
Then, direct to https://localhost:9443 on the web browser. Username: jovyan ; Password: jupyter

Upload this file to the server:
[project_topic_models.ipynb](https://github.com/secorey/AWS-project/blob/main/project_topic_models.ipynb)

NOTE: To run the Jupyter noteboook, you must install a package on the primary EC2 instance. To do so, while SSHed into the primary node, run:
```
sudo pip3 install sparknlp boto3 mysql-connector-python
```

This notebook generates a Latent Dirilecht Allocation (LDA) topic model for different subgroups of the posts. First, both tables in RDS are loaded into Spark dataframes. Then, two topic models are created for each of the 6 diversity axes, with one topic model corresponding to the most diverse cities and the other topic model corresponding to the least diverse cities. For example, to construct the topic models for religious diversity, posts from the top 100 most religiously diverse cities were compared to the top 100 least religiously diverse cities.

## Results
All topic models are displayed in [project_topic_models.ipynb](https://github.com/secorey/AWS-project/blob/main/project_topic_models.ipynb). Overall, results are mostly qualitative, but we do see differences in topics of cities that are more diverse compared to topics that are less diverse - the most overall diverse cities had more mentions of food while the least overall diverse had more mentions of politics. The difference in topics is not, as stark, however, for religious diversity. In general, topics for extremely religiously diverse cities were not that different from topics for non-religiously diverse cities. This suggests that religious diversity does not contribute much to the things that people talk about, and thus the opinions, beliefs, and attitudes that people may have.
