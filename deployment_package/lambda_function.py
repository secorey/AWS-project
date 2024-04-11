import requests
import boto3
import mysql.connector
from datetime import datetime


def process_words(text):
    '''
    Remove newlines from a piece of text.
    '''
    text = text.replace('|', ' ')
    return text.replace('\n', ' ')


def scrape(reddit_handle):
    '''
    This function scrapes the 1,000 most recent posts from a reddit page.

    Inputs: 
      reddit_handle (str): the name of the reddit page

    Returns:
      all_posts (list): a list of posts, where each post is represented as a 
      dictionary containing the reddit handle, title, body, and timestamp of the
      post
    '''
    print('Reddit handle: ', reddit_handle)
    # Information for scraping Reddit data:
    auth = requests.auth.HTTPBasicAuth('auth_1', 'auth_2')
    data = {'grant_type': 'password',
            'username': '',
            'password': ''}
    headers = {'User-Agent': 'MyBot/0.0.1'}
    res = requests.post('https://www.reddit.com/api/v1/access_token',
                        auth=auth, data=data, headers=headers)
    TOKEN = res.json()['access_token']
    headers = {**headers, **{'Authorization': f"bearer {TOKEN}"}}

    earliest_post = None
    all_posts = []
    while True:
        try:
            res = requests.get(f"https://oauth.reddit.com/r/{reddit_handle}/new", 
                        headers=headers,
                        params={'limit': '100',
                                'after': earliest_post})
        except:
            continue
        try:
            posts = res.json()['data']['children']
        except KeyError:
            print(f'Posts could not be gathered for {reddit_handle}.')
            return all_posts

        if len(posts) == 0:
            break
        for post in posts:
            new_post = {}
            # Process title and body:
            try:
                new_post['reddit_handle'] = reddit_handle
                new_post['title'] = process_words(post['data']['title'])
                new_post['body'] = process_words(post['data']['selftext'])
                dt = datetime.fromtimestamp(post['data']['created_utc'])
                new_post['timestamp'] = dt.strftime('%Y-%m-%d %H:%M:%S')
            except KeyError:
                print(f'There was an error in collecting posts for \
                      {reddit_handle}.')
                return all_posts
            all_posts.append(new_post)
        earliest_post = post['data']['name']
    return all_posts


def lambda_handler(event, context):
    '''
    Given a reddit handle, this program will scrape the 1,000 most recent posts 
    from that handle. Then, it will add those posts to RDS with their 
    corresponding handle.
    '''

    # Gather posts:
    reddit_handles = event['reddit_handle']
    # Generate a list of handles that cause an error or do not yield results:
    bad_handles = []
    posts = []
    for reddit_handle in reddit_handles:
        if reddit_handle in {'', 'NaN'}:
            continue
        new_posts = scrape(reddit_handle)
        # Scrape function will return an empty list if there is an error:
        if new_posts == []:
            bad_handles.append(reddit_handle)
        posts += new_posts

    # Connect to RDS:
    rds = boto3.client('rds')
    db = rds.describe_db_instances()['DBInstances'][0]
    ENDPOINT = db['Endpoint']['Address']
    PORT = db['Endpoint']['Port']
    rds_name = 'rds_project'
    conn =  mysql.connector.connect(host=ENDPOINT,
                                    user="username",
                                    passwd="password", 
                                    port=PORT, 
                                    database=rds_name)
    cur = conn.cursor()

    # Add posts to RDS:
    table_name = 'posts'
    col_names = ['reddit_handle', 'title', 'body', 'timestamp']
    insert_query = f"""
        INSERT INTO {table_name} ({', '.join(col_names)}) 
        VALUES (%s, %s, %s, %s)
    """
    data_to_insert = [[post[col] for col in col_names] for post in posts]
    cur.executemany(insert_query, data_to_insert)
    conn.commit()

    # Add any bad handles to RDS:
    table_name = 'diversity_scores'
    query = f"""
        UPDATE {table_name}
        SET bad_handle = True
        WHERE reddit_handle = %s
    """
    for handle in bad_handles:
        cur.execute(query, (handle,))
        conn.commit()

    # Close MySQL connection:
    conn.close()
    return
