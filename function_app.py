import azure.functions as func
import datetime
import json
import logging
import requests
from requests_oauthlib import OAuth1
from tweet_generator import tweet_text_generator
from dotenv import load_dotenv
import os

load_dotenv()
consumer_key = os.environ.get('consumer_key')
consumer_secret = os.environ.get('consumer_secret')
access_token = os.environ.get('access_token')
access_token_secret = os.environ.get('access_token_secret')
environment = os.environ.get('ENVIRONMENT')

csv_file = 'martyrs.csv'
position_file = 'last_position.txt' # File to store the last read row number
json_file = 'tweets_content.json'

app = func.FunctionApp()

def format_tweet(fact):
    return {"text": "{}".format(fact)}


def connect_to_oauth(consumer_key, consumer_secret, acccess_token, access_token_secret):
    url = "https://api.twitter.com/2/tweets"
    auth = OAuth1(consumer_key, consumer_secret, acccess_token, access_token_secret)
    return url, auth


def run():
    tweet, new_start_row = tweet_text_generator(csv_file, position_file, json_file)
    chars_count = len(tweet)
    logging.info("Generated tweet text with %d characters", chars_count)
    logging.info("New start row: %d", new_start_row)
    if chars_count > 280:
        logging.warning("Tweet text is too long")
        return False
    
    payload = format_tweet(tweet)
    url, auth = connect_to_oauth(
        consumer_key, consumer_secret, access_token, access_token_secret
    )
    logging.info("Authentication to X is successful.")
    request = requests.post(
        auth=auth, url=url, json=payload, headers={"Content-Type": "application/json"}
    )
    logging.info(request.text)
    if request.status_code != 201:
        return False
    return True
    

@app.route(route="TweetLauncher", auth_level=func.AuthLevel.ANONYMOUS)
def TweetLauncher(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    isExecutedSuccessfully = run()

    logging.info('This HTTP triggered function executed successfully: %s', isExecutedSuccessfully)

    response = func.HttpResponse("This HTTP triggered function executed successfully.", status_code=200) if isExecutedSuccessfully else func.HttpResponse("This HTTP triggered function did not execute successfully.", status_code=500)

    return response