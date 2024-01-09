import azure.functions as func
import datetime
import json
import logging
import requests
from requests_oauthlib import OAuth1
import os
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

consumer_key = config['DEFAULT']['consumer_key']
consumer_secret = config['DEFAULT']['consumer_secret']
access_token = config['DEFAULT']['access_token']
access_token_secret = config['DEFAULT']['access_token_secret']

app = func.FunctionApp()

def random_fact():
    return "Hello world !"


def format_fact(fact):
    return {"text": "{}".format(fact)}


def connect_to_oauth(consumer_key, consumer_secret, acccess_token, access_token_secret):
    url = "https://api.twitter.com/2/tweets"
    auth = OAuth1(consumer_key, consumer_secret, acccess_token, access_token_secret)
    return url, auth


def hello_pubsub():
    fact = random_fact()
    payload = format_fact(fact)
    url, auth = connect_to_oauth(
        consumer_key, consumer_secret, access_token, access_token_secret
    )
    logging.info("Auth ok !")
    request = requests.post(
        auth=auth, url=url, json=payload, headers={"Content-Type": "application/json"}
    )
    logging.info(request.text)

@app.route(route="TweetLauncher", auth_level=func.AuthLevel.ANONYMOUS)
def TweetLauncher(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    hello_pubsub()

    return func.HttpResponse(
             "This HTTP triggered function executed successfully.",
             status_code=200
        )