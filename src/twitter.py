import os, subprocess, glob
import tweepy as tw
import pandas as pd
from dotenv import load_dotenv

def get_twitter_santander_data():
    load_dotenv()
    consumer_key= os.getenv("CONSUMER_KEY")
    consumer_secret= os.getenv("CONSUMER_SECRET")
    access_token= os.getenv("ACCESS_TOKEN")
    access_token_secret= os.getenv("ACCESS_TOKE_SECRET")

    auth = tw.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tw.API(auth, wait_on_rate_limit=True)

    tweets = tw.Cursor(api.search_tweets, q="#Santander",
                        lang="en",).items(200)

    resultado = [
        {
            "id": tweet._json["id"], "user_id": tweet._json["user"]["id"], "geolocation": tweet._json["geo"], "retweet_count": tweet._json["retweet_count"], 
            "texto": tweet._json["text"].replace("\n", "").replace(";", ","), "in_reply_to_user_id": tweet._json["in_reply_to_user_id"]
        } for tweet in tweets]
    data =  pd.DataFrame(resultado)
    subprocess.run("mkdir -p ./tmp2".split(" "))
    data.to_csv("./tmp2/twitter_data.csv", sep=';', index=False)

def put_twitter_santander_data():
    subprocess.run("hdfs dfs -mkdir -p /marketing_santander_data".split(" "))
    for file in glob.glob("./tmp2/*"):
        put_command = f"hdfs dfs -put {file} /marketing_santander_data"
        subprocess.run(put_command.split())

    check_if_exists = "hdfs dfs -ls /marketing_santander_data"
    process2 = subprocess.Popen(check_if_exists.split(), stdout=subprocess.PIPE)
    output, error = process2.communicate()
    if "twitter_data.csv" in output.decode("utf-8"):
        subprocess.run("rm -r ./tmp2".split())
    return
