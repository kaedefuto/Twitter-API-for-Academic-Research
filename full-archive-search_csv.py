import requests
#import os
import json
import pandas as pd
import re
#import csv
import time

# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'

#bearer_token = os.environ.get("")
bearer_token = ""
search_url = "https://api.twitter.com/2/tweets/search/all"

# Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
# expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields
#query_params = {'query': '(from:twitterdev -is:retweet) OR #twitterdev','tweet.fields': 'author_id'}

query = "台風"
start_time = "2021-06-12T01:00:00+09:00"
end_time = "2020-06-12T01:03:00+09:00"
since_id = ""
until_id = ""
max_results = 500
next_token =  ""
"""
query_params = {'query': query,
                'start_time' : start_time,
                'end_time' : end_time,
                'max_results' : max_results,
                'tweet.fields' : "created_at",
                #'expansions' : "author_id",
                #'user.fields' : "created_at"
                }
"""

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", search_url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def shape_data(data):
    for i, d in enumerate(data):
        
        data[i]["text"] = d["text"].encode("cp932",errors="ignore").decode("cp932")
        # 全角スペース、タブ、改行を削除
        data[i]["text"] = re.sub(r"[\u3000\t\n\r\r\n]", "", d["text"])
        data[i]["text"] = re.sub(r"[\r\r\n]", "", d["text"])
    return data

def main():
    
    request_iterator = 0
    next_token = ""
    break_flag = False
    df = pd.DataFrame()
    df_split = pd.DataFrame()
    count=0
    # 次ページがなくなるまで次ページのクエリを取得
    while True:
        try:
            data2["next_token"]
        except KeyError: # 次ページがない(next_tokenがない)場合はループを抜ける
            del data2
            break_flag = True
        except NameError: # TARGET_WORDS内の各要素で初めてAPIを取得するとき
            print("---start---")
        else: # 2ページめ以降の処理
            next_token = data2["next_token"]
        finally:
            if break_flag == True: 
                break
            if count==0:
                query_params = {'query': query,
                                'start_time' : start_time,
                                'end_time' : end_time,
                                'max_results' : max_results,
                                'tweet.fields' : "created_at",
                                }
                
            else:
                query_params = {'query': query,
                                'start_time' : start_time,
                                'end_time' : end_time,
                                'max_results' : max_results,
                                'tweet.fields' : "created_at",
                                'next_token' : next_token
                                }
            
            headers = create_headers(bearer_token)
            json_response = connect_to_endpoint(search_url, headers, query_params)
            data =  json_response["data"]
            data2 =  json_response["meta"]

            df1 = pd.DataFrame(shape_data(data))
            df_split = pd.concat([df_split, df1])

            df = pd.concat([df, df1])

            request_iterator += 1
            time.sleep(2)#App rate limit: 1 request per second

            if count!=0 and count%1000==0:#Excel用
                df_split.to_csv("result_{}.csv".format(count*500),encoding="cp932",header=False, index=False)
                df_split = pd.DataFrame()
                print("1000回リクエスト")
                
            if request_iterator >= 300: # 300requestを超えたら止める
                print("App rate limit: 300 requests per 15-minute window")
                time.sleep(16.00*60) #App rate limit: 300 requests per 15-minute window
                request_iterator = 0

            print("{}回目".format(count))
            count+=1
    print(str(count)+ "回リクエストしました")
    df.to_csv("result.csv",encoding="cp932",header=False, index=False)


if __name__ == "__main__":
    main()
