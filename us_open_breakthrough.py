import tweepy
import pandas as pd
import requests
from urllib.parse import unquote
from datetime import datetime
from datetime import timedelta
import flair
import re
import bs4
import pytz
import math
import time
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
import threading

#Initial Setup
class TradingApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self,self)
        self.pos_df = pd.DataFrame(columns=['Account', 'Symbol', 'SecType',
                                            'Currency', 'Position', 'Avg cost'])
        
    def error(self, reqId, errorCode, errorString):
        print("Error {} {} {}".format(reqId,errorCode,errorString))
        
    def nextValidId(self, orderId):
        super().nextValidId(orderId)
        self.nextValidOrderId = orderId
        print("NextValidId:", orderId)
        
    def position(self, account, contract, position, avgCost):
        super().position(account, contract, position, avgCost)
        dictionary = {"Account":account, "Symbol": contract.symbol, "SecType": contract.secType,
                      "Currency": contract.currency, "Position": position, "Avg cost": avgCost}
        self.pos_df = self.pos_df.append(dictionary, ignore_index=True)

def websocket_con():
    app.run()

def pause_until(hour, minute, second, timezone):
    tz = pytz.timezone(timezone)
    while True:
        time_now = datetime.now(tz)
        if time_now.hour == hour and time_now.minute == minute and time_now.second == second:
            break
        
def usSmartStock(symbol):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "STK"
    contract.currency = "USD"
    contract.exchange = "SMART"
    contract.primaryExchange = "SMART"
    return contract

def stopOrder(action, stopPrice, quantity):
    order = Order()
    order.action = action
    order.orderType = "STP"
    order.auxPrice = stopPrice
    order.totalQuantity = quantity
    return order

def marketOrder(action, quantity):
    order = Order()
    order.action = action
    order.orderType = "MKT"
    order.totalQuantity = quantity
    return order

def marketIfTouched(action, price, quantity):
    order = Order()
    order.action = action
    order.orderType = "MIT"
    order.totalQuantity = quantity
    order.auxPrice = price
    return order

#Main Code
def yfin_premkt(symbol):
    url = 'https://finance.yahoo.com/quote/'+str(symbol)
    sess = requests.session()
    headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}
    req = sess.get(url, headers=headers)
    soup = bs4.BeautifulSoup(req.text, features='lxml')
    return float((soup.find('span', class_='C($primaryColor) Fz(24px) Fw(b)').string).replace(',',''))

def lineNotifyMessage(token, msg):
      headers = {"Authorization": "Bearer " + token, 
                 "Content-Type" : "application/x-www-form-urlencoded"}
      payload = {'message': msg}
      r = requests.post("https://notify-api.line.me/api/notify", headers = headers, params = payload)

def s2p(x):
    try:
        return float(x.strip('%'))/100
    except:
        return 0

def s2f(x):
    try:
        return float(x.replace(',', ''))
    except:
        return 0

def cleanTxt(text):
    text = re.sub('@[A-Za-z0-9]+', '', text)
    text = re.sub('#', '', text)
    text = re.sub('RT[\s]+', '', text)
    text = re.sub('https?:\/\/\S+', '', text)
    return text              

def avgList(lst):
    if sum(lst) == 0:
        return 0
    else:
        return sum(lst)/len(lst)

pause_until(9,25,0,'US/Eastern')

main_url = 'https://www.barchart.com/'

api_url = 'https://www.barchart.com/proxies/core-api/v1/quotes/get'

cookies_headers = {'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                  'accept-encoding': 'gzip, deflate, br',
                  'accept-language': 'en-US,en;q=0.9',
                  'cache-control': 'max-age=0',
                  'upgrade-insecure-requests': '1',
                  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36'}

cookies_params = {'page': 'all'}

s = requests.Session()
cookies_request = s.get(main_url, params = cookies_params, headers = cookies_headers)

api_headers = {'accept': 'application/json',
               'accept-encoding': 'gzip, deflate, br',
               'accept-language': 'en-US,en;q=0.9',
               'referer': 'https://www.barchart.com/stocks/performance/gap/gap-up?viewName=main&page=all',
               'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36',
               'x-xsrf-token': unquote(unquote(s.cookies.get_dict()['XSRF-TOKEN']))}

gap_up_params = {'fields': 'symbol,symbolName,lastPrice,priceChange,percentChange,gapUp,gapUpPercent,highPrice,lowPrice,volume,tradeTime,symbolCode,symbolType,hasOptions',
                  'list': 'stocks.gaps.up.us',
                  'root': 'CL',
                  'orderBy': 'gapUpPercent',
                  'orderDir': 'desc',
                  'meta': 'field.shortName,field.type,field.description',
                  'hasOptions': 'true',
                  'raw': '1'}

gap_down_params = {'fields': 'symbol,symbolName,lastPrice,priceChange,percentChange,gapDown,gapDownPercent,highPrice,lowPrice,volume,tradeTime,symbolCode,symbolType,hasOptions',
                  'list': 'stocks.gaps.down.us',
                  'root': 'CL',
                  'orderBy': 'gapDownPercent',
                  'orderDir': 'asc',
                  'meta': 'field.shortName,field.type,field.description',
                  'hasOptions': 'true',
                  'raw': '1'}

gap_up_req = s.get(api_url, params = gap_up_params, headers = api_headers)
gap_up_json = gap_up_req.json()

gap_down_req = s.get(api_url, params = gap_down_params, headers = api_headers)
gap_down_json = gap_down_req.json()

gap_up_df = pd.DataFrame(gap_up_json['data'])
# gap_up_df.to_csv('gap_up_us_'+str(datetime.today().year)+'_'+str(datetime.today().month)+'_'+str(datetime.today().day)+'.csv', index=False)
gap_down_df = pd.DataFrame(gap_down_json['data'])
# gap_down_df.to_csv('gap_down_us_'+str(datetime.today().year)+'_'+str(datetime.today().month)+'_'+str(datetime.today().day)+'.csv', index=False)

gap_up_df['gapUpPercent'] = gap_up_df['gapUpPercent'].apply(s2p)
gap_up_df['highPrice'] = gap_up_df['highPrice'].apply(s2f)
gap_up_df = gap_up_df.loc[gap_up_df['gapUpPercent'] >= 0.04]
gap_up_symbols = gap_up_df['symbol'].to_list()
pre_mkt_highs = gap_up_df['highPrice'].to_list()
for i in range(len(gap_up_symbols)):
    try:
        quote = yfin_premkt(gap_up_symbols[i])
        if quote > pre_mkt_highs[i]:
            pre_mkt_highs[i] = quote
    except AttributeError:
        continue

# Twitter API keys
consumer_key = "XXXXXXXXXXXXXXXXXXXXX" 
consumer_secret = "XXXXXXXXXXXXXXXXXXXXX"
access_token = "XXXXXXXXXXXXXXXXXXXXX"
access_token_secret = "XXXXXXXXXXXXXXXXXXXXX"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

#print(datetime.utcnow())

sentiment_model = flair.models.TextClassifier.load('en-sentiment')

avg_sentiment_score = []
tweets = []
scores = []

for stock in gap_up_symbols:
    cursor = tweepy.Cursor(api.search, q='$'+stock, result_type='mixed', tweet_mode='extended', lang='en').items(round(1800/gap_up_df.shape[0]))
    for i in cursor:
        if datetime.utcnow()-i.created_at <= timedelta(days=1):
            tweets.append(cleanTxt(i.full_text))
    for tweet in tweets:
        tweet = flair.data.Sentence(tweet)
        sentiment_model.predict(tweet)
        if tweet.labels[0].value == 'POSITIVE':
            scores.append(tweet.labels[0].score)
        elif tweet.labels[0].value == 'NEGATIVE':
            scores.append(-tweet.labels[0].score)
    avg_sentiment_score.append(avgList(scores))
    scores=[]
    tweets =[]
    
sentiment_df = pd.DataFrame(list(zip(gap_up_symbols, avg_sentiment_score, pre_mkt_highs)), columns = ['Symbol', 'Score', 'Stop Price'])
sentiment_df = sentiment_df.loc[sentiment_df['Score'] > 0]
sentiment_df = sentiment_df.reset_index(drop=True)

line_token = 'XXXXXXXXXXXXXXXXXXXXX'
lineNotifyMessage(line_token, sentiment_df.to_string().rjust(1+len(sentiment_df.to_string()), '\n'))

if not sentiment_df.empty:
    app = TradingApp()      
    app.connect("XXX.X.X.X", 0000, clientId=2) # change according to GateWay settings
    con_thread = threading.Thread(target=websocket_con, daemon=True)
    con_thread.start()
    time.sleep(1)
    
    pause_until(9,30,5,'US/Eastern')
    order_id = app.nextValidOrderId
    
    order_list = []
    
    for i in range(sentiment_df.shape[0]):
        app.placeOrder(order_id,usSmartStock(sentiment_df['Symbol'][i]),marketIfTouched("SELL",sentiment_df['Stop Price'][i],math.floor(1000/sentiment_df['Stop Price'][i])))
        order_list.append(order_id)
        order_id += 1
        
    # time.sleep(5)
    
    pause_until(10,0,0,'US/Eastern')
    app.reqGlobalCancel()
    time.sleep(1)
    app.reqPositions()
    time.sleep(1)
    pos_df = app.pos_df
    print(pos_df)
    
    for j in range(pos_df.shape[0]):
        if pos_df['Symbol'][j] in set(sentiment_df['Symbol']) and pos_df['Position'][j] != 0:
            app.placeOrder(order_id,usSmartStock(pos_df['Symbol'][j]),marketOrder("BUY",abs(pos_df['Position'][j])))
            order_id += 1
            
    # app.close()
