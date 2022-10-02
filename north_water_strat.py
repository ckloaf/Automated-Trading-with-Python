import requests
import bs4
import re
from datetime import datetime, timedelta
import datetime as dt
import pytz
import pandas as pd
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.execution import *
import threading
import time
import urllib.request as req

#Initial Setup
class TradingApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self,self)
        self.pos_df = pd.DataFrame(columns=['Account', 'Symbol', 'SecType',
                                            'Currency', 'Position', 'Avg cost'])
        self.execution_df = pd.DataFrame(columns=['ReqId', 'PermId', 'Symbol',
                                                  'SecType', 'Currency', 'ExecId',
                                                  'Time', 'Account', 'Exchange',
                                                  'Side', 'Shares', 'Price',
                                                  'AvPrice', 'cumQty', 'OrderRef'])
        
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
    
    def execDetails(self, reqId, contract, execution):
        super().execDetails(reqId, contract, execution)
        #print("ExecDetails. ReqId:", reqId, "Symbol:", contract.symbol, "SecType:", contract.secType, "Currency:", contract.currency, execution)
        dictionary = {"ReqId":reqId, "PermId":execution.permId, "Symbol":contract.symbol, "SecType":contract.secType, "Currency":contract.currency, 
                      "ExecId":execution.execId, "Time":execution.time, "Account":execution.acctNumber, "Exchange":execution.exchange,
                      "Side":execution.side, "Shares":execution.shares, "Price":execution.price,
                      "AvPrice":execution.avgPrice, "cumQty":execution.cumQty, "OrderRef":execution.orderRef}
        self.execution_df = self.execution_df.append(dictionary, ignore_index=True)
        
def websocket_con():
    app.run()

def pause_until(hour, minute, second, timezone):
    tz = pytz.timezone(timezone)
    while True:
        time_now = datetime.now(tz)
        if time_now.hour == hour and time_now.minute == minute and time_now.second == second:
            break
        
def futuresContract(symbol, exchange, currency, expiry):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "FUT"
    contract.exchange = exchange
    contract.currency = currency
    contract.lastTradeDateOrContractMonth = expiry
    return contract

def marketOrder(action, quantity):
    order = Order()
    order.action = action
    order.orderType = "MKT"
    order.totalQuantity = quantity
    #order.tif = "OPG"
    return order

def nextHSIFexpire():
    tz = pytz.timezone('Hongkong')
    url = 'https://misc.interactivebrokers.com/cstools/contract_info/v3.10/index.php?action=Futures%20Search&entityId=a19207010&lang=en&ib_entity=&wlId=IB&showEntities=Y'
    with req.urlopen(url) as response:
        data = response.read().decode('utf-8')
    root = bs4.BeautifulSoup(data, 'html.parser')
    dates = []
    for contract in root.find_all("td", text="HKFE"):
        dates.append(datetime.strptime(contract.find_next_sibling("td").text, '%d/%m/%Y').date())
    if (dates[0] - datetime.now(tz).date()).days >= 2:
        exp_date = dates[0] 
    else:
        exp_date = dates[1]
    return str(exp_date).replace('-','')

def scrapeMoneyFlow():
    url = 'http://wdata.aastocks.com/datafeed/getquotabalance.ashx?mkt=hk-connect&lang=eng'
    res = requests.get(url)
    try:
        flow = float(res.json()[0].get('qv'))/100000000
    except:
        flow = 0
    return flow

def symbolPos(symbol):
    try:    
        ind = app.pos_df['Symbol'].where(app.pos_df['Symbol']==symbol).last_valid_index()
        return app.pos_df['Position'][ind]
    except:
        return 0

def tradingHour():
    now = datetime.now(pytz.timezone('Hongkong')).time()
    start_time = dt.time(9,15,0)
    pause_start = dt.time(12,0,0)
    pause_end = dt.time(13,0,0)
    end_time = dt.time(16,30,0)
    if (now >= start_time and now <= pause_start and now.minute % 5 == 0 and now.second == 0) or (now >= pause_end and now <= end_time and now.minute % 5 == 0 and now.second == 0):
        return True
    else:
        return False

def get_VHSI():
    url = 'https://www.jpmhkwarrants.com/en_hk/ajax/market-terms-real-time-box/code/VHSI/delay/1?_='
    sess = requests.session()
    req = sess.get(url)
    soup = bs4.BeautifulSoup(req.text, features='lxml')
    return float(soup.find("li", {"class": "no_line"}).text.split()[1])

def get_recent_VHSI(n:int):
    url = 'https://www.jpmhkwarrants.com/en_hk/data/chart/underlyingChart/code/VHSI/period/0/delay/1'
    sess = requests.session()
    req = sess.get(url).json()
    data = pd.DataFrame.from_dict(req['mainData']['underlying']).tail(n)
    return data['open'].mean()

def lineNotifyMessage(token, msg):
      headers = {"Authorization": "Bearer " + token, 
                 "Content-Type" : "application/x-www-form-urlencoded"}
      payload = {'message': msg}
      r = requests.post("https://notify-api.line.me/api/notify", headers = headers, params = payload)
    
tz_hk = pytz.timezone('Hongkong')
expire = nextHSIFexpire()

money_flow = []

app = TradingApp()      
app.connect("XXX.X.X.X", 0000, clientId=1) # change according to GateWay settings
con_thread = threading.Thread(target=websocket_con, daemon=True)
con_thread.start()
time.sleep(1)

order_id = app.nextValidOrderId
time.sleep(1)
app.reqPositions()
time.sleep(1)

buy = 0.0 # optimize parameter
sell = -0.0 # optimize parameter

line_token = 'XXXXXXXXXXXXXXXXXXXXXXX' #insert your line notify token

pause_until(9, 15, 0, 'Hongkong')

trade_signal = False

while True:
    money_flow.append(scrapeMoneyFlow())
    try:
        last_diff = -(money_flow[-1]-money_flow[-2]) if money_flow[-1] != 0 and money_flow[-2] != 0 else 0
    except:
        last_diff = 0
    pos_hsif = symbolPos('MHI')
    pos_time = time.time()
    if datetime.now(tz_hk).time().hour == 9 and datetime.now(tz_hk).time().minute == 35:
        open_ = get_VHSI()
        recent = get_recent_VHSI(10)
        vhsi_diff = open_ - recent
        lineNotifyMessage(line_token, "Today's VHSI Open & strat signal: {} {:.2f}".format(open_, vhsi_diff))
        if vhsi_diff < 0:
            trade_signal = True
    if tradingHour() == True and trade_signal == True:
        if last_diff > buy and pos_hsif == 0: # Buy
            app.placeOrder(order_id,futuresContract("MHI", "HKFE", "HKD", expire),marketOrder("BUY", 1))
            order_id += 1
        elif last_diff > buy and pos_hsif < 0: # Buy
            app.placeOrder(order_id,futuresContract("MHI", "HKFE", "HKD", expire),marketOrder("BUY", abs(pos_hsif)+1))
            order_id += 1
        elif last_diff < sell and pos_hsif == 0: # Sell
            app.placeOrder(order_id,futuresContract("MHI", "HKFE", "HKD", expire),marketOrder("SELL", 1))
            order_id += 1
        elif last_diff < sell and pos_hsif > 0: # Sell
            app.placeOrder(order_id,futuresContract("MHI", "HKFE", "HKD", expire),marketOrder("SELL", abs(pos_hsif)+1))
            order_id += 1
        elif last_diff <= buy and last_diff >= sell and pos_hsif > 0: # Close Long
            app.placeOrder(order_id,futuresContract("MHI", "HKFE", "HKD", expire),marketOrder("SELL", abs(pos_hsif)))
            order_id += 1
        elif last_diff <= buy and last_diff >= sell and pos_hsif < 0: # Close Short
            app.placeOrder(order_id,futuresContract("MHI", "HKFE", "HKD", expire),marketOrder("BUY", abs(pos_hsif)))
            order_id += 1
    elif tradingHour() == False and datetime.now(tz_hk).time() > dt.time(16,31,0) and trade_signal == True:
        app.reqExecutions(21, ExecutionFilter())
        time.sleep(1)
        execution_df = app.execution_df
        execution_df = execution_df[execution_df['ReqId'] == 21]
        execution_df = execution_df.reset_index(drop=True)
        execution_df.to_csv(str(datetime.now().date())+'_north_water_execution.csv', index=False)
        for i in range(execution_df.shape[0]):
            if execution_df.at[i, 'Side'] == 'BOT':
                execution_df.at[i, 'AvPrice'] = -execution_df.at[i, 'AvPrice']
        pnl = execution_df['AvPrice'].sum()*10 - execution_df.shape[0]*12.1
        lineNotifyMessage(line_token, "Today's north water strat real PnL: {}".format(str(pnl)))
        break
    next_time = datetime.now(tz_hk) + timedelta(minutes=5)
    try:
        url = 'http://wdata.aastocks.com/datafeed/getquotabalance.ashx?mkt=hk-connect&lang=eng'
        res = requests.get(url)
    except:
        pass
    pause_until(next_time.hour, next_time.minute, 0, 'Hongkong')
