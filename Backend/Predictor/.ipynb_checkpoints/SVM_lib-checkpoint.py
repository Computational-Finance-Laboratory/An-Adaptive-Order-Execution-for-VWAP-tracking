import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from numpy import log
import warnings
warnings.filterwarnings('ignore')
from sklearn.metrics import confusion_matrix
from imblearn.over_sampling import SMOTE
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVC
import datetime
from sklearn.metrics import precision_recall_fscore_support as score
import pickle

def save(data , fn):
    with open(fn, 'wb') as f:
        pickle.dump(data, f)
def load(fn): 
    with open(fn, 'rb') as f:
        data = pickle.load(f)
        f.close()
    return data
def time_statistic( t , Best , Bid , Offer ):
    BestOffer = Best.loc[:t].iloc[-1].BestOffer
    BestBid   = Best.loc[:t].iloc[-1].BestBid
    Offer_Vol = Offer.loc[:t].iloc[-1][BestOffer]
    Bid_Vol   = Bid.loc[:t].iloc[-1][BestBid]
    return BestBid , BestOffer , Bid_Vol , Offer_Vol , BestBid + ( Bid_Vol / ( Bid_Vol + Offer_Vol ) ) * ( BestOffer - BestBid )
dt = datetime.timedelta( microseconds = 1)
dt_bigger = datetime.timedelta( seconds = 0)

def lnreg( x , y ):
    A = np.vstack([x, np.ones(len(x))]).T
    return np.linalg.lstsq(A, y, rcond=None)[0]

def remove_nt( data ):
    d = data.index[0]
    morning = data[datetime.datetime(d.year,d.month,d.day,10,1):datetime.datetime(d.year,d.month,d.day,12,29)]
    afternoon = data[datetime.datetime(d.year,d.month,d.day,14,31):datetime.datetime(d.year,d.month,d.day,16,29)]
    return morning.append(afternoon).copy()

    
def get_data(quote):
    #get data contain feature and price for training SVM
    try:
        #if file already exist just load it up 
        df = pd.read_csv( f"/mnt/nfs/set/nat_project/New_Data_Format/Data/{quote}_PRICE.csv")
        df['Time'] = pd.to_datetime(df['Time'])
        df.set_index('Time',inplace = True)
    except:
        #if there is no PRICE csv we begin to process data and store it to csv
        #load data contain limit orderbook (BID,OFFER)
        Data = load( f'/mnt/nfs/set/.data3/{quote}.dat' )
        df = pd.DataFrame()
        for date in Data.keys():
            print(date,end = '\r')
            #resample data to 5 minute
            df_date = Data[date][2].copy().resample('5T').first()
            BID = Data[date][0].resample('5T').first()
            Offer = Data[date][1].resample('5T').first()
            #create feature
            df_date['BestBidVolume'] = np.nan
            df_date['BestOfferVolume'] = np.nan
            df_date['Prices'] = df_date['BestBid']
            df_date['return'] = np.log(df_date['Prices'].shift(-1) / df_date['Prices'])
            df_date['rolling_std'] = df_date['Prices'].rolling(2).std()
            df_date['local_minimum'] = df_date['Prices'].shift(1).rolling(1).min()
            #df_date['local_maximum'] = df_date['Prices'].shift(1).rolling(2).max()
            df_date['min - Prices'] =  (df_date['local_minimum'] - df_date['Prices']) / df_date['local_minimum']
            
            #find BestBidVolume, BestOfferVolume
            for i in range(len(df_date.index)):
                try:
                    df_date['BestBidVolume'].iloc[i] = BID[df_date['BestBid'].iloc[i]].iloc[i]
                    df_date['BestOfferVolume'].iloc[i] = Offer[df_date['BestOffer'].iloc[i]].iloc[i]
                except:
                    pass
            df_date = df_date.dropna()
            #get rid of market lucnh break time by concat 2 df using between_time
            df_date = pd.concat([df_date.between_time("10:05","12:25"),df_date.between_time("14:35","16:25")])
            
            #concat this current date's df to main df
            if len(df) == 0:
                df = df_date
            else:
                df = pd.concat([df,df_date])
        #save df
        df.to_csv(f"/mnt/nfs/set/nat_project/New_Data_Format/Data/{quote}_PRICE.csv")
        del Data
    # get rid of inf by replace with np.nan to drop later it will mostly be around 16:2x 
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    #get rid of row where BestBid price is greater that BestOffer (something might went wrong in data)
    df = df[df['BestBid'] < df['BestOffer']]
    df = df[df['BestOfferVolume'] >0]
    df = df[df['BestBidVolume'] >0]
    #create midprice (did not use anyway)
    df['FirstPrice'] = (df['BestBid'] + df['BestOffer'])/2
    #create last feature
    df['OrderImbalance(at the touch)'] = (df['BestBidVolume'] - df['BestOfferVolume'])/(df['BestOfferVolume'] + df['BestBidVolume'])
    df = df.dropna()
    return df

def get_VWAP(quote,df):
    #in this function we will find VWAP for last 5 minute which is one of the feature we use
    
    #set df range (it will take alot of time if we start at 2017)
    df = df["2019-11-01":]
    #path for new trade file
    new_path = '/mnt/nfs/set/newData'
    trade = pd.DataFrame()
    VWAP = pd.Series()
    for date in df.index.map(lambda t: t.date()).unique():
        try:
            print(date,end= '\r')
            #get stock's OrderbookID
            orderbook = pd.read_csv(f"{new_path}/{str(date).replace('-','')}/csv/orderbook.csv")
            OrderbookID = orderbook[orderbook['Persistant Name'] == quote]['OrderbookID'].iloc[0]
            #load current date's trade file
            trade_D = pd.read_csv(f"{new_path}/{str(date).replace('-','')}/csv/{quote}_{OrderbookID}_trade.csv")
            #we will use trade in EventType2 == "T_TO_T" only to avoid miscalculation of VWAP
            trade_D = trade_D[trade_D['EventType2'] == "T_TO_T"]
            #create VWAP of last 5 minutes
            trade_D['Values'] = trade_D["Volume"] * trade_D['Price']
            trade_D['Event Time'] = pd.to_datetime(trade_D['Event Time']) 
            trade_D.set_index('Event Time',inplace = True)
            window = datetime.timedelta( minutes = 5 )
            trade_5T = trade_D.groupby(pd.Grouper(freq=window))
            _VWAP = trade_5T['Values'].sum()/trade_5T['Volume'].sum()
            _VWAP = _VWAP.fillna(method = 'ffill')
            _VWAP = _VWAP.shift(1)
            #concat this current date's VWAP to main VWAP
            if (len(VWAP) == 0):
                VWAP = _VWAP
            else:
                VWAP = pd.concat([VWAP,_VWAP])
        except:
            pass
    return VWAP


def get_SVM_df(quote):
    print("load data")
    df = get_data(quote)
    print("load VWAP")
    VWAP = get_VWAP(quote,df)
    VWAP = VWAP.rename("VWAP")
    df = df.join(VWAP,how='left')
    print("train SVM")
    df= df.dropna()
    df = df["2019-11-01":]
    df['Cost']  = ((np.log(df['VWAP'] / df['Prices'])))* 10000 
    #label class
    df['Class'] = np.where((abs(df['return']) >= 0), np.sign(df['return'])*1, (0))
    df_featured = df[['OrderImbalance(at the touch)','min - Prices','rolling_std','Cost','Class']]
    #create result dataframe to store result and actual answer
    df_result = pd.DataFrame(df['Class'])
    df_result['pred'] = 0
    train_window = 500
    #train model using train_window previous datapoints(for example 200 datapoints) to train and predict next future return
    for i in range(train_window,len(df_featured.index)):
        print(quote , df_featured.index[i],end = '\r')
        X_train = df_featured[i-train_window : i][['OrderImbalance(at the touch)','min - Prices','rolling_std','Cost']]
        y_train = df_featured[i-train_window : i]['Class'].astype("int64")
        #oversample data using smote to prevent model from predict class 0 only
        oversample = SMOTE()
        '''try:
            X_train, y_train = oversample.fit_resample(X_train, y_train)
        except:
            pass'''
        clf = make_pipeline(StandardScaler(), SVC(kernel="linear",class_weight='balanced'))
        if len(y_train.unique()) == 1:
            #if all previous datapoints has only one class then predict same class
            df_result['pred'].iloc[i] = y_train.unique()[0]
        else:
            clf.fit(X_train, y_train)
            X_test = df_featured.iloc[i][['OrderImbalance(at the touch)','min - Prices','rolling_std','Cost']]
            df_result['pred'].iloc[i] = clf.predict(np.array(X_test).reshape(1,-1))

    df_result = df_result[train_window:]
    df =df.join(df_result['pred'],how = 'right')
    df.to_csv(f"/mnt/nfs/set/nat_project/New_Data_Format/SVM_result/{quote}_SVM.csv")


def load_SVM_df(quote):
    df = pd.DataFrame()
    try:
        df = pd.read_csv(f"/mnt/nfs/set/nat_project/New_Data_Format/SVM_result/{quote}_SVM.csv")
    except:
        get_SVM_df(quote)
        df = pd.read_csv(f"/mnt/nfs/set/nat_project/New_Data_Format/SVM_result/{quote}_SVM.csv")
    return df