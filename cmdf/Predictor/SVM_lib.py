import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from numpy import log
import warnings
warnings.filterwarnings('ignore')
from imblearn.over_sampling import SMOTE
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVC
import datetime
import pickle

def load(fn): 
    with open(fn, 'rb') as f:
        data = pickle.load(f)
        f.close()
    return data


def remove_nt( data ):
    d = data.index[0]
    morning = data[datetime.datetime(d.year,d.month,d.day,10,1):datetime.datetime(d.year,d.month,d.day,12,29)]
    afternoon = data[datetime.datetime(d.year,d.month,d.day,14,31):datetime.datetime(d.year,d.month,d.day,16,29)]
    return morning.append(afternoon).copy()

    
def get_data(quote):
    #get data contain feature and price for training SVM
    try:
        #if file already exist 
        df = pd.read_csv( f"./SVM_Data/Data/{quote}_PRICE.csv")
        df['Time'] = pd.to_datetime(df['Time'])
        df.set_index('Time',inplace = True)
    except:
        #create data if it doesn't exist
        #load data contain limit orderbook (BID,OFFER)
        Data = load( f"./BidOfferVolume/{quote}.dat" )
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
        df.to_csv(f"./SVM_Data/Data/{quote}_PRICE.csv")
        del Data
    # get rid of inf by replace with np.nan to drop later,it will mostly be around 16:2x 
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    #get rid of row where BestBid price is greater that BestOffer (something might went wrong in that time)
    df = df[df['BestBid'] < df['BestOffer']]
    df = df[df['BestOfferVolume'] >0]
    df = df[df['BestBidVolume'] >0]
    #create midprice (did not use in SVM)
    df['FirstPrice'] = (df['BestBid'] + df['BestOffer'])/2
    #create OrderImbalance
    df['OrderImbalance(at the touch)'] = (df['BestBidVolume'] - df['BestOfferVolume'])/(df['BestOfferVolume'] + df['BestBidVolume'])
    df = df.dropna()
    return df

def get_VWAP(quote,df):
    #in this function we will find VWAP from trade order 5 minutes in the pass which is one of the feature we use
    
    #set df range
    df = df["2019-11-01":]
    trade = pd.DataFrame()
    VWAP = pd.Series()
    for date in df.index.map(lambda t: t.date()).unique():
        try:
            print(date,end= '\r')
            #get stock's OrderbookID
            orderbook = pd.read_csv(f"Data/{str(date).replace('-','')}/pickle/orderbook.dat")
            OrderbookID = orderbook[orderbook['Persistant Name'] == quote]['OrderbookID'].iloc[0]
            #load current date's trade file
            trade_D = pd.read_csv(f"./Data/{str(date).replace('-','')}/pickle/{quote}_{OrderbookID}_trade.dat")
            #we will use trade in EventType2 == "T_TO_T" only to avoid of the book trade
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
    df = df.join(VWAP,how='left')   #join feature
    print("train SVM")
    df= df.dropna()
    df = df["2019-11-01":]          #set range
    df['Cost']  = ((np.log(df['VWAP'] / df['Prices'])))* 10000 
    df['Class'] = np.where((abs(df['return']) >= 0), np.sign(df['return'])*1, (0))      #label class
    df_featured = df[['OrderImbalance(at the touch)','min - Prices','rolling_std','Cost','Class']]  #feature for SVM
    #create result dataframe to store result and actual Class(for benchmark)
    df_result = pd.DataFrame(df['Class'])
    df_result['pred'] = 0
    train_window = 500
    #train model using train_window previous datapoints(for example 500 datapoints) to train and predict next future return
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
            X = df_featured.iloc[i][['OrderImbalance(at the touch)','min - Prices','rolling_std','Cost']]
            df_result['pred'].iloc[i] = clf.predict(np.array(X).reshape(1,-1))

    df_result = df_result[train_window:]
    df =df.join(df_result['pred'],how = 'right')
    df.to_csv(f"./SVM_Data/SVM_result/{quote}_SVM.csv")


def load_SVM_df(quote):
    df = pd.DataFrame()
    try:
        df = pd.read_csv(f"./SVM_Data/SVM_result/{quote}_SVM.csv")
    except:
        get_SVM_df(quote)
        df = pd.read_csv(f"./SVM_Data/SVM_result/{quote}_SVM.csv")
    return df