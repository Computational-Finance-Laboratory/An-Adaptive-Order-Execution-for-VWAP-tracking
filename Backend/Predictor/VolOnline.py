import pandas as pd
import numpy as np
import datetime
import multiprocessing as mp
import pickle
import time
import os
from sklearn.linear_model import LinearRegression

def save(data , fn):
    with open(fn, 'wb') as f:
        pickle.dump(data, f)
def load(fn): 
    with open(fn, 'rb') as f:
        data = pickle.load(f)
    return data
def groupTrade( newTrade ):
    newTrade            = newTrade.set_index('Timestamp')
    newTrade.index      = newTrade.index + datetime.timedelta( seconds = 60 )
    newTrade['Hours']   = newTrade.index.hour
    newTrade['Minutes'] = newTrade.index.minute
    newTrade            = newTrade.groupby( ['Hours','Minutes'] ).sum()[['Volume']]
    return newTrade

class VolPred:
    def __init__( self , s , n , test_data = 200 ):
        list_date = os.listdir('Predictor/Data')[1:-1]
        list_date = sorted( list_date )
        
        self.debug = [ ]
        Trades = [ ]
        for d in list_date:
            print( d , end = '\r')
            Trade = load(f'Predictor/Data/{d}/pickle/{s}_{n}_trade.dat')
            Trades.append( [ Trade[Trade.EventType2 == 'T_TO_T'].copy() , Trade[Trade.EventType2 == 'AUCTION'] ].copy()  )
            
        test_data = 200
        trainX    = { }
        trainY    = { }
        counter   = 0
        
        for elem in Trades[:-test_data]:
            print( counter , end = '\r')
            group_trade = groupTrade( elem[0] )
            group_trade[ "Cumsum" ] = group_trade.Volume.cumsum()
            group_trade_data        = np.array( group_trade )
            group_trade_index       = np.array( group_trade.index )
            
            self.debug.append( group_trade )
            
            if len( elem[1] ) == 0 or ( elem[1].iloc[0].Timestamp.hour > 11 ):
                ATO = 0
            else:
                ATO = elem[1].iloc[0].Volume

            for i in range( len( group_trade ) ):
                for j in range( i+1 , len( group_trade ) ):
                    start = group_trade_index[i]
                    end   = group_trade_index[j]

                    CurrnetVolume  = group_trade_data[i][1]
                    if i > 0:
                        IntervalVolume = group_trade_data[j][1] - group_trade_data[i-1][1]
                    else:
                        IntervalVolume = group_trade_data[j][1]

                    paramX         = [ CurrnetVolume , ATO ]
                    paramY         = IntervalVolume

                    if start not in trainX:
                        trainX[start] = { }
                        trainY[start] = { }
                    if end not in trainX[start]:
                        trainX[start][end] = [ ]
                        trainY[start][end] = [ ]
                    trainX[start][end].append( paramX )
                    trainY[start][end].append( paramY )
            counter += 1
            
        models = { }
        for key1 in trainX.keys():
            print(f" {key1}    " , end = '\r')
            models[ key1 ] = { }
            for key2 in trainX[key1].keys():
                models[ key1 ][ key2 ] = LinearRegression().fit( trainX[key1][key2] , trainY[key1][key2] )
                
        self.models = models
    def predict( self , start , end , Volume , ATO ):
        return self.models[start][end].predict( [ [ Volume , ATO ] ] )
    def predict_with_data( self , start , end , trade , auction ):
        group_trade = groupTrade( trade )
        ATO         = auction.iloc[0].Volume
        group_trade[ "Cumsum" ] = group_trade.Volume.cumsum()
        
        group_trade_list = list( group_trade.index )
        
        start_index       = group_trade_list.index[ start ]
        end_index         = group_trade_list.index[  end  ]
        
        Volume            = groupTrade.iloc[start_index].Cumsum()
        
        return self.predict( start , end , Volume , ATO ) 

class VolumeOnline:
    def __init__( self , volpred , sim , start = ( 10 , 5 ) , end = ( 16 , 29 ) ):
        self.volpred = volpred
        self.dt      = datetime.timedelta( microseconds = 1 )
        self.sim     = sim
        self.start   = start
        self.end     = end
        
    def getPlan( self , side , Vol = False):
        trade          = self.sim.Trade
        windowed_trade = trade[ ( trade.Timestamp >= self.sim.Current - self.sim.step + self.dt ) &  ( trade.Timestamp <= self.sim.Current ) ]
        
        Minutes        = self.sim.Current.minute
        Hours          = self.sim.Current.hour
        
        VolumesNow    = trade[trade.Timestamp <= self.sim.Current].Volume.sum( )
        ATO           = self.sim.auction.iloc[0].Volume
        
        if ( Hours , Minutes) == self.end:
            VolumeLeft = 0
        else:
            VolumeLeft    = self.volpred.predict ( ( Hours , Minutes) , self.end , VolumesNow , ATO )[0]
        
        Volumes        = windowed_trade.Volume.sum()
        
        transaction = windowed_trade.groupby(['Price']).sum()
        res = { }
        for i in range( len( transaction ) ):
            res[ transaction.index[i] ] = transaction.Volume.iloc[i]
            
        LO = 0
        MO = 0 
        
        Transaction = res
        for key in Transaction:
            if key <= self.sim.BestBid:
                LO += Transaction[key]
            else:
                MO += Transaction[key] 
        if LO + MO != 0:
            MO = MO/( LO + MO )
        else:
            MO = 0

        if side == 1:
            MO = min( [ 0.75 , MO ] )
        else:
            MO = max( [ 0.25 , MO ] )
            
        if VolumeLeft + Volumes == 0:
            returnVol = 0
        else:
            returnVol = min( [ max( [ Volumes / ( VolumeLeft + Volumes ) , 0 ] ) , 1 ] )
        if Vol:
            return returnVol , MO , Volumes 
        return returnVol , MO
