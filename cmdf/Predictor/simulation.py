import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import datetime
import matplotlib.dates as mdates
import multiprocessing as mp
import pickle
from sklearn.metrics import mean_squared_error
from sklearn.metrics import r2_score

def save(data , fn):
    with open(fn, 'wb') as f:
        pickle.dump(data, f)
def load(fn): 
    with open(fn, 'rb') as f:
        data = pickle.load(f)
    return data
def isBid( l ):
    if l == 'F':
        return False
    if l == 'T':
        return True
    return l
def generate_side( a , Event ):
    if np.isnan(a):
        return "A"
    Side = Event[Event.SeqNumber == a].iloc[-1].IsBid
    if Side == 'T':
        return 'S'
    if Side == 'F':
        return 'B'
    return np.nan

class Queue:
    def __init__( self ):
        self.queue = [ ]
        self.isLock = False
        self.isPseudoLock = False
    def add( self , value , ID ):
        if self.isLock:
            raise Exception("Try to edit locked queue")
        self.queue.append( [ ID , value ] )
    def remove_last( self , N ):
        if self.isLock:
            raise Exception("Try to edit locked queue")
        RemainValue = N
        Index = len( self.queue ) - 1
        while Index >= 0 and RemainValue > 0 :
            if self.queue[Index][0] > 0: # If other paticipant ID
                if self.queue[Index][1] > RemainValue:
                    self.queue[Index][1] -= RemainValue
                    RemainValue = 0
                    break
                else:
                    RemainValue -= self.queue[Index][1]
                    del self.queue[Index]
                    Index -= 1
            else:
                Index -= 1
        if RemainValue != 0:
            raise Exception('Try to remove empty queue.')
        if self.get_sum == 0:
            self.queue = [ ]
    def remove_first( self , N , reverse = False ):
        if self.isLock:
            raise Exception("Try to edit locked queue")
        reverse     = ( reverse == False ) * 2 - 1
        RemainValue   = N
        MyRemainValue = N
        Index       = 0
        while Index < len( self.queue ) and RemainValue > 0:
            if self.queue[Index][0] * reverse  > 0  : # If other paticipant ID
                if self.queue[Index][1] > RemainValue:
                    self.queue[Index][1] -= RemainValue
                    RemainValue = 0
                    break
                else:
                    RemainValue -= self.queue[Index][1]
                    del self.queue[Index]
            else:
                MyRemainValue = min( [ MyRemainValue , RemainValue ] )
                if self.queue[Index][1] > MyRemainValue:
                    self.queue[Index][1] -= MyRemainValue
                    MyRemainValue         = 0
                    Index                += 1
                else:
                    MyRemainValue        -= self.queue[Index][1]
                    del self.queue[Index]
        if RemainValue != 0:
            raise Exception('Try to remove empty queue.')
        if self.get_sum == 0:
            self.queue = [ ]
    def search_id( self , ID ):
        ID_list = list( map( lambda x : x[0] , self.queue ) )
        if ID in ID_list:
            return self.queue [ ID_list.index( ID ) ]
        else:
            return False
    def delete_id( self , ID ):
        ID_list = list( map( lambda x : x[0] , self.queue ) )
        if ID in ID_list:
            del self.queue [ ID_list.index( ID ) ]
            return True
        else:
            return False
    def lock( self , pseudo = False ):
        if pseudo:
            self.isPseudoLock = True
        else:
            self.isLock = True
    def unlock( self , pseudo = False ):
        if pseudo:
            self.isPseudoLock = False
        else:
            self.isLock = False
    def get_sum( self , alien = False ):
        s = 0
        for elem in self.queue:
            if elem[0] > 0:
                s += elem[1]
            elif elem[0] < 0 and alien == True:
                s += elem[1]
        return s
    def organize( self ):
        if self.get_sum() == 0:
            self.queue == [ ]
        return True
    
class Simulation:
    def __init__( self , Event , Trade , Auction , morn_start_time = ( 10 , 5 ) , noon_start_time = ( 14 , 35 ) ,
                  morn_end_time = ( 12 , 29 ) , noon_end_time = ( 16 , 29 )  , step = 60 ):
        self.Event     = Event.copy()
        self.Trade2    = Trade[Trade.EventType2 != 'TRADE_REPORT'].copy()
        self.Trade     = Trade[Trade.EventType2 == 'T_TO_T'].copy()
        self.auction   = Trade[Trade.EventType2 == 'AUCTION'].copy()
        self.Auction   = Auction.copy()
        LastAuction = Auction[Auction.IsFinal == 'T'].groupby(['Timestamp']).first()
        self.AuctionTime = list( LastAuction.index )
        self.Event['IsBid'] = self.Event['IsBid'].apply( isBid )
        self.RealEvent = self.Event.reset_index().append( self.Trade ).sort_values("SeqNumber")
        self.EventData = np.array( self.RealEvent ) 
        self.queue  = { }
        self.locker = { }
        self.Temporary = { }
        self.LastTime  = self.EventData[0][0]
        self.id        = -1
        self.Mapper    = { }
        self.tracker   = tracker( self.queue , self.Mapper )
        d = self.Event.index[0]

        self.morn_start_time   = datetime.datetime( d.year , d.month , d.day , morn_start_time[0] , morn_start_time[1] )
        self.morn_end_time     = datetime.datetime( d.year , d.month , d.day , morn_end_time[0] , morn_end_time[1] )
        self.noon_start_time   = datetime.datetime( d.year , d.month , d.day , noon_start_time[0] , noon_start_time[1] )
        self.noon_end_time    = datetime.datetime( d.year , d.month , d.day , noon_end_time[0] , noon_end_time[1] )
        
        self.Current          = datetime.datetime( d.year , d.month , d.day , 9 , 50 )
        self.step             = datetime.timedelta( seconds = step )
        self.Iterator         = 0
        self.LastTime         = self.Current
        
        self.isTransit        = False
        
        self.feed_until( self.morn_start_time )
       
        self.Current          = self.morn_start_time
        
        
        
    def forward( self ):
        self.LastTime = self.Current
        self.Current += self.step
        Timelimit = self.morn_end_time
        if self.Current.hour == 12 and self.Current > Timelimit:
            self.Current = self.noon_start_time
        self.tracker.update()
    def do( self ):
        self.update( )
        self.feed_until( self.Current )
        self.update( )
    def isClosed( self ):
        Timelimit = self.noon_end_time
        if self.Current >= Timelimit  :
            return True
        return False
    def feed_until( self , time ):
        while self.Iterator < len( self.EventData ) and self.EventData[ self.Iterator ][0] < time:
            self.feed( self.EventData[ self.Iterator ] )
            self.Iterator += 1
    def __feed( self , msg ):
        elem    = msg
        isTrade = np.isnan( elem[2] )
        Time    = elem[0]
        if not isTrade:
            Change  = elem[3]
            Price   = elem[1]
            IsBid   = elem[2]
            Flag2   = elem[5]
            Mark    = elem[6]
            key     = ( Price , IsBid )
            
            if Flag2 == 'LO' or Mark == 2:
                if elem[4] == 'INSERT':
                    if key not in self.queue:
                        self.queue[key]  = Queue()
                    if self.queue[key].isLock == False: # If not lock , Just update as it is
                        if Change > 0:
                            self.queue[key].add( Change , 1 )
                        else:
                            self.queue[key].remove_last( -Change )
                    else:                               # If lock , then must to calculate Real Change 
                        Before = self.queue[key].get_sum()
                        Change = Change - Before
                        self.queue[key].unlock()
                        if Change > 0:
                            self.queue[key].add( Change , 1 )
                        else:
                            self.queue[key].remove_last( -Change )
                elif elem[4] == 'CANCLE':               \
                    self.queue[key].lock()
                elif elem[4] == 'UPDATE':               # Update as it is
                    if key not in self.queue and Mark == 2:
                        self.queue[key]  = Queue()
                    if Change > 0:
                        self.queue[key].add( Change , 1 )
                    else:
                        self.queue[key].remove_last( -Change )
                self.queue[key].unlock( pseudo = True )
            if Flag2 == 'MO' and elem[4] == 'CANCLE':
                self.queue[key].lock() 
        else:
            Price = elem[1]
            Volume = elem[14]
            Side   = elem[-1]
            
            if Side == 'B':
                key     = ( Price , False )
            elif Side == 'S':
                key     = ( Price , True )
            else:
                raise Exception("Unexpected Side")
            if key not in self.queue :
                raise Exception("Level does not valid.")
            self.queue[key].remove_first( Volume )
        self.queue[key].organize()
    def __transit( self ):
        if self.isTransit == True:
            return
        for key in self.queue:
            if self.queue[key].get_sum() > 0:
                self.queue[key].lock( pseudo = True )
        self.isTransit = True
    def feed( self , msg ):
        if self.LastTime.hour <= 13 and msg[0].hour >= 13:
            self.__transit( )
        try:
            if len( self.Temporary ) > 0 and msg[0] > self.TemporaryTime: 
                try:
                    for key in self.Temporary:
                        for elem in self.Temporary[key]:
                            self.__feed( elem )
                        print(f"Handle successful {key}")
                    self.Temporary = { }
                except Exception as e:
                    raise Exception(f"Handle not successful. --> {e}")
                    
            self.__feed( msg )
        except Exception as e:
            if str( e )[:6] == 'Handle':
                raise Exception(f"{e}")
            if ( msg[0] , msg[1] )  not in self.Temporary:
                self.Temporary[ (msg[0],msg[1]) ] = [ ]
                print( f"Handle unsync data on {msg[0]} with price { msg[1] } cause {e}" )
            self.TemporaryTime = msg[0]
            self.Temporary[ (msg[0],msg[1]) ] .append( msg )
    def update( self ):
        self.BestBid = -1
        self.BestOffer = np.inf
        self.Ceiling = False
        self.Floor   = False
        for key in self.queue:
            if self.queue[key].isLock == False and self.queue[key].isPseudoLock == False :
                if key[1] == True: # Bid
                    if key[0] > self.BestBid:
                        self.BestBid = key[0]
                else:              # Offer
                    if key[0] < self.BestOffer:
                        self.BestOffer = key[0]
        if self.BestBid   == -1:
            self.Floor   = True
        if self.BestOffer == np.inf:
            self.Ceiling = True
    def addLO( self , Price , amount  ):
        
        self.update()
        
        if Price == 'BestBid':
            Price = self.BestBid
        elif Price == 'BestOffer':
            Price = self.BestOffer
        
        if Price <= self.BestBid:
            Side  = True
            sSide = 'Bid'
        elif Price >= self.BestOffer:
            Side  = False
            sSide = 'Offer'
        else:
            raise Exception(f"{Price} not avialable due ceiling ( { self.BestOffer == np.inf } ) or floor ( { self.BestBid == -1 } )")
        
        key = ( Price , Side )
        
        if Price == np.inf:
            raise Exception("Cannot place LO ( offer ) at ceiling")
        if Price == -1:
            raise Exception("Cannot place LO ( bid ) at floor")
        
        if key not in self.queue:
            raise Exception(f"No queue with price = {key[0]} isBid = {key[1]} ")
        
        if Side == True and self.Floor:
            raise Exception(f"Try to place BID when at floor")
        elif Side == False and self.Ceiling:
            raise Exception(f"Try to place OFFER when at ceiling")
        
        if self.queue[key].isLock == False and self.queue[key].isPseudoLock == False:
            self.queue[key].add( amount , self.id )
            self.Mapper[self.id] = key
            self.tracker.add( self.id , self.Current , 'LO' , sSide , key[0] , amount , amount )
            self.id -= 1
        else:
            raise Exception("Try to insert locked queue")
            
    def addMO( self , amount , Side ):
        self.update()
        if Side == 'Bid':
            if self.Ceiling:
                raise Exception(f"Try to place MO ( buy ) when at ceiling")
            Price = self.BestBid
        elif Side == 'Offer':
            if self.Floor:
                raise Exception(f"Try to place MO ( sell ) when at ceiling")
            Price = self.BestOffer
        else:
            raise Exception("Unexpected Side")
            
        self.tracker.add( 0 , self.Current , 'MO' , Side , Price , amount , 0 , status = False )
    def cancleLO( self , ID ):
        self.tracker.update() 
        self.queue[ self.Mapper[ID] ].delete_id( ID )
        self.tracker.lock( ID )
    def myVWAP(self):
        history = self.tracker.get()
        vol     = history.InitValue - history.CurrentValue
        VWAP    = np.sum( vol * history.Level ) / np.sum( vol )
        return VWAP
    def marketVWAP( self , ATOATC = True ): 
        if not ATOATC:
            return ( self.Trade.Volume*self.Trade.Price ).sum() / self.Trade.Volume.sum()
        else:
            return ( self.Trade2.Volume*self.Trade2.Price ).sum() / self.Trade2.Volume.sum()
    
class tracker():
    def __init__(self , queue , Mapper ):
        self.data  = { "ID" : [] , "Time" : [] , "Type" : [] , "Side" : [] , "Level" : [] , "InitValue" : [] , "CurrentValue" : [] , "Status" : [] }
        self.queue = queue
        self.Mapper = Mapper
    def add( self , ID , Time , Type , Side , Level , InitValue , CurrentValue , status = True ):
        self.data["ID"]   .append( ID )
        self.data["Time"] .append( Time ) 
        self.data["Type"] .append( Type )
        self.data["Side"] .append( Side )
        self.data["Level"] .append( Level )
        self.data["InitValue"] .append( InitValue )
        self.data["CurrentValue"] .append( CurrentValue )
        self.data["Status"] .append( status )
    def update( self ):
        for i in range(len(self.data["ID"])):
            if self.data["Status"][i]: 
                ID  = self.data["ID"][i]
                key = self.Mapper[ ID ]
                status = self.queue[key].search_id( ID )
                #print( status )
                if status == False or status[1] <= 0:
                    self.data["CurrentValue"][i] = 0
                    self.data["Status"][i] = False
                else:
                    self.data["CurrentValue"][i] = status[1]
    def lock( self , ID ):
        for i in range(len(self.data["ID"])):
            if self.data["ID"][i] == ID:
                self.data["Status"][i] = False
    def get( self ):
        self.update()
        return pd.DataFrame( self.data ).set_index(["ID"])