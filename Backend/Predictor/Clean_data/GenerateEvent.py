# --------- Config -----------
Ncores     = 1
input_path = '../Data' 


import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import datetime
import multiprocessing as mp
import pickle
import time
import copy
import os

pd.set_option('display.max_rows', 300 )

# save and load file
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
def toSide( l ):
    l = isBid( l )
    if l == True:
        return 'S'
    if l == False:
        return 'B'
    return 'A'

def is_unsort( l ):
    for i in range(1,len(l)):
        if l[i] < l[i-1]:
            return i
    return False

def generate_side( a , Event ):
    if np.isnan(a):
        return "A"
    Side = Event[Event.SeqNumber == a].iloc[-1].IsBid
    if Side == 'T':
        return 'S'
    if Side == 'F':
        return 'B'
    return np.nan
# ---------------------------------- save file until loadable ---------------------------------
def save_file_pickle( data , path , force = False ):
    try:
        if force:
            a=b # exact error
        load( path )
    except:
        save( data , path )
        save_file_pickle( data , path )
def save_file_csv( data , path , force = False ):
    try:
        if force:
            a=b # exact error
        pd.read_csv( path )
    except:
        data.to_csv( path )
        save_file_csv( data , path )
    
# --------------------------------- check if file loadable ------------------------------------
def test_load_pickle( path ):
    try:
        data = load(path)
    except:
        return False
    del data
    return True
def test_load_csv( path ):
    try:
        data = pd.read_csv( path )
    except:
        return False
    del data
    return True
def test_loads_pickle( paths ):
    for path in paths:
        if test_load_pickle( path ) == False:
            return False
    return True
def test_loads_csv( paths ):
    for path in paths:
        if test_load_csv( path ) == False:
            return False
    return True

def gen( date , s , n ):
    Book = load(f'{input_path}/{date}/pickle/{s}_{n}_book.dat').set_index('Timestamp')
    Auction = load(f'{input_path}/{date}/pickle/{s}_{n}_auction.dat')
    Trade = load(f'{input_path}/{date}/pickle/{s}_{n}_trade.dat')
    
    Book['SeqNumber'] = Book['SeqNumber'].apply( lambda x : int( x ) )
    Trade['SeqNumber'] = Trade['SeqNumber'].apply( lambda x : int( x ) * 10 )
    Trade2 = Trade.copy()
    Trade = Trade[Trade.EventType2 == 'T_TO_T']

    list_date = os.listdir(f'{input_path}')[1:]
    #t = Trade.groupby(['Timestamp','Price']).count().reset_index().groupby(['Timestamp']).count().sort_values(['Price']).iloc[-2].name
    #if len( Auction[Auction.IsFinal == 'T'] ) == 3:
    #    raise Exception("Auction is already correct")
    LastAuction = Auction[Auction.IsFinal == 'T'].groupby(['Timestamp']).first()
    AuctionTime = list( LastAuction.index )
    if len( AuctionTime ) == 0:
        raise Exception(f'No data')
    if len( AuctionTime ) != 3: # Need to detect when market start 
        raise Exception(f'Auction Time incorrected')
    #print( "Fixing : " , date , s , n )
    BookMorning = Book[ ( Book.index >= AuctionTime[0] ) & ( Book.index.time < datetime.time( 13 , 0 ) )]
    BookMornData = np.array( BookMorning )

    BookAfter = Book[ ( Book.index >= AuctionTime[1] ) & ( Book.index.time < datetime.time( 16 , 31 ) ) ]
    BookAfterData = np.array( BookAfter )

    Result = { "Time" : [] , "Price" : [] , "IsBid" : [] , "Change" : [] ,"Flag1" : [] , "Mark" : [] , "SeqNumber" : []  }
    Data   = { }

    # ------------------ Generate Change of orderbook over time -----------------------
    # Mark = 1 when current Timestamp < last Timestamp otherwise Markk = 0
    # Mark = 1 happen in case new level appear from hidden level
    # example BestBid change from 50.25 --> 50
    # price 49 ( Bid ) will appear with mark 1 ( because it already has order but since previous time it not in 5 Bid level )
    # price 50.25 ( Offer ) will appear with mark 0 ( Previous it hasn't any order in this level )

    Mark       = 0
    LastTime   = datetime.datetime( 1990 , 1 , 1 , 1 , 1 , 1 )
    for i in range( len( BookMornData ) ):
        Time  = BookMorning.index[i]
        Type  = BookMornData[i][4]
        Price = BookMornData[i][5]
        Volume = BookMornData[i][6]
        Flag   = BookMornData[i][7]
        Seq    = BookMornData[i][2]
        if Time < LastTime:
            Time = LastTime
            Mark = 1
        if Flag != 'CANCLE':    
            if ( Type , Price ) not in Data:
                Data[ ( Type , Price ) ] = 0
            Change = Volume - Data[ ( Type , Price ) ]
            if Change != 0 :
                Result["Time"] += [ Time ]
                Result["Price"] += [ Price ]
                Result["IsBid"] += [ Type ]
                Result["Change"] += [ Change ]
                Result["Flag1"] += [ Flag ]
                Result['Mark'] += [ Mark ]
                Result["SeqNumber"] += [ Seq ]
            Data[ ( Type , Price ) ] = Volume
        else:
            Result["Time"] += [ Time ]
            Result["Price"] += [ Price ]
            Result["IsBid"] += [ Type ]
            Result["Change"] += [ -Volume ]
            Result["Flag1"] += [ Flag ]
            Result['Mark'] += [ Mark ]
            Result["SeqNumber"] += [ Seq ]
            Data[ ( Type , Price ) ] = 0
            
        LastTime = Time
        Mark     = 0

    Mark       = 0
    for i in range( len( BookAfterData ) ):
        Time  = BookAfter.index[i]
        Type  = BookAfterData[i][4]
        Price = BookAfterData[i][5]
        Volume = BookAfterData[i][6]
        Flag   = BookAfterData[i][7]
        Seq    = BookAfterData[i][2]

        if Time < LastTime:
            Time = LastTime
            Mark = 1
        if Flag != 'CANCLE':    
            if ( Type , Price ) not in Data:
                Data[ ( Type , Price ) ] = 0
            Change = Volume - Data[ ( Type , Price ) ]
            if Change != 0 :
                Result["Time"] += [ Time ]
                Result["Price"] += [ Price ]
                Result["IsBid"] += [ Type ]
                Result["Change"] += [ Change ]
                Result["Flag1"] += [ Flag ]
                Result['Mark'] += [ Mark ]
                Result["SeqNumber"] += [ Seq ]
            Data[ ( Type , Price ) ] = Volume
        else:
            Result["Time"] += [ Time ]
            Result["Price"] += [ Price ]
            Result["IsBid"] += [ Type ]
            Result["Change"] += [ -Volume ]
            Result["Flag1"] += [ Flag ]
            Result['Mark'] += [ Mark ]
            Result["SeqNumber"] += [ Seq ]
            Data[ ( Type , Price ) ] = 0
        LastTime = Time
        Mark     = 0

    Result = pd.DataFrame( Result )
    

    #---------------------------- Indicate relation between Change event and Trade history --------------------

    # indicate what trade order relation with book order
    # Example [ 1 , 10 , 25 ] means Trade number 1 to 10 
    Linker = [ ] # [ StartTrade , EndTrade , BookNumber ] 
    
    # Use in case where Trade volume not equal to Orderbook Change volume , it will create virtual order to make orderbook and trade sync
    # Example there are Sell 300 stocks at price 50thb with seqnumber = 20
    # then change event occur in orderbook at price 50thb with -200 stocks with seqnumber = 25
    # Then it will create [ 50 , 300 , -200 , 20 ] 
    # mean at Price 50 There are Trade start at 20 with 300 stocks but in orderbook in decrease only 200 stock

    Adder  = [ ] # [ Price , TradeVolume , ChangeVolume , Start ]
    TradeTemp = Trade
    BookTemp  = Result[Result.Mark == 0]

    merged    = BookTemp.append( TradeTemp ).sort_values(['SeqNumber'])
    

    Link      = { }
    Trader    = { }
    for itr in range( len( merged ) ) :
        if merged.iloc[itr].Mark >= 0: # Found in book
            Price  = merged.iloc[itr].Price
            Change = merged.iloc[itr].Change
            Type   = merged.iloc[itr].Flag1
            SeqNumber = merged.iloc[itr].SeqNumber
            if Price in Trader and Trader[Price] > 0 and Type != 'INSERT' :
                if Change * -1 != Trader[Price]:
                    Adder.append( [ Price , Trader[Price] , Change , ( merged.iloc[itr].IsBid == 'T' ) * 2 -1 ,  Link[Price][0]   ] )
                    #print( merged.Time.iloc[itr]  , Change , Trader )
                Trader[Price] = 0
                Link[Price][2] = SeqNumber
                Linker.append( Link[Price] )
                
                #print("Book : " , Link[Price] )
                del Link[Price]

        else:                          # Found in Trader
            Price = merged.iloc[itr].Price
            Volume = merged.iloc[itr].Volume
            SeqNumber = merged.iloc[itr].SeqNumber
            if Price not in Trader:
                Trader[Price] = 0
            if Price not in Link:
                Link[Price] = [ SeqNumber , None , None ]
            Link[Price][1] = SeqNumber
            Trader[Price] += Volume
            #print("Trade : " , SeqNumber)
    for k in Trader:
        if Trader[k] > 0:
            raise Exception(f"{k} Trade Nokoru")

    Adder = np.array( Adder )
    Linker = np.array( Linker )
    if len( Linker ) > 0:
        ind= np.argsort( Linker[:,-2])
        Linker = Linker[ind]
    # Adding new columns to trade 
    # OrderbookRef  = Event in orderbook correspond to that trade
    # OrderbookRef2 = self generate event that correspond to that trade
    OrderbookRef  = [] 
    OrderbookRef2 = []
    SQ = np.array( Trade2.SeqNumber )
    TY = np.array( Trade2.EventType2 )
    Index  = 0 
    Index2 = 0
    
    while Index < len( Trade2 ):
        if TY[Index] == 'T_TO_T':
            #print( SQ[Index] )
            while not ( SQ[Index] >= Linker[Index2][0] and SQ[Index] <= Linker[Index2][1] ) :
                Index2 += 1
                #print( "Linker Change to " , Linker[Index2] )
            if len(Adder) > 0 and Linker[Index2][0] in Adder[:,-1]:
                OrderbookRef2 += [ Linker[Index2][0] ]
            else:
                OrderbookRef2 += [ np.nan ]
            OrderbookRef += [ Linker[Index2][2] ]
        else:
            OrderbookRef  += [ np.nan ]
            OrderbookRef2 += [ np.nan ]
        Index += 1
    Trade2['OrderbookRef'] = OrderbookRef
    Trade2['OrderbookRef2'] = OrderbookRef2

    Event = { "Timestamp" : [] , "Price" : [] , "IsBid" : [] , "Change" : [] , "Flag1" : [] , "Flag2" : [] , "Mark" : [] , "SeqNumber" : [] }
    ResultData = np.array( Result )
    #----------------------------------- Merge adder and linker to real event data  --------------------------
    Last       = 0
    AdderIndex = 0
    for elem in ResultData:
        Time   = elem[0]
        Price  = elem[1]
        IsBid  = elem[2]
        Change = elem[3]
        Flag1  = elem[4]
        Mark   = elem[5]
        SeqNumber = elem[6]
        while AdderIndex < len(Adder) and Adder[AdderIndex][4] > Last and Adder[AdderIndex][4] < SeqNumber:
            aTime  = Trade[Trade.SeqNumber == Adder[AdderIndex][4]].iloc[-1].Timestamp
            aPrice = Adder[AdderIndex][0]
            aIsBid = Adder[AdderIndex][3]
            if aIsBid == 1:
                aIsBid = True
            elif aIsBid == -1:
                aIsBid = False
            else:
                aIsBid = np.nan
            aChange = Adder[AdderIndex][1] + Adder[AdderIndex][2]
            aFlag1  = "UPDATE"
            aMark   = 2
            aSeqNumber = Adder[AdderIndex][4]
            Event["Timestamp"] += [ aTime ]
            Event["Price"]     += [ aPrice ]
            Event["IsBid"]     += [ aIsBid ]
            Event["Change"]     += [ aChange ]
            Event["Flag1"]     += [ aFlag1 ]
            Event["Flag2"]     += [ 'MO' ]
            Event["Mark"]     += [ aMark ]
            Event["SeqNumber"]     += [ aSeqNumber ]
            AdderIndex += 1

        Event["Timestamp"] += [ Time ]
        Event["Price"]     += [ Price ]
        Event["IsBid"]     += [ IsBid ]
        Event["Change"]     += [ Change ]
        Event["Flag1"]     += [ Flag1 ]
        if SeqNumber in OrderbookRef:
            Event["Flag2"]     += [ 'MO' ]
        else:
            Event["Flag2"]     += [ 'LO' ]
        Event["Mark"]     += [ Mark ]
        Event["SeqNumber"]     += [ SeqNumber ]
        Last = SeqNumber

    Event = pd.DataFrame( Event ).set_index('Timestamp')

    Sides = []
    
    for elem in Trade2.OrderbookRef:
        Sides += [ generate_side( elem , Event ) ]
    Trade2['Side'] = Sides
    
    return Event , Trade2

# Fix bug in function gen
# in gen() there are bug in linker that happen when there are trade many price in same time
def generate_new_trade( d , s , n ):
    Event  = load(f'{input_path}/{d}/pickle/{s}_{n}_event.dat')
    Trade  = load(f'{input_path}/{d}/pickle/{s}_{n}_trade2.dat')

    save_file_pickle( Trade , f'{input_path}/cache/{d}/{s}_{n}_trade2.bck')
    save_file_pickle( Event , f'{input_path}/cache/{d}/{s}_{n}_event.bck')
    
    Trade = Trade.drop( columns = ['OrderbookRef','OrderbookRef2','Side'] ) # drop relation to orderbook
    Trade2 = Trade.copy()

    Event.SeqNumber = Event.SeqNumber - ( Event.Mark == 2 ) * 0.5
    Trade = Trade[Trade.EventType2 == 'T_TO_T']
    Merged = Trade.append( Event.reset_index() ).sort_values(['SeqNumber'])
    
    
    MergedData = np.array( Merged )

    # In case of there are visual order ( to make trade and orderbook sync together .if they not sync , It will hard to create simulation )
    # Timeline   visual update price order --> Trade#1 --> Trade#n --> real update price order

    # In case of normal update ( Trade stock equal to decreased stocks in orderbook )
    # Timeline   Trade#1 --> Trade#n --> real update price order

    # Use to create TradeMap
    PriceData = { } # { 20 : [1,2,3,4] } mean at 20 price there are trade seqnumber = 1,2,3,4 and will reset when found change in orderbook with price 20
    TradeMap  = { } # { 12 : ( 34 , True ) } mean Trade 12 correspond to event update number 34 with Bid side 
    # Use to create Ref2Map
    Ref2Data  = { } # { 20 : 39 } there are current visual order number 39 at price 20 , it's will reset when found change in orderbook with price 20
    Ref2Map   = { } # { 12 : 10 } mean Trade order 12 correspond to visual order update number 10

    MO_List   = [ ] # List of event update number that correspond to trade
    for elem in MergedData:
        isTrade = np.isnan( elem[-1] )
        Time    = elem[0]

        if not isTrade:
            EventType = elem[-3]
            Price     = elem[9]
            Mark      = elem[-1]
            Change    = elem[-4]
            SeqNumber = elem[2]
            IsBid     = elem[-5]
            if Mark == 2:
                Ref2Data[Price] = SeqNumber
                MO_List        += [ SeqNumber ]
            elif EventType != 'INSERT':
                if Price in PriceData:
                    for seq in PriceData[Price]:
                        TradeMap[seq] = ( SeqNumber , IsBid )
                    del PriceData[Price]
                    if Price in Ref2Data:
                        del Ref2Data[Price]
                    MO_List        += [ SeqNumber ]
        else:
            SeqNumber = elem[2]
            Price     = elem[9]
            Volume    = elem[8]

            if Price not in PriceData:
                PriceData[Price] = [ ]

            PriceData[Price].append( SeqNumber )

            if Price in Ref2Data:
                Ref2Map[ SeqNumber ] = Ref2Data[ Price ] 

    # Reconstuct TradeMap and Ref2Map to dataframe
    Ref  = [ ] 
    Ref2 = [ ]
    Side = [ ]
    Flag2 = [ ]
    TradeData = np.array( Trade2 )
    for elem in TradeData: # Filter only trade data
        SeqNumber = elem[2]

        if SeqNumber in TradeMap: 
            Ref.append( TradeMap[SeqNumber][0] ) 
            Side.append( TradeMap[SeqNumber][1] )
        else:
            Ref.append( np.nan )
            Side.append( np.nan )

        if SeqNumber in Ref2Map:
            Ref2.append( Ref2Map[SeqNumber] )
        else:
            Ref2.append( np.nan )
    
    for seq in Event.SeqNumber:
        if seq in MO_List:
            Flag2 += [ 'MO' ]
        else:
            Flag2 += [ 'LO' ]
    Event['Flag2'] = Flag2
    Trade2[ 'OrderbookRef' ] = Ref
    Trade2[ 'OrderbookRef2' ] = Ref2
    Trade2[ 'Side' ] = Side

    Trade2[ 'Side' ] = Trade2[ 'Side' ].apply( toSide )
    
    save_file_pickle( Trade2 , f'{input_path}/{d}/pickle/{s}_{n}_trade2.dat' , force = True  )
    save_file_csv( Trade2 , f'{input_path}/{d}/pickle/{s}_{n}_trade2.csv' , force = True  )
    
    save_file_pickle( Event , f'{input_path}/{d}/pickle/{s}_{n}_event.dat' , force = True  )
    save_file_csv( Event , f'{input_path}/{d}/pickle/{s}_{n}_event.csv' , force = True  )

# ----------------- Loop in each symbol -------------------------
def generate_Trade_Event( date ):
    list_symbol = os.listdir(f'{input_path}/{date}/pickle')

    # --------------- find orderbook and extract symbol -----------
    symbols = []
    for elem in list_symbol:
        filename = elem.split('.')[0]
        data     = filename.split('_')
        if len(data) >= 3 and data[-1] == 'book':
            Name = '_'.join( data[:-2] )
            ID   = data[-2]
            symbols.append( (Name,ID) )
    print( date )
    time.sleep( np.random.rand() )
    for symbol in symbols:
        s = symbol[0]
        n = symbol[1]
        pickle_files = []
        pickle_files.append( f'{input_path}/{date}/pickle/{s}_{n}_event.dat' )
        pickle_files.append( f'{input_path}/{date}/pickle/{s}_{n}_trade2.dat' )
        csv_files    = []
        csv_files.append( f'{input_path}/{date}/pickle/{s}_{n}_event.csv' )
        csv_files.append( f'{input_path}/{date}/pickle/{s}_{n}_trade2.csv' )
        
        if True: 
        #if test_loads_pickle( pickle_files ) == False or test_loads_csv( csv_files ) == False: # uncomment if want to skip if file is detected
            try:
                #print( f"{date} {s} {n}   " )
                Event , Trade  = gen( date , s , n )
    
                save_file_pickle( Event   , f'{input_path}/{date}/pickle/{s}_{n}_event.dat' , force = True )
                save_file_pickle( Trade , f'{input_path}/{date}/pickle/{s}_{n}_trade2.dat' , force = True  )

                save_file_csv( Event   , f'{input_path}/{date}/csv/{s}_{n}_event.csv' , force = True )
                save_file_csv( Trade , f'{input_path}/{date}/csv/{s}_{n}_trade2.csv' , force = True  )
                
                generate_new_trade( date , s , n )
                
            except Exception as e:
                if str(e) not in ['Auction is already correct','No data']:
                    print( f"{date} {s} {n} " , e )
        
list_date = os.listdir(f'{input_path}')[1:]
pool = mp.Pool( Ncores )
pool.map( generate_Trade_Event , list_date ) 
