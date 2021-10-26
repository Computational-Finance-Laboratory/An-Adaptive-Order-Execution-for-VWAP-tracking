#------------------- Config --------------------
Ncores      = 1
input_path  = '/mnt/nfs/set'
output_path = '../Data'

import regex
import multiprocessing as mp
import pandas as pd
import numpy as np
import datetime
import os
import pathlib
import pickle
import time

def read_txt( path ):
    f = open(path, "r")
    text = f.read()
    f.close()
    return text
def StringToTime1( string ): #DD/MM/YY HH:MM:SS.sss
    fmt = '%d/%m/%y %H:%M:%S.%f'
    return datetime.datetime.strptime( string , fmt )
def StringToTime2( string ): #YYYY-MM-DDTHH:MM:SS.sss
    fmt = '%Y-%m-%dT%H:%M:%S.%f'
    return datetime.datetime.strptime( string , fmt )
def StringToTime3( string ): #YYYY-MM-DD
    fmt = '%Y-%m-%d'
    return datetime.datetime.strptime( string , fmt )

# Extract Message to ( Time , ID , parameter )
def Extract( text ):
    Time , ID , context = regex.findall( '\[([^\]\|]+)\][ ]*([0-9]+)=\[(.*)\]$' , text )[0]
    Params = regex.findall('([0-9]+)=([^\|]*)',context)
    return Time , ID , dict( Params )
# Split Text of message to list of each line
def TextToLines( text ):
    lines = text.split('\n')
    i = 0
    while i != len(lines):
        if lines[i] == '':
            del lines[i]
        else:
            i+=1
    return lines
# Save pickle file
def save(data , fn):
    with open(fn, 'wb') as f:
        pickle.dump(data, f)
# load pickle file
def load(fn): 
    with open(fn, 'rb') as f:
        data = pickle.load(f)
        f.close()
    return data

# Use with MBL
# Generate list of stock 
def generate_orderbook_id( MBL ):
    Symbol_map = { }
    Symbol_map_rev = { }
    for elem in MBL:
        if elem[1] == '296' and elem[2]['501'] == 'S' and elem[2]['502'] == 'F': # Normal stock and not odd lot
            Symbol_map[ elem[2]['13'] ] = elem[2]['22']
            Symbol_map_rev[ elem[2]['22'] ] = elem[2]['13']
    return Symbol_map , Symbol_map_rev

#Find if any message have certain value
def search_orderbook_relate( DataMBL , ID ):
    res = []
    for elem in DataMBL:
        for elem2 in elem[2].values():
            if elem2 == ID:
                res.append( elem )
                break
    return res

#Search message by id
def search_id( Data  ,ID ):
    res = []
    for elem in Data:
        if elem[1] == ID:
            res += [ elem ]
    return res

#get list of all id in message
def unique_id( Data ):
    res = { }
    for elem in Data:
        if elem[1] not in res:
            res[elem[1]] = 0
        res[elem[1]] += 1
    return res

def generateDataframe_296( Data ):
    Data296 = search_id(Data , '296') # Filter only  message 296
    Index = {'6' : 'Timestamp' , '8' : 'Name' , '13' : 'Persistant Name' , '16' : 'Current Name' , '17' : 'Market' , 
             '18' : 'MarketList' , '19' : 'Segment' , '20' : 'Close price' , '22' : 'OrderbookID' , '31' : 'ValidDate' , 
             '501' : 'Type' , '502' : 'isOdd' , '503' : 'CeilingPrice' , '504' : 'FloorPrice'}
    NeedMul = ["Close price","CeilingPrice","FloorPrice"] # index mapping for message id = 296
    Result = { "Event Time" : [] , "Id" : [] }  # Data to construct dataframe
    for i in Index:
        Result[Index[i]] = []
    
    for elem in Data296:  # Run throught every 296 message
        if elem[2]['501'] == 'S' and elem[2]['502'] == 'F':
            Result['Event Time'].append( StringToTime1( elem[0] ) )
            Result['Id'].append( elem[1] )
            for index in Index: # Run throught every index
                if index in elem[2]:
                    val_to_add = elem[2][index] 
                    if Index[index] in NeedMul:
                        val_to_add = int( val_to_add ) / 1000000
                    Result[Index[index]].append( val_to_add )
                else:
                    Result[Index[index]].append( np.NaN )
    Result = pd.DataFrame(Result)
    Result['Timestamp'] = Result['Timestamp'].apply( StringToTime2 )
    return Result

def generateDataframe_62( Data ):
    Data62 = search_id(Data , '62') # Filter only message 62
    Index  = {'2' : 'SeqNumber' , '3' : 'Timestamp' , '5' : 'OrderbookID' , '6' : 'Imbalance', '7' : 'AuctionPrice' ,
             '8' : 'ResumeTime', '9' : 'MatchQuantity' , '501' : 'IsFinal'} # Index mapping for message 62
    Result = { "Event Time" : [] , "Id" : [] } 
    NeedMul = ["Imbalance","AuctionPrice","MatchQuantity"] # index that need it's own value divide by 1000000
    for i in Index:
        Result[Index[i]] = []
    
    for elem in Data62: # Run through  every message id 62
        Result['Event Time'].append( StringToTime1( elem[0] ) ) 
        Result['Id'].append( elem[1] )
        for index in Index: # Run through every index
            if index in elem[2]: 
                val_to_add = elem[2][index] 
                if Index[index] in NeedMul:
                    val_to_add = int( val_to_add ) / 1000000
                Result[Index[index]].append( val_to_add )
            else:
                Result[Index[index]].append( np.NaN )
    Result = pd.DataFrame(Result)
    Result['Timestamp'] = Result['Timestamp'].apply( StringToTime2 )
    return Result

def eventMapper( eventNum ):
    if eventNum == '1':
        return 'INSERT'
    elif eventNum == '2':
        return 'CANCLE'
    elif eventNum == '3':
        return 'UPDATE'
    else:
        return np.NaN
def generateDataframe_140( Data ):
    Data140 = search_id(Data , '140')
    Index = {'2' : 'SeqNumber' , '3' : 'Timestamp' , '5' : 'OrderbookID' , '6' : 'isBid', '7' : 'Price' ,
             '8' : 'Volume', '9' : 'EventType' }
    Result = { "Event Time" : [] , "Id" : [] }
    NeedMul = ["Price","Volume"]
    for i in Index:
        Result[Index[i]] = []
    
    for elem in Data140:
        Result['Event Time'].append( StringToTime1( elem[0] ) )
        Result['Id'].append( elem[1] )
        for index in Index:
            if index in elem[2]:
                val_to_add = elem[2][index] 
                if Index[index] in NeedMul:
                    val_to_add = int( val_to_add ) / 1000000
                Result[Index[index]].append( val_to_add )
            else:
                Result[Index[index]].append( np.NaN )
    Result = pd.DataFrame(Result)
    Result['Timestamp'] = Result['Timestamp'].apply( StringToTime2 )
    Result['EventType'] = Result['EventType'].apply( eventMapper )
    return Result

def eventMapper1( eventNum ):
    if eventNum == '1':
        return 'NEW'
    elif eventNum == '2':
        return 'BUSTED'
    else:
        return np.NaN
def eventMapper2( eventNum ):
    if eventNum == '2':
        return 'TRADE_REPORT'
    elif eventNum == '3':
        return 'AUCTION'
    elif eventNum == '6':
        return 'T_TO_T'
    else:
        return np.NaN
def generateDataframe_49( Data ):
    Data49 = search_id(Data , '49')
    IndexStr = '''2 : SeqNumber
    3 : Timestamp
    5 : OrderbookID
    6 : EventType1
    7 : EventType2
    9 : TradeTime
    10 : Volume
    11 : Price
    15 : isTBL
    22 : TotalVolume
    23 : TotalTurnOver
    28 : LastRefPrice
    515 : TotalNTrades
    516 : TradeReportType'''.split('\n')
    Index = {}
    for i in IndexStr:
        isplit = i.split(':')
        Index[ isplit[0].strip() ] = isplit[1].strip() 
    Result = { "Event Time" : [] , "Id" : [] }
    NeedMul = ["Volume","Price","TotalVolume","TotalTurnOver","LastRefPrice","AvgPrice","OpenPrice","TotalTradeReportVolume",
              "TotalTradeReportTurnOver"]
    for i in Index:
        Result[Index[i]] = []
    for elem in Data49:
        Result['Event Time'].append( StringToTime1( elem[0] ) )
        Result['Id'].append( elem[1] )
        for index in Index:
            if index in elem[2]:
                val_to_add = elem[2][index] 
                if Index[index] in NeedMul:
                    val_to_add = int( val_to_add ) / 1000000
                Result[Index[index]].append( val_to_add )
            else:
                Result[Index[index]].append( np.NaN )
    Result = pd.DataFrame(Result)
    Result['Timestamp'] = Result['Timestamp'].apply( StringToTime2 )
    Result['TradeTime'] = Result['TradeTime'].apply( StringToTime2 )
    Result['EventType1'] = Result['EventType1'].apply( eventMapper1 )
    Result['EventType2'] = Result['EventType2'].apply( eventMapper2 )
    return Result

def get_all_files():
    base = input_path
    list_ym = []
    home_files = os.listdir(base)
    for file in home_files:
        found = regex.findall('^feed-mbl-([0-9]+)$' , file)
        if len(found) == 1:
            list_ym.append( found[0] )
    list_ymd = []
    for ym in list_ym:
        ymd_files = os.listdir(f'{base}/feed-mbl-{ym}')
        for file in ymd_files:
            found = regex.findall('^feed-mbl-([0-9]+).txt$',file)
            if len(found) == 1:
                list_ymd.append( [ f'{base}/feed-mbl-{ym}/feed-mbl-{found[0]}.txt' , f'{base}/feed-trade-{ym}/feed-trade-{found[0]}.txt' , found[0]] )
    return list_ymd

# if find found --> do nothing , if find not found --> save file
def save_file_pickle( data , path , force = False ):
    try:
        if force:
            a=b # exact error
        load( path )                           # try to read file
    except:
        save( data , path )                    # save file
        save_file_pickle( data , path )       # call this function again to check if data is properly save
        
# if find found --> do nothing , if find not found --> save file
def save_file_csv( data , path , force = False ):
    try:
        if force:
            a=b # exact error
        pd.read_csv( path )                   # try to read file
    except:
        data.to_csv( path )                   # save file
        save_file_csv( data , path )          # call this function again to check if data is properly save
        
# if find found --> read result from file --> return , if find not found --> calculate data --> write result --> return result
def save_load_pickle( path , readpath , force = False ):
    try:
        if force:
            a=b # exact error
        res = load( path )                   # try to rad file
    except:
        Data = TextToLines( read_txt( readpath ) )   
        Data = list( map( Extract , Data ) )           # calculate data  
        save_file_pickle( Data , path )               # save file
        res  = save_load_pickle( path , readpath )     # call this function again to check if data is properly save
    return res
# if find found --> read result from file --> return , if find not found --> calculate data --> write result --> return result
def save_load_func_pickle( path , func , param , force = False):
    try:
        if force:
            a=b # exact error
        res = load( path )                   # try to rad file
    except:
        Data = func(param)                   # calculate data  
        save_file_pickle( Data , path )     # save file
        res  = save_load_func_pickle( path , func , param ) # call this function again to check if data is properly save
    return res

# check if file exist and not corrupt
def test_load_pickle( path ):
    try:
        data = load(path)
    except:
        return False
    del data
    return True
# check if file exist and not corrupt
def test_load_csv( path ):
    try:
        data = pd.read_csv( path )
    except:
        return False
    del data
    return True
# check if files exist and not corrupt
def test_loads_pickle( paths ):
    for path in paths:
        if test_load_pickle( path ) == False:
            return False
    return True
# check if files exist and not corrupt
def test_loads_csv( paths ):
    for path in paths:
        if test_load_csv( path ) == False:
            return False
    return True


def save_to_file_date( files ): # files = ( Path to MBL file , Path to TRD file , Date )
    MBL_path = files[0]
    TRD_path = files[1]
    Date     = files[2]
    #print( "Start : " , Date )
    _base = output_path
    base = f'{_base}/{Date}'               # Path of output
    base_pickle = f'{base}/pickle' # Path of Pickle output
    base_csv    = f'{base}/csv'    # Path of csv output
    base_cache  = f'{_base}/cache/{Date}'  # Path of cache output
    pathlib.Path(base_pickle).mkdir(parents=True, exist_ok=True)  # Create result folder
    pathlib.Path(base_csv).mkdir(parents=True, exist_ok=True)     
    pathlib.Path(base_cache).mkdir(parents=True, exist_ok=True)  
    #MBL   = TextToLines( read_txt( MBL_path ) )
    #Trade = TextToLines( read_txt( TRD_path ) )
    #MBL   = list( map( Extract , MBL ) )
    #Trade = list( map( Extract , Trade ) )
    
    print( f"{Date} Extract MBL ... " )
    MBL   = save_load_pickle( f'{base_cache}/MBL_Raw.dat'   , MBL_path )    # Calculate MBL data or read from cache
    #print( "Complete")
    print( f"{Date} Extract TRD ... " )
    TRD = save_load_pickle( f'{base_cache}/Trade_Raw.dat' , TRD_path )      # Calculate Trade data or read from cache
    #print( "Complete")
    print( f"{Date} Generate Orderbook ... " )
    orderbookID , orderbookID_rev = generate_orderbook_id( MBL )  # Get list of available symbol
    Orderbook = generateDataframe_296( MBL )  # calculate df of message id = 296 
    
    
    save_file_pickle( Orderbook , f'{base_pickle}/orderbook.dat' , force = True ) # save orderbook
    save_file_csv    ( Orderbook , f'{ base_csv }/orderbook.csv'  , force = True ) # save orderbook
    #print( "Complete")
    print( f"{Date} Generate Auction ... " )
    Auction = save_load_func_pickle ( f'{base_cache}/Auction.dat' , generateDataframe_62 , MBL ) # get df of orderbook id = 62
    #print( "Complete")
    print( f"{Date} Generate Book ... " )
    Book    = save_load_func_pickle ( f'{base_cache}/Book.dat' , generateDataframe_140 , MBL ) # get df of orderbook id = 140
    #print( "Complete")
    print( f"{Date} Generate Trade ... " )
    Trade   = save_load_func_pickle ( f'{base_cache}/Trade.dat' , generateDataframe_49 , TRD ) # get df of orderbook id = 49
    #print( "Complete")
    print( f"{Date} Generate Symbol ... " )
    count = 0
    for ID in orderbookID_rev.keys():
        Name   = orderbookID_rev[ID] + '_' + ID
        #print( f'{Date} : {Name}        ' , end = '\r' )
        
        pickle_files = [ f'{base_pickle}/{Name}_auction.dat' , f'{base_pickle}/{Name}_book.dat' , f'{base_pickle}/{Name}_trade.dat' ]
        csv_files    = [ f'{ base_csv }/{Name}_auction.csv'  , f'{ base_csv }/{Name}_book.csv'  , f'{ base_csv }/{Name}_trade.csv' ]
        if test_loads_pickle( pickle_files ) == False or test_loads_csv( csv_files ) == False: # Check if files exist
            count += 1
            _Auction = Auction[Auction.OrderbookID == ID] # Auction Data
            _Book    = Book[Book.OrderbookID == ID]       # Orderbook Data
            _Trade   = Trade[Trade.OrderbookID == ID]     # Trade Data
            save_file_pickle( _Auction , f'{base_pickle}/{Name}_auction.dat')  # Save Auction
            save_file_csv    ( _Auction , f'{ base_csv }/{Name}_auction.csv')  # Save Auction

            save_file_pickle( _Book , f'{base_pickle}/{Name}_book.dat') # Save orderbook
            save_file_csv    ( _Book , f'{ base_csv }/{Name}_book.csv') # Save orderbook

            save_file_pickle( _Trade , f'{base_pickle}/{Name}_trade.dat') # Save Trade
            save_file_csv    ( _Trade , f'{ base_csv }/{Name}_trade.csv') # Save Trade
            del _Auction
            del _Book
            del _Trade
    print( f"{Date} Complete ( {count} )" )
    del MBL
    del Trade
    del Auction
    del Book
    
pool = mp.Pool( Ncores )
pool.map( save_to_file_date , get_all_files() ) 




