import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import datetime
import multiprocessing as mp
import pickle
from Predictor import simulation,VolOnline,SVM_lib
pd.set_option('display.max_rows', 10000 )


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


def unix_time_millis(dt):
    return (dt - epoch).total_seconds() * 1000.0


def create_sim(Event , Trade , Auction,start_time,end_time):
    morn_start_time = ( 10 * 60 ) + 5 
    noon_start_time = ( 14 * 60 ) + 35 
    morn_end_time = ( 12 * 60 ) + 29 
    noon_end_time = ( 16 * 60 ) + 29 
    start_time_min = ( start_time[0] * 60 ) + start_time[1]
    end_time_min = ( end_time[0] * 60 ) + end_time[1]
    print(start_time,end_time) 
    if start_time_min < noon_start_time and start_time_min > morn_end_time:
        start_time = (14,35)
    if end_time_min < noon_start_time and end_time_min > morn_end_time:
        end_time = (12,29)
    if start_time_min < noon_start_time and start_time_min > morn_end_time:
        if end_time_min < noon_start_time and end_time_min > morn_end_time:
            start_time = (12,29)
            end_time = (12,29)   
     
    print(start_time,end_time)    
    if start_time_min < morn_start_time:
        start_time = (10,5)
    if end_time_min > noon_end_time:
        end_time = (16,29)
    print(start_time,end_time) 
    sim = simulation.Simulation( Event , Trade , Auction , morn_start_time= start_time , noon_end_time= end_time)
    
    return sim

def simulate(OrderbookID, symbol, DATE , S , time_to_wait , MO_changerate , Volume_changerate , side , start_time , end_time , management = True ):
    time_to_wait = time_to_wait * 60
    # try to get volpred from pickle 
    # if we can't find volpred then create volpred and dump into pickle for future use
    try:
        fileObj = open(f'Predictor/VolPredDir/volpred_{symbol}.obj', 'rb')
        volpred = pickle.load(fileObj)
        fileObj.close()
    except Exception as e:
        print(e)
        volpred = VolOnline.VolPred( symbol, OrderbookID )
        fileObj = open(f'Predictor/VolPredDir/volpred_{symbol}.obj', 'wb')
        pickle.dump(volpred,fileObj)
        fileObj.close()
    #______________________ Load Data to create simulation _______________________
    orderbook = load(f"./Data/{DATE}/pickle/orderbook.dat")
    OrderbookID = orderbook[orderbook['Persistant Name'] == symbol].iloc[0]['OrderbookID']
    Event = load(f"./Data/{DATE}/pickle/{symbol}_{OrderbookID}_event.dat")
    Trade = load(f"./Data/{DATE}/pickle/{symbol}_{OrderbookID}_trade2.dat")
    Auction = load(f"./Data/{DATE}/pickle/{symbol}_{OrderbookID}_auction.dat")
    
    #_____________________________________________________________________________
    
    #_______________________________Load SVM_____________________________________
    #load SVM prediction and resample to 1 minute
    #if there is no SVM prediction in storage yet the script should create a new one automatically
    SVM_df = SVM_lib.load_SVM_df(symbol)    #the function will either load file or create a new one
    SVM_df['Time'] = pd.to_datetime(SVM_df['Time'])
    SVM_df.set_index('Time',inplace = True)
    SVM_df = SVM_df.resample('T').ffill()
    #_____________________________________________________________________________
    
    #_________________________Calculate Hyper(ish)parameter_______________________
    # determine how frequent for simulation to work(in this case is 1 minute)
    Interval = datetime.timedelta( seconds = 60 )
    # S will become remaining volume and C will become target volume
    C = S
    #_____________________________________________________________________________
    
    #_____________________________Create Simulation and plan______________________
    # create simulation
    start_time  = (start_time.hour,start_time.minute)
    end_time = (end_time.hour,end_time.minute)
    sim = create_sim( Event , Trade , Auction , start_time , end_time)
    # ------------------------- Algotithm initialize -----------------------------
    plan = VolOnline.VolumeOnline( volpred,sim )  # Date , class simulation , class Volpred
    #print( Data[DATE][3].iloc[-100:] )
    #Create MO LO side  side = 1 mean buy side = -1 mean sell
    
    #side 1 == BUY  -1 == SELL
    if side == 1:
        MO_side = "Offer"
        LO_side = "Bid"
    elif side == -1:
        MO_side = "Bid"
        LO_side = "Offer"
    isClosed = False
    while not isClosed:
        sim.do()                     # simulate until end of timestamp
        #stop at last minute and dump MO
        isClosed = sim.isClosed()
        if isClosed:
            break
        management = True
        t = sim.Current              #  Current time in simulation
        
        #get the amount of stock to execute (in form of percentage of remaining volume(S))
        Amount = plan.getPlan( side = side )[0]
        # ratio betwwen MO and LO
        MO_LO  = plan.getPlan( side = side )[1]
        #switch ratio if sell
        if side == -1:
            MO_LO = 1 - MO_LO
        
        #short term price prediction from SVM
        pred = 0
        # we need to use try except for to prevent error from SVM 
        try:
            pred      = SVM_df.loc[sim.Current].pred
        except:
            pass
        
        #Calculate amount of stock to execute
        toExecute   =  Amount * S
        if (S <= 0):
            S = 0
            toExecute = 0
        MOratio = MO_LO
        
        # Change Volume and MO_LO ratio by SVM prediction
        if pred == side:
            toExecute   = min( [ S , toExecute * ( 1 + Volume_changerate ) ] )
            MOratio = min( [ MOratio * ( 1 + MO_changerate ) , 1 ] )
        elif pred == -side:
            toExecute   = max( [ 0 , toExecute * ( 1 - Volume_changerate ) ] )
            MOratio = max( [ MOratio * ( 1 - MO_changerate ) , 0 ] )
        if side == 1:
            LO_price = "BestBid"
        else:
            LO_price = 'BestOffer'

        # if Ceiling or Floor happend
        # Ceiling you can use LO only order's side is BUY
        # Floor you can use LO only order's side is SELL
        if sim.Ceiling and sim.Floor:
            raise Exception("Ceiling == Floor Exception")
            
        if sim.Ceiling:
            if side == 1 :
                MOratio = 0
                management = False
            else:
                MOratio = 1
        if sim.Floor:
            if side == 1:
                MOratio = 1
            else:
                MOratio = 0
                management = False
        #Execute order
        MO_volume = int((toExecute * MOratio)/100) * 100
        LO_volume =  int((toExecute * ( 1 - MOratio ))/100) * 100
        if LO_volume > 0:
            sim.addLO( LO_price , LO_volume )         # add LO order
        if MO_volume > 0:
            sim.addMO( MO_volume , MO_side )            # add MO order

        #------------------------------ Manage Policy ---------------------------------------------
        if management:
            if not(sim.Ceiling or sim.Floor):
                
                #cancel LO that is not execute in limited time 
                tracker = sim.tracker.get()
                tracker = tracker[tracker['Status'] == True]
                tracker['time to wait'] = (sim.Current - tracker['Time'])
                tracker['time to wait'] = tracker['time to wait'].apply(datetime.timedelta.total_seconds)

                #if we pass 12.30 we need to do this code below
                if sim.Current > datetime.datetime.strptime( str(sim.Current)[2:10] + " 14:30:00", '%y-%m-%d %H:%M:%S'):
                    #for all order that been place before 12.30 we will reduce time to wait 150 minutes (2.5 hour of lunch break)
                    for i in range(len(tracker.index)):
                        if tracker['Time'].iloc[i] < datetime.datetime.strptime( str(sim.Current)[2:10] + " 14:30:00", '%y-%m-%d %H:%M:%S'):
                            tracker['time to wait'].iloc[i] -= 120 * 60

                #now find all LO that exceed limit time 
                #Cancel it and addMO instead
                for i in range(len(tracker.index)):
                    if tracker['time to wait'].iloc[i] > time_to_wait and tracker['Status'].iloc[i] == True :
                        cancel_volume = tracker['CurrentValue'].iloc[i]
                        sim.cancleLO(tracker.index[i])  
                        sim.addMO(cancel_volume,MO_side)

        # ------------------------------------------------------------------------------------------
        S -= toExecute
        
        
        sim.forward()                                         # forward simulation time to next interval
    sim.do()                                                  # simulation of the last interval
    NotExecutedVolume = C - (sim.tracker.get()['InitValue'] - sim.tracker.get()['CurrentValue']).sum()
    left = max( [ NotExecutedVolume , 0 ] )          # find remaining amount of stock that need to execute
    #print(DATE,left,sim.get_statistic()[0] ,left + sim.get_statistic()[0] )
    if ( side == 1 and not sim.Ceiling ) or ( side == -1 and not sim.Floor ):
        sim.addMO( left , MO_side )                               # add MO order (ATC)
    else:
        print( f"{symbol} , {DATE} cannot do last execution due to Ceilng or Floor" )
    #get tracker of MO LO 
    sim_tracker = sim.tracker.get()
    return sim_tracker, sim.marketVWAP()

def get_execution(symbol, volume, start_time, end_time, side):
    #create date variable it should be in this format "20200707"
    date = str(pd.to_datetime(start_time))[:10].replace('-','')

    orderbook = load(f"./Data/{DATE}/pickle/orderbook.dat")
    OrderbookID = orderbook[orderbook['Persistant Name'] == symbol].iloc[0]['OrderbookID']

    #start_time and end_time format is YY-MM-DD HH:MM:SS (2020-10-17 10:00:00) 
    #but sometime format is not consistent (2020-2-7 10:00:00) instead of (2020-02-07 10:00:00)
    #we need to use pd.to_datetime to convert into correct format before convert to datetime.datetime
    start_time = datetime.datetime.strptime(str(pd.to_datetime(start_time)), '%Y-%m-%d %H:%M:%S')
    end_time = datetime.datetime.strptime(str(pd.to_datetime(end_time)), '%Y-%m-%d %H:%M:%S')
    #set fix argument
    timeout = 60
    MoChangeRate = 50
    VolChangeRate = 0
    
    sim_tracker,marketVWAP  = simulate( OrderbookID, symbol, date , volume , timeout * 60 , MoChangeRate/100 , 
                                        VolChangeRate/100, side , start_time, end_time )
    return sim_tracker,marketVWAP
    
if __name__ == "__main__":
    #for test only
    symbol = "STOCK"
    print(get_execution(symbol,1000000,"2020-07-07 10:00:00","2020-07-07 16:30:00",1))