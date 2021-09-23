from datetime import time
import datetime
import pandas as pd
import numpy as np
import datetime
from Predictor.model import *

# def predict(symbol, volume, start_time, end_time):
#     #print(symbol, volume, start_time, end_time)
#     df = pd.DataFrame({
#         "limit_volume": [1000, 2000, 3000, 4000],
#         "market_volume": [400, 300, 200, 100],
#         }, index=[time(10, 1), time(10, 2), time(10, 3), time(10, 4)]
#     )
#     df.index.name = "time"
#     return df

#def predict(symbol, volume, start_time, end_time, side):
#    list1 = []
#    list2 = []
#    for i in range (25):
#        list1.append(random.randint(1, 100)*1000)
#        list2.append(random.randint(1, 100)*1000)
#    print(start_time,end_time,type(start_time),type(end_time))
#    df = pd.DataFrame({
#        "limit_volume": list1,
#        "market_volume": list2,
#        }, index=[time(10, 1), time(10, 2), time(10, 3), time(10, 4),
#        time(10, 5), time(10, 6), time(10, 7), time(10, 8),
#        time(10, 9), time(10, 10), time(10, 11), time(10, 12),
#        time(10, 13), time(10, 14), time(10, 15), time(10, 16),
#        time(10, 17), time(10, 18), time(10, 19), time(10, 20),
#        time(10, 21), time(10, 22), time(10, 23), time(10, 24),
#        time(10, 25)]
#    )
#    print(side)
#    df.index.name = "time"
#    return df

def predict(symbol, volume, start_time, end_time, side):
    #convert side to int
    if side == 'Buy':
        side = 1
    else:
        side = -1
    #get sim result    
    sim_tracker,marketVWAP = get_execution(symbol,volume,start_time,end_time,side)
    
    #calculate Executed Volume at each minute
    sim_tracker['ExecutedVolume'] = sim_tracker['InitValue'] - sim_tracker['CurrentValue']
    #Convert timestamp into datetime.time
    sim_tracker['Time'] = sim_tracker.Time.apply( lambda x : pd.pandas._libs.tslibs.timestamps.Timestamp.to_pydatetime(x).time())
    #Get rid of time where there is no Excuted Volume
    sim_tracker = sim_tracker[sim_tracker['ExecutedVolume'] > 0]
    #Create table in form of list of dict of string
    table = []
    for i in range(len(sim_tracker.index)):
        time = sim_tracker['Time'].iloc[i]
        volume = sim_tracker['ExecutedVolume'].iloc[i]
        price = sim_tracker['Level'].iloc[i]
        otype = sim_tracker['Type'].iloc[i]
        row = {}
        row['time'] = str(time)[:5]
        row['volume'] = str(int(volume))
        row['price'] = str(price)
        row['otype'] = str(otype)
        table.append(row)

    return table,marketVWAP
