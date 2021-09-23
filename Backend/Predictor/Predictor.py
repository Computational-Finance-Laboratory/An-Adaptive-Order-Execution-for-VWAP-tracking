from datetime import time
import datetime
import pandas as pd
import numpy as np
import datetime
from Predictor.model import *

def predict(symbol, volume, start_time, end_time, side):
    #convert side to int
    if side == 'Buy':
        side = 1
    else:
        side = -1
    #get Executiion result    
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
