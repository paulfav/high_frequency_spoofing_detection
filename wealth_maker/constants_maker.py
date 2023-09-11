import numpy as np
import os



class Constants(): 

    def __init__(self) -> None:


        # GENERAL
        self.DATA_NAMES = [
        f'/Users/paulfaverjon/Desktop/preparation/data/orderbooks_{part}/orderbooks-5limits-part{i}.parquet' 
        for part in [1] for i in range(10,20) ]


        self.DATA_NAMES_SIMUL = [
        f'/Users/paulfaverjon/Desktop/preparation/data/orderbooks_{part}/orderbooks-5limits-part{i}.parquet' 
        for part in [1] for i in range(10,12) ]


        self.IMBALANCE_BUCKETISED_VALUES = [round(-1 + 0.1 * i, 1) for i in range(21)]
        self.IMBALANCE_DISTANCES_RANGE = [6] #[np.round(i,2) for i in np.arange(1, 11, 1)] 
        #not just 6 just so that the parallelizzed code can still work


        self.DELTA_EVENTS = 350
        self.MANIP_DISTANCE_BPS_RANGE = [round(i,2) for i in np.arange(0.01, 3, 0.25)]  # Way too much according to preliminary studies.
        self.MANIP_SIZE_MULTIPLE_RANGE = [round(i,2) for i in np.arange(0.5, 100, 5)]


        #CALCULATING COST FUNCTION

        self.EPSILON_PLUS = 0.000 # maker fee
        self.EPSILON_MOINS = 0.0005# taker fee
        self.OPTIMAL_IMBALANCE_DISTANCE = 6  # for expectation






        
    

