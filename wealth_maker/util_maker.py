import pandas as pd
from time import time
from joblib import Parallel, delayed
import multiprocessing
import collections as collec
from scipy.interpolate import UnivariateSpline
import os
import importlib
import constants_maker
importlib.reload(constants_maker)
from constants_maker import *

constant = Constants()





####### SOME PREPROCESSING AND POST PROCESSING #######


def get_volume_differences(line, data, delta_events): 

    #['etimestamp','eprice', 'etype', 'esize_ini', 'eside', 'bp0', 'ap0', 'is_from_marketable']
    

    row = data[line]
    distances_to_sides_to_added_liquidity = {distance:{'B':0 , 'S': 0} for distance in constant.IMBALANCE_DISTANCES_RANGE }
    iter_line = line 
    events_looked_at=0
    data_iter_line = row
    limit_order_memory = []
   
    while events_looked_at < delta_events :
        iter_line -= 1
        if iter_line < 0 : 
            break

        events_looked_at += 1
        if (data_iter_line[2] != 'A') or data_iter_line[-1] == True : #order_type and marketable = False
            data_iter_line = data[iter_line]
            continue
        limit_order_memory.append(
            {'price' : data_iter_line[1], #eprice
             'side' : data_iter_line[4], #eside
             'volume': data_iter_line[3] #esize_ini
             })
        data_iter_line = data[iter_line]
    

    
    distances_to_sides_to_prices_ini = {distance :
                                     {'B' : data_iter_line[5] *(1 - distance / 10_000) , 'S' : data_iter_line[6]* (1 + distance / 10_000) } 
                                     for distance in constant.IMBALANCE_DISTANCES_RANGE} #best bid, best_ask
    

    for limit_order in limit_order_memory : 
        trade_side = limit_order['side']
        for distance in constant.IMBALANCE_DISTANCES_RANGE : 
            sides_to_prices_ini = distances_to_sides_to_prices_ini[distance]
            
            if ((trade_side == 'B') and (limit_order['price'] >= sides_to_prices_ini['B'])) or (
                (trade_side == 'S') and (limit_order['price'] <= sides_to_prices_ini['S'])) :
                distances_to_sides_to_added_liquidity[distance][trade_side] += limit_order['volume']

    return distances_to_sides_to_added_liquidity





def get_imbalance(vol_bid_diff, vol_ask_diff):

    if vol_bid_diff + vol_ask_diff == 0 :
        return 0
    
    imbalance =  (vol_bid_diff - vol_ask_diff) / (vol_bid_diff + vol_ask_diff)
    return imbalance





def get_differences_imbalances(distances_to_sides_to_added_liquidity):
    

    distances_to_imbalances = {distance : get_imbalance(distances_to_sides_to_added_liquidity[distance]['B'],
                                                               distances_to_sides_to_added_liquidity[distance]['S'])
                                                               for distance in constant.IMBALANCE_DISTANCES_RANGE}
    
    return distances_to_imbalances 




def preprocess(name):
    data = pd.read_parquet(name)

    mean_trade_size = data.loc[data['etype'] == 'T', 'esize_ini'].mean()
    data = data[['etimestamp','eprice', 'etype', 'esize_ini', 'eside', 'bp0', 'ap0', 'is_from_marketable']]
    dataloc = data.values
    return mean_trade_size , dataloc




#########  GET DATA FOR  P (Lbid(delta, Q, imbalance) <  h ) ##################################



def get_data_L_bid(manip_order_distance):
    
    order_id = 0
    tradecimetery_to_features_to_values3 = []
    t_start = time()

    delta_events = constant.DELTA_EVENTS
    multiples_range = constant.MANIP_SIZE_MULTIPLE_RANGE



    for name in constant.DATA_NAMES:
        #print(name)

        ats, dataloc = preprocess(name)
        length = len(dataloc)
        

        

        multiple_to_volumes = {multiple : multiple * ats for multiple in multiples_range}



        line = delta_events + 1
        order_state = False
        
        while line < length - 1 : 
            current_timestamp = dataloc[line][0]
            while dataloc[line+1][0] == current_timestamp : 
                line += 1
                if line + 1 == length : break
            #We are now at the last line of a timestamp
            
            if order_state != 'pending': # if the order was executed or partially executed, it will post a new order. If it wasn't executed, it will let the order continue
                #for another timestamp
                #print("posting order")
                order_state = 'pending'
                row_order = dataloc[line]
                order_id += 1
                #print(row_order[0])

                distances_to_sides_to_added_liquidity = get_volume_differences(line, dataloc, delta_events)
                distances_to_imbalances = get_differences_imbalances(distances_to_sides_to_added_liquidity)
            
                manip_bid_to_values = {
                        "distance" : manip_order_distance,
                        "price": row_order[5] * ( 1 - manip_order_distance / 10_000), #where we place our manip order
                        "order_id": order_id,
                        "timestamp_insertion": row_order[0], #timestamp
                        "best_bid_at_insertion": row_order[5], #bp0
                        "volume_under": 0,
                        "len_boucle" : 0}
                
                for distance, imbalance in distances_to_imbalances.items():
                    #print(imbalance)
                    manip_bid_to_values[f'var_imbalance_at_{distance}'] = round(imbalance,1) #this replaces the buckets


            line += 1
            # we are now at the first line of the timestamp of our order
            iterline = line
            if line >= length - 2 : break

            while dataloc[iterline + 1][0] == dataloc[line][0]: #same timestamp
                row_iter = dataloc[iterline]
                if  (row_iter[2] == 'T') & (row_iter[4]=='B') & (row_iter[1]< manip_bid_to_values['price']): # there is a trade at this timestamp that was over our target order in price.
                    manip_bid_to_values['volume_under'] += row_iter[3] # we add the order's size.
            
                iterline +=1 
                if iterline + 1 >= length : break

            # we are now at the last line of our timestamp
            data_iter_line = dataloc[iterline]

            if (data_iter_line[5] >= manip_bid_to_values['best_bid_at_insertion']) & (
                manip_bid_to_values['volume_under'] == 0 
            ): # if the best hasn't moved down and we weren't executed, the order is kept.
                line = iterline + 1
                manip_bid_to_values['len_boucle'] +=1

                continue

            # in any other case, our order dies.
            for multiple in multiples_range :
                manip_bid_to_values[f'{multiple}_ats_ratio_exec'] = min(1 , manip_bid_to_values['volume_under'] / multiple_to_volumes[multiple] )

            manip_bid_to_values['lifetime'] = data_iter_line[0] - manip_bid_to_values['timestamp_insertion']
            tradecimetery_to_features_to_values3.append(manip_bid_to_values)

            order_state = 'Executed'


            
            line = iterline + 1
            

    print(f'calculated exec data *** {manip_order_distance} bps for manip order distance  in {round(time()-t_start, 2)}s')        

    return tradecimetery_to_features_to_values3




def get_parallel_data_L_bid():
    
    num_cores = multiprocessing.cpu_count()
    results = Parallel(n_jobs=num_cores)(delayed(get_data_L_bid)(distance) for distance in constant.MANIP_DISTANCE_BPS_RANGE)
    manip_distance_to_orders_to_values = {constant.MANIP_DISTANCE_BPS_RANGE[i]: results[i] for i in range(len(constant.MANIP_DISTANCE_BPS_RANGE))}
    return manip_distance_to_orders_to_values





def get_all_probas_L_bid(manip_distance_to_orders_to_values):
    #takes a dictionnary : {manip distance : [list of orders wih different sizes and the imbalance variation
    #plus other features.
    manip_distance_to_multiple_to_imbalance_level_to_values_to_ratios = {
        distance : {multiple : {level : { value : []
                                         for value in constant.IMBALANCE_BUCKETISED_VALUES

        } for level in constant.IMBALANCE_DISTANCES_RANGE
        }for multiple in constant.MANIP_SIZE_MULTIPLE_RANGE
        } for distance in constant.MANIP_DISTANCE_BPS_RANGE}
    
    manip_distance_to_multiple_to_imbalance_level_to_values_to_probas = {
        distance : {multiple : {level : { value : 0
                                         for value in constant.IMBALANCE_BUCKETISED_VALUES

        } for level in constant.IMBALANCE_DISTANCES_RANGE
        }for multiple in constant.MANIP_SIZE_MULTIPLE_RANGE
        } for distance in constant.MANIP_DISTANCE_BPS_RANGE}


    
    for (
        manip_distance, multiple_to_imbalance_level_to_values_to_ratios
     ) in manip_distance_to_multiple_to_imbalance_level_to_values_to_ratios.items():
        
        orders_to_values = manip_distance_to_orders_to_values[manip_distance]
        for order in orders_to_values :
            
            for imbalance_level in constant.IMBALANCE_DISTANCES_RANGE :
                value = order[f'var_imbalance_at_{imbalance_level}']
                for multiple in constant.MANIP_SIZE_MULTIPLE_RANGE :
                    ratio = order[f'{multiple}_ats_ratio_exec']
                    multiple_to_imbalance_level_to_values_to_ratios[multiple][imbalance_level][value].append(ratio)
        

        for multiple, imbalance_level_to_values_to_ratios in multiple_to_imbalance_level_to_values_to_ratios.items():
            for imbalance_level, values_to_ratios in imbalance_level_to_values_to_ratios.items():
                for value, ratios in values_to_ratios.items(): 
                    total = len(ratios)
                    manip_distance_to_multiple_to_imbalance_level_to_values_to_probas[manip_distance][multiple][imbalance_level][value] = sum(ratios) / total if total else 0
                
        print(manip_distance_to_multiple_to_imbalance_level_to_values_to_probas[manip_distance])
 


    return manip_distance_to_multiple_to_imbalance_level_to_values_to_probas





############## GET DATA FOR P(L_ASK(0, 1, imbalance) < h)


def get_orders_data_target():

    delta_events = constant.DELTA_EVENTS
    
    order_id = 0
    tradecimetery_to_features_to_values3 = []
    t_start = time()

    for name in constant.DATA_NAMES:
    
        
       
        mean_trade_size, dataloc = preprocess(name)
        length = len(dataloc)
        current_timestamp = dataloc[0][0]

        line = delta_events + 1
        order_state = False
        
        while line < length - 1 : 
            while dataloc[line + 1][0] == current_timestamp : 
                line += 1
                if line + 1 == length : break
            #We are now at the last line of a timestamp
            
            if order_state != 'pending': # if the order was executed or partially executed, it will post a new order. If it wasn't executed, it will let the order continue
                #for another timestamp
                order_state = 'pending'
                row_order = dataloc[line]
                order_id += 1

                distances_to_sides_to_added_liquidity = get_volume_differences(line, dataloc, delta_events)
                distances_to_imbalances = get_differences_imbalances(distances_to_sides_to_added_liquidity)
            
                target_ask_to_values = {
                        "price": row_order[6] * ( 1 + 0 / 10_000), #where we want to be executed
                        "order_id": order_id,
                        "timestamp_insertion": row_order[0], #timestamp
                        "best_bid_at_insertion": row_order[5], #bp0
                        "volume_over": 0,
                        "len_boucle" : 0,
                        "size_order" : mean_trade_size # rolling mean
                        }
                
                for distance, imbalance in distances_to_imbalances.items():
                    #print(imbalance)
                    target_ask_to_values[f'var_imbalance_at_{distance}'] = round(imbalance,1) #this replaces the buckets
            
            
            line += 1
            # we are now at the first line of the timestamp of our order
            iterline = line
            if line >= length - 2 : break

            while dataloc[iterline + 1][0] == dataloc[line][0]: #same timestamp
                row_iter = dataloc[iterline]
                if  (row_iter[2] == 'T') & (row_iter[4]=='S') & (row_iter[1]>= target_ask_to_values['price']): # there is a trade at this timestamp that was over our target order in price.
                    target_ask_to_values['volume_over'] += row_iter[3] # we add the order's size.
            
                iterline +=1 
                if iterline + 1 >= length : break

            # we are now at the last line of our timestamp
            data_iter_line = dataloc[iterline]

            if (data_iter_line[5] >= target_ask_to_values['best_bid_at_insertion']) & (
                target_ask_to_values['volume_over'] == 0 
            ): # if the best hasn't moved down and we weren't executed, the order is kept.
                line = iterline + 1
                target_ask_to_values['len_boucle'] +=1

                continue

            # in any other case, our order dies.
            #
            target_ask_to_values[f'ratio_exec'] = min(1 , target_ask_to_values['volume_over'] / mean_trade_size )

            target_ask_to_values['lifetime'] = data_iter_line[0] - target_ask_to_values['timestamp_insertion']
            tradecimetery_to_features_to_values3.append(target_ask_to_values)

            order_state = 'Executed'

            
            line = iterline + 1
            

    print(f'calculated exec data *** {delta_events} delta_events  in {round(time()-t_start, 2)}s')        

    return tradecimetery_to_features_to_values3









def get_probas_L_ask(tradecimetery_to_features_to_values3): 

    distances_to_values_to_orders = {distance : {value : [] for value in constant.IMBALANCE_BUCKETISED_VALUES} for distance in constant.IMBALANCE_DISTANCES_RANGE}

    for trade in tradecimetery_to_features_to_values3: 
        ratio_exec = trade['ratio_exec']
        for distance in constant.IMBALANCE_DISTANCES_RANGE :
            distances_to_values_to_orders[distance][trade[f'var_imbalance_at_{distance}']].append(ratio_exec)
    
    distances_to_imbalances_to_probas_L_ask = {
        distance : {
            imbalance : 0 for imbalance in constant.IMBALANCE_BUCKETISED_VALUES
            } for distance in constant.IMBALANCE_DISTANCES_RANGE
            }


    for distance, imbalance_values_to_ratio_execs in distances_to_values_to_orders.items() :
        for imbalance, ratio_execs in imbalance_values_to_ratio_execs.items() : 
            
            total = len(ratio_execs) 
            distances_to_imbalances_to_probas_L_ask[distance][imbalance] = sum(ratio_execs) / total if total !=0 else 0

            

    return distances_to_imbalances_to_probas_L_ask

 
        





############# GET DATA FOR EXPECT[DELTA_P] #######################



def get_bid_movement():

    delta_events = constant.DELTA_EVENTS

    price_id = 0
    pricescimetery_to_features_to_values = []
    t_start = time()

    for name in constant.DATA_NAMES :

        _, dataloc = preprocess(name)
        length = len(dataloc)
        current_timestamp = dataloc[0][0]
        length = len(dataloc)
        line = delta_events + 1
        current_timestamp = dataloc[line][0]
        while dataloc[line + 1][0] == current_timestamp : 
            line += 1
            if line + 1 >= length : break
        
        # we are now at the end of a timestamp
        while line < length - 2 :
            # we are now at the end of a timestamp
            
            price_id +=1
            row = dataloc[line]
            bid_price_feature_to_value = {
                'price_id' : price_id,
                'timestamp_insertion' : row[0], 
                'best_bid_at_insertion' : row[5],
                }
            
            distances_to_sides_to_added_liquidity = get_volume_differences(line, dataloc, delta_events)
            distances_to_imbalances = get_differences_imbalances(distances_to_sides_to_added_liquidity)
            #print(distances_to_imbalances)
            
            for distance, imbalance in distances_to_imbalances.items():
                #print(imbalance)
                bid_price_feature_to_value[f'var_imbalance_at_{distance}'] = round(imbalance,1) #this replaces the buckets
            
            while line < length - 2 : 
                line += 1
                # we are now at the first line of the timestamp of our order
                current_timestamp = dataloc[line][0]
                while dataloc[line + 1][0] == current_timestamp : 
                    line += 1
                    if line  == length - 1 : break
                
                
                # we are now at the last line of the timestamp of our order
                if dataloc[line][5] < bid_price_feature_to_value['best_bid_at_insertion']: 
                    bid_price_feature_to_value['lifetime'] = dataloc[line][0] - bid_price_feature_to_value['timestamp_insertion']
                    bid_price_feature_to_value['best_price_movement'] = dataloc[line][5] - bid_price_feature_to_value['best_bid_at_insertion']
                    pricescimetery_to_features_to_values.append(bid_price_feature_to_value)
                    break

          


    print(f'calculated exec data *** {delta_events} delta_events in {round(time()-t_start, 2)}s')        

    return pricescimetery_to_features_to_values





def round_to_nearest_point_2(n):
    return round(round(n / 0.2) * 0.2, 1)


def get_values(pricescimetery_to_features_to_values):

    distances_to_imbalances_values_to_price_moves = {distance : {value : [] 
                                                                 for value in constant.IMBALANCE_BUCKETISED_VALUES} 
                                                    for distance in constant.IMBALANCE_DISTANCES_RANGE}
    L = len(pricescimetery_to_features_to_values)

    for order in pricescimetery_to_features_to_values : 
        best_price_mov = order['best_price_movement']
        for distance in constant.IMBALANCE_DISTANCES_RANGE :
            distances_to_imbalances_values_to_price_moves[distance][order[f'var_imbalance_at_{distance}']].append(best_price_mov)
    
    return distances_to_imbalances_values_to_price_moves




def get_probas(distances_to_imbalance_values_to_price_moves): 

    distances_to_imbalances_to_diff_to_probas = {
        distance : {
            imbalance : {} for imbalance in constant.IMBALANCE_BUCKETISED_VALUES
            } for distance in constant.IMBALANCE_DISTANCES_RANGE
            }


    for distance, imbalance_values_to_price_moves in distances_to_imbalance_values_to_price_moves.items() :

        for imbalance, price_moves in imbalance_values_to_price_moves.items() : 
            rounded_prices = [round_to_nearest_point_2(price_diff) for price_diff in price_moves]

            total = len(rounded_prices)
            prices_to_counts = collec.Counter(rounded_prices)
            prices_to_probas = {price : count/total for price, count in prices_to_counts.items()}

            distances_to_imbalances_to_diff_to_probas[distance][imbalance] = prices_to_probas

            

    return distances_to_imbalances_to_diff_to_probas




    
def get_conditional_expectancies(distances_to_imbalances_to_diff_to_probas) : 

    distances_to_imbalances_to_expectancies = {distance : {} for distance in constant.IMBALANCE_DISTANCES_RANGE}


    for distance, imbalances_to_diff_to_probas in distances_to_imbalances_to_diff_to_probas.items():

        for imbalance, diff_to_probas in imbalances_to_diff_to_probas.items():

            expect = sum(diff * proba for (diff,proba) in diff_to_probas.items())
            distances_to_imbalances_to_expectancies[distance][imbalance] = expect


    return distances_to_imbalances_to_expectancies





def get_dist_to_imb_to_expect(): 

    pricescimetery_to_features_to_values = get_bid_movement()
    distances_to_imbalance_to_probas = get_probas(get_values(pricescimetery_to_features_to_values))
    distances_to_imbalances_to_expectancies = get_conditional_expectancies(distances_to_imbalance_to_probas)

    return distances_to_imbalances_to_expectancies



def get_dist_to_imb_to_expect_splined() :

    dist_to_imb_to_expect_splined = { distance : {}
                                     for distance in constant.IMBALANCE_DISTANCES_RANGE}

    distances_to_imbalances_to_expectancies = get_dist_to_imb_to_expect()
    for distance, imbalances_to_expectancies in distances_to_imbalances_to_expectancies.items() :
        y_ = list(imbalances_to_expectancies.values())
        x_ = list(imbalances_to_expectancies.keys())
        spl = UnivariateSpline(x_, y_)
        spl.set_smoothing_factor(0.5)
        dist_to_imb_to_expect_splined[distance] = spl(x_)

    return dist_to_imb_to_expect_splined



########## IMPLEMENETING THE JOINT PROBABILITY P[Lbid>h, Lask>h ] #######




def get_L_bid_L_ask_data(manip_distance):


    delta_events = constant.DELTA_EVENTS
    
    
    order_id = 0
    tradecimetery_to_features_to_values3 = []
    t_start = time()

    for name in constant.DATA_NAMES:
    
        mean_trade_size, dataloc = preprocess(name)


        length = len(dataloc)
        current_timestamp = dataloc[0][0]

        line = delta_events + 1
        order_state = False
        
        while line < length - 1 : 
            while dataloc[line + 1][0] == current_timestamp : 
                line += 1
                if line + 1 == length : break
            #We are now at the last line of a timestamp
            
            if order_state != 'pending': # if one of the order was executed or partially executed, it will post a new order. If it wasn't executed, it will let the order continue
                #for another timestamp
                order_state = 'pending'
                row_order = dataloc[line]
                order_id += 1

                distances_to_sides_to_added_liquidity = get_volume_differences(line, dataloc, delta_events)
                distances_to_imbalances = get_differences_imbalances(distances_to_sides_to_added_liquidity)
            
                target_ask_to_values = {
                        "price": row_order[6] * ( 1 + 0 / 10_000), #where we want to be executed
                        "order_id": order_id,
                        "timestamp_insertion": row_order[0], #timestamp
                        "best_bid_at_insertion": row_order[5], #bp0
                        "volume_over": 0,
                        "len_boucle" : 0,
                        "size_order" : mean_trade_size # rolling mean
                        }
                
                manip_bid_to_values = {
                    "price" : row_order[5] * (1 - manip_distance/10_000), 
                    "volume_under" : 0
                }
                

                for distance, imbalance in distances_to_imbalances.items():
                    #print(imbalance)
                    target_ask_to_values[f'var_imbalance_at_{distance}'] = round(imbalance,1) #this replaces the buckets
            
            
            line += 1
            # we are now at the first line of the timestamp of our order
            iterline = line
            if line >= length - 2 : break

            while dataloc[iterline + 1][0] == dataloc[line][0]: #same timestamp
                row_iter = dataloc[iterline]
                if  (row_iter[2] == 'T') :
                    if (row_iter[4]=='S') & (row_iter[1] >= target_ask_to_values['price']): 
                        # there is a trade at this timestamp that was over our target order in price.
                        target_ask_to_values['volume_over'] += row_iter[3] # we add the order's size.
                    if (row_iter[4] == 'B') & (row_iter[1] <= manip_bid_to_values['price']):
                        manip_bid_to_values['volume_under'] += row_iter[3]
            
                iterline +=1 
                if iterline + 1 >= length : break

            # we are now at the last line of our timestamp
            data_iter_line = dataloc[iterline]

            if (data_iter_line[5] >= target_ask_to_values['best_bid_at_insertion']) & (
                target_ask_to_values['volume_over'] == 0) & (manip_bid_to_values['volume_under'] == 0
            ): # if the best hasn't moved down and we weren't executed, the order is kept.
                line = iterline + 1
                target_ask_to_values['len_boucle'] +=1
                continue

            

            elif (data_iter_line[5] >= target_ask_to_values['best_bid_at_insertion']) & ((
                target_ask_to_values['volume_over'] != 0) | (manip_bid_to_values['volume_under'] != 0
            )):
                target_ask_to_values['ratio_exec'] = 1   #min(1 , target_ask_to_values['volume_over'] / mean_trade_size )
                target_ask_to_values['lifetime'] = data_iter_line[0] - target_ask_to_values['timestamp_insertion']
                tradecimetery_to_features_to_values3.append(target_ask_to_values)
                order_state = 'Executed'
                line = iterline + 1

            else : 
                target_ask_to_values['ratio_exec'] = 0   #min(1 , target_ask_to_values['volume_over'] / mean_trade_size )
                target_ask_to_values['lifetime'] = data_iter_line[0] - target_ask_to_values['timestamp_insertion']
                tradecimetery_to_features_to_values3.append(target_ask_to_values)
                order_state = 'Executed'
                line = iterline + 1
                

    print(f'calculated exec data *** {delta_events} delta_events  in {round(time()-t_start, 2)}s')        

    return tradecimetery_to_features_to_values3




def get_parallel_data_L_bid_L_ask():
    
    num_cores = multiprocessing.cpu_count()
    results = Parallel(n_jobs=num_cores)(delayed(get_L_bid_L_ask_data)(distance) for distance in constant.MANIP_DISTANCE_BPS_RANGE)
    manip_distance_to_orders_to_values_L_bid_L_ask = {constant.MANIP_DISTANCE_BPS_RANGE[i]: results[i] for i in range(len(constant.MANIP_DISTANCE_BPS_RANGE))}
    return manip_distance_to_orders_to_values_L_bid_L_ask





def get_all_probas_L_bid_L_ask(manip_distance_to_orders_to_values_L_bid_L_ask):
    #takes a dictionnary : {manip distance : [list of orders wih different sizes and the imbalance variation
    #plus other features.
    manip_distance_to_imbalance_level_to_values_to_ratios = {
        distance : {level : { value : []
                                         for value in constant.IMBALANCE_BUCKETISED_VALUES

        } for level in constant.IMBALANCE_DISTANCES_RANGE
        } for distance in constant.MANIP_DISTANCE_BPS_RANGE}
    
    manip_distance_to_imbalance_level_to_values_to_probas_L_bid_L_ask = {
        distance :{level : { value : 0
                                         for value in constant.IMBALANCE_BUCKETISED_VALUES

        } for level in constant.IMBALANCE_DISTANCES_RANGE
        } for distance in constant.MANIP_DISTANCE_BPS_RANGE}


    
    for (
        manip_distance, imbalance_level_to_values_to_ratios
     ) in manip_distance_to_imbalance_level_to_values_to_ratios.items():
        #print(type(manip_distance_to_orders_to_values_L_bid_L_ask))
        orders_to_values = manip_distance_to_orders_to_values_L_bid_L_ask[manip_distance]
        for order in orders_to_values: 
            for imbalance_level in constant.IMBALANCE_DISTANCES_RANGE :
                value = order[f'var_imbalance_at_{imbalance_level}']
                ratio = order['ratio_exec']
                imbalance_level_to_values_to_ratios[imbalance_level][value].append(ratio)
        

        for imbalance_level, values_to_ratios in imbalance_level_to_values_to_ratios.items():
            for value, ratios in values_to_ratios.items(): 
                total = len(ratios)
                manip_distance_to_imbalance_level_to_values_to_probas_L_bid_L_ask[manip_distance][imbalance_level][value] = sum(ratios) / total if total else 0
                
        #print(manip_distance_to_imbalance_level_to_values_to_probas_L_bid_L_ask[manip_distance])
 


    return manip_distance_to_imbalance_level_to_values_to_probas_L_bid_L_ask












################ IMPLEMENTING THE COST FUNCTION IN ITSELF : 


def get_volume_difference_local(line, data, distance):
        row = data[line]
        sides_to_added_liquidity = {'B':0 , 'S': 0} 
        iter_line = line 
        events_looked_at=0
        data_iter_line = row
        limit_order_memory = []
    
        while events_looked_at < constant.DELTA_EVENTS :
            iter_line -= 1
            if iter_line < 0 : 
                break
            events_looked_at += 1
            if (data_iter_line[2] != 'A') or data_iter_line[-1] == True : #order_type and marketable = False
                data_iter_line = data[iter_line]
                continue
            limit_order_memory.append(
                {'price' : data_iter_line[1], #eprice
                'side' : data_iter_line[4], #eside
                'volume': data_iter_line[3] #esize_ini
                })
            data_iter_line = data[iter_line]

            sides_to_prices_ini = {
            'B' : data_iter_line[5] *(1 - distance / 10_000) , 
            'S' : data_iter_line[6]* (1 + distance / 10_000) } 
                #best bid, best_ask

        for limit_order in limit_order_memory : 
            trade_side = limit_order['side']
            if ((trade_side == 'B') and (limit_order['price'] >= sides_to_prices_ini['B'])) or (
                (trade_side == 'S') and (limit_order['price'] <= sides_to_prices_ini['S'])) :
                sides_to_added_liquidity[trade_side] += limit_order['volume']

        return sides_to_added_liquidity


def get_volume_difference_local_plusone(line, data, distance):
        row = data[line]
        sides_to_added_liquidity = {'B':0 , 'S': 0} 
        iter_line = line 
        events_looked_at=0
        data_iter_line = row
        limit_order_memory = []
    
        while events_looked_at < constant.DELTA_EVENTS + 1 :
            iter_line -= 1
            if iter_line < 0 : 
                break
            events_looked_at += 1
            if (data_iter_line[2] != 'A') or data_iter_line[-1] == True : #order_type and marketable = False
                data_iter_line = data[iter_line]
                continue
            limit_order_memory.append(
                {'price' : data_iter_line[1], #eprice
                'side' : data_iter_line[4], #eside
                'volume': data_iter_line[3] #esize_ini
                })
            data_iter_line = data[iter_line]

            sides_to_prices_ini = {
            'B' : data_iter_line[5] *(1 - distance / 10_000) , 
            'S' : data_iter_line[6]* (1 + distance / 10_000) } 
                #best bid, best_ask

        for limit_order in limit_order_memory : 
            trade_side = limit_order['side']
            if ((trade_side == 'B') and (limit_order['price'] >= sides_to_prices_ini['B'])) or (
                (trade_side == 'S') and (limit_order['price'] <= sides_to_prices_ini['S'])) :
                sides_to_added_liquidity[trade_side] += limit_order['volume']

        return sides_to_added_liquidity



def get_difference_imbalance_local(sides_to_added_liquidity):
        
        imbalance = get_imbalance(
            sides_to_added_liquidity['B'],
            sides_to_added_liquidity['S'])
                                                                
        return imbalance

def get_sum_liquidity_local(sides_to_added_liquidity):
    return sides_to_added_liquidity['B'] + sides_to_added_liquidity['S']







def wealth_function(row, line, data, ats, 
                    dist_to_imb_to_expect_spined, 
                    manip_distance_to_multiple_to_imbalance_level_to_values_to_probas_L_bid,
                    distances_to_imbalances_to_probas_L_ask,
                    manip_distance_to_imbalance_level_to_values_to_probas_L_bid_L_ask
                   ) :
    # we call this cost function at a line/ row where 'etype' == 'A' and 'eside' == 'B', and line >=300
     

    #['etimestamp','eprice', 'etype', 'esize_ini', 'eside', 'bp0', 'ap0', 'is_from_marketable']
    epsi_plus = constant.EPSILON_PLUS
    epsi_moins = constant.EPSILON_MOINS
    Q = row[3]/ats
    price = row[1]
    bp0 = row[5]
    ap0 = row [6]



    ## Calculating the standardized values

    multiple_range = constant.MANIP_SIZE_MULTIPLE_RANGE
    multiple = min(multiple_range, key = lambda q : abs(q - Q))
    bps_dist = 10_000 * ( 1 - (price / bp0))
    if bps_dist < 0 : print(row)
    effect_dist = min(constant.MANIP_DISTANCE_BPS_RANGE, key = lambda dist : abs (dist - bps_dist))

    # We need to see which imbalance we consider
    if  (bps_dist >= constant.OPTIMAL_IMBALANCE_DISTANCE): 
        return None
    else : 
        imbalance_dist = constant.OPTIMAL_IMBALANCE_DISTANCE

    sides_to_added_liquidity_before = get_volume_difference_local(line, data, imbalance_dist)
    imbalance_before_raw = get_difference_imbalance_local(sides_to_added_liquidity_before)
    Qa_Qb =  get_sum_liquidity_local(sides_to_added_liquidity_before)
    imbalance_after_raw = get_difference_imbalance_local(get_volume_difference_local_plusone(line + 1, data, imbalance_dist))
    imbalance_after = round(imbalance_after_raw, 1) #this bucketises the imbalances

    proba_L_bid_less_h = manip_distance_to_multiple_to_imbalance_level_to_values_to_probas_L_bid[effect_dist][multiple][imbalance_dist][imbalance_after]
    proba_L_ask_less_h = distances_to_imbalances_to_probas_L_ask[imbalance_dist][imbalance_after]
    proba_L_bid_L_ask = manip_distance_to_imbalance_level_to_values_to_probas_L_bid_L_ask[effect_dist][imbalance_dist][imbalance_after]

    wealth_L_bid_less_h = (-Q * ats* (1 + epsi_plus) * price   +   (Q + 1) * ats * (1 - epsi_moins) * (price))

    wealth_L_ask_less_h = ats * ap0 * ( 1 + epsi_plus)
    wealth_L_ask_L_bid = ats * (1 - epsi_moins) * (bp0 + dist_to_imb_to_expect_spined[imbalance_dist][constant.IMBALANCE_BUCKETISED_VALUES.index(imbalance_after)])
    wealth_no_spoof = ats * bp0 * (1 - epsi_moins)
    wealth = proba_L_bid_less_h * wealth_L_bid_less_h +  proba_L_ask_less_h * wealth_L_ask_less_h + proba_L_bid_L_ask * wealth_L_ask_L_bid- wealth_no_spoof

    return wealth, imbalance_before_raw ,imbalance_after_raw, bps_dist, Qa_Qb, row[3]





def get_wealth_values(dist_to_imb_to_expect_spined,
                    manip_distance_to_multiple_to_imbalance_level_to_values_to_probas_L_bid,
                    distances_to_imbalances_to_probas_L_ask,
                    manip_distance_to_imbalance_level_to_values_to_probas_L_bid_L_ask): 

    
    timestamps = []
    wealths =[]
    imbalances_before = []
    distances = []
    Qa_Qb_liste = []
    order_volumes = []
    imbalances_after = []

    for name in constant.DATA_NAMES_SIMUL : 
        print(name)
        ats, dataloc = preprocess(name)
        dataloc = dataloc[constant.DELTA_EVENTS +1 : ]

        for line, row in enumerate(dataloc[ : -2]) : 

            if (row[4] =='B') & (row[2] == 'A') & (row[-1] != True): # An limit order is added on the bid side.
                
                try :
                    wealth, imbalance_before_raw, imbalance_after, bps_dist, Qa_Qb, order_volume = wealth_function(row, line, dataloc, ats, 
                        dist_to_imb_to_expect_spined, 
                        manip_distance_to_multiple_to_imbalance_level_to_values_to_probas_L_bid,
                        distances_to_imbalances_to_probas_L_ask,
                        manip_distance_to_imbalance_level_to_values_to_probas_L_bid_L_ask
                    )
                except : 
                    continue
                
                timestamps.append(row[0])
                wealths.append(wealth)
                imbalances_before.append(imbalance_before_raw)
                distances.append (bps_dist)
                Qa_Qb_liste.append(Qa_Qb)
                order_volumes.append(order_volume)
                imbalances_after.append(imbalance_after)

                    

    
    return timestamps, wealths, imbalances_before, imbalances_after, distances, Qa_Qb_liste, order_volumes










































