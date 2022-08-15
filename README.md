# CDF-TRIANGULAR-RESEARCH
import sys
import pandas as pd
import numpy as np
import os
from datetime import timedelta
import more_itertools as mit


#input_path = sys.argv[1] ; output_path = sys.argv[2]
input_path = r'C:\Users\finrix_workstation_1\Desktop\CFD RESEARCH\Inputs\Incoming_Inputs'
output_path = r'C:\Users\finrix_workstation_1\Desktop\CFD RESEARCH\Outputs\Summary_Outputs'
#r'C:\Users\finrix_workstation_1\Desktop\CFD RESEARCH\Inputs\Incoming_Inputs' r'C:\Users\finrix_workstation_1\Desktop\CFD RESEARCH\Outputs\Summary_Outputs'

if not os.path.isdir(output_path):
    os.makedirs(output_path)

#getting only the full rows

def get_full_rows(data_frame):
    data_new = data_frame.copy(); data_new.dropna(inplace = True) ; data_new.reset_index(drop=True,inplace=True)
    return data_new

#making input better

def input_array (input):
    input = input.replace('/' , '-')
    input = input.split()
    input = [s.replace('BUY', 'Ask') for s in input]; input = [s.replace('SELL', 'Bid') for s in input]
    input_array = np.array ([input[1], input[3], input[5], input[2], input[4], input[6]])

    return input_array

def input_array_reverse(input):

    input = input.replace('/','-')
    input = input.split()
    input = [s.replace('BUY', 'Bid') for s in input]; input = [s.replace('SELL', 'Ask') for s in input]
    input_array = np.array ([input[1], input[3], input[5], input[2], input[4], input[6]])

    return input_array


txt = open(r'C:\Users\finrix_workstation_1\Desktop\CFD RESEARCH\Trios-Names\Yağız_output_length_3_USD-USD.txt', 'r')
trio = list(map(lambda x: x.replace('/', '-'), txt.readlines()))

names = pd.read_csv(r'C:\Users\finrix_workstation_1\Desktop\CFD RESEARCH\Trios-Names\names.txt', names=['Names'])['Names']
names.drop(5, inplace = True)
names.drop(35, inplace = True)
names.reset_index(drop = True, inplace = True)

formula_max = [0.9999, 0.9999, 0.9999, 0.99, 0.99 ,0.99, .05, 0.05, 0.05 ]
formula_min = [0.9999, 0.99,0.05 ,0.9999, 0.99,0.05 ,0.9999, 0.99,0.05 ]

def combine_date_frames(string):
    empty = pd.DataFrame()

    for month in range(1, 10):

        for day in range(1, 10):
            if os.path.exists(r'%s\20220%s0%s-UnluFx-%s-Quote-MetaTrader.csv' %(input_path, month, day, string)):
                df = pd.read_csv(r'%s\20220%s0%s-UnluFx-%s-Quote-MetaTrader.csv' %(input_path,month, day, string))
                merged = empty.append(df, ignore_index=True)
                empty = merged

        for day in range(10, 32):
            if os.path.exists(r'%s\20220%s%s-UnluFx-%s-Quote-MetaTrader.csv' %(input_path, month, day, string)):
                df = pd.read_csv(r'%s\20220%s%s-UnluFx-%s-Quote-MetaTrader.csv' %(input_path, month, day, string))
                merged = empty.append(df, ignore_index=True)
                empty = merged

    merged.sort_values(by='Timestamp', inplace=True)
    merged.drop_duplicates(subset=['Timestamp'], keep='last', ignore_index=True, inplace=True)

    merged.to_csv(r'C:\Users\finrix_workstation_1\Desktop\CFD RESEARCH\Inputs\Created_Inputs/%s.csv' % string, index=False)

    return

"""for name in names:
   combine_date_frames(name)"""


def read_data_three_variables(data_1_name, data_2_name, data_3_name, ask_bid_1, ask_bid_2, ask_bid_3, date_name='Timestamp', date='20220701', market='MetaTrader'):
    data_1 = pd.read_csv(r'C:\Users\finrix_workstation_1\Desktop\CFD RESEARCH\Inputs\Created_Inputs\%s.csv' %(data_1_name))
    data_2 = pd.read_csv(r'C:\Users\finrix_workstation_1\Desktop\CFD RESEARCH\Inputs\Created_Inputs\%s.csv' %(data_2_name))
    data_3 = pd.read_csv(r'C:\Users\finrix_workstation_1\Desktop\CFD RESEARCH\Inputs\Created_Inputs\%s.csv' %(data_3_name))

    data_2 = data_2[data_2['Timestamp'] <= 10000000000000]
    data_1 = data_1[data_1['Timestamp'] <= 10000000000000]
    data_3 = data_3[data_3['Timestamp'] <= 10000000000000]

    data_1_date_time = pd.to_datetime(data_1[date_name], unit='ms')
    data_2_date_time = pd.to_datetime(data_2[date_name], unit='ms')
    data_3_date_time = pd.to_datetime(data_3[date_name], unit='ms')

    data_1_close = data_1['Best%sPrice' % ask_bid_1].astype(float)
    data_2_close = data_2['Best%sPrice' % ask_bid_2].astype(float)
    data_3_close = data_3['Best%sPrice' % ask_bid_3].astype(float)

    data_1_final = pd.DataFrame({'Tarih_Saat': data_1_date_time, 'Kapanış %s' % data_1_name: data_1_close})
    data_2_final = pd.DataFrame({'Tarih_Saat': data_2_date_time, 'Kapanış %s' % data_2_name: data_2_close})
    data_3_final = pd.DataFrame({'Tarih_Saat': data_3_date_time, 'Kapanış %s' % data_3_name: data_3_close})

    return data_1_final, data_2_final, data_3_final


def merge_sort_duplicate_three_variables(data_frame_1, data_frame_2, data_frame_3):
    merged = pd.merge(data_frame_1, data_frame_2, how='outer', on=['Tarih_Saat'])
    merged_2 = pd.merge(merged, data_frame_3, how='outer', on=['Tarih_Saat'])

    df = merged_2.sort_values("Tarih_Saat", ignore_index=True)

    final = df.drop_duplicates(subset=['Tarih_Saat'], keep='first', ignore_index=True)

    return final


def handle_missing_points(data_frame, variable_name_1, variable_name_2, variable_name_3):
    df = data_frame.copy()

    df['Kapanış %s' % variable_name_1] = df['Kapanış %s' % variable_name_1].fillna(method='ffill')
    df['Kapanış %s' % variable_name_2] = df['Kapanış %s' % variable_name_2].fillna(method='ffill')
    df['Kapanış %s' % variable_name_3] = df['Kapanış %s' % variable_name_3].fillna(method='ffill')

    return df


def find_extrema_by_threshold(day_data_frame, min, max, day_name="Tarih_Saat", value_name='Formula'):

    temp = day_data_frame[((day_data_frame['Formula'] >= day_data_frame['Formula'].quantile(min)) | (day_data_frame['Formula'] >= day_data_frame['Formula'].quantile(max)))]
    min_value = temp[temp['Formula'] >= day_data_frame['Formula'].quantile(min)]['Formula']
    max_value = temp[temp['Formula'] >= day_data_frame['Formula'].quantile(max)]['Formula']

    data = pd.DataFrame({'Tarih_Saat': temp[day_name], 'Max': max_value, 'Min': min_value})

    temp_2 = pd.merge(day_data_frame, data, on='Tarih_Saat', how='outer')

    temp_bigger = day_data_frame[((day_data_frame['Formula'] >= day_data_frame['Formula'].quantile(min)) | (day_data_frame['Formula'] >= day_data_frame['Formula'].quantile(max)))]

    min_value_bigger = temp_bigger[temp_bigger['Formula'] >= day_data_frame['Formula'].quantile(min)]['Formula']
    max_value_bigger = temp_bigger[temp_bigger['Formula'] >= day_data_frame['Formula'].quantile(max)]['Formula']

    data_bigger = pd.DataFrame({'Tarih_Saat': temp_bigger[day_name], 'Max': max_value_bigger, 'Min': min_value_bigger})

    df_1 = temp_2.sort_values('Tarih_Saat', ignore_index=True).drop_duplicates(subset=['Tarih_Saat'], keep='first',
                                                                               ignore_index=True)
    df_2 = data.sort_values('Tarih_Saat', ignore_index=True).drop_duplicates(subset=['Tarih_Saat'], keep='first',
                                                                             ignore_index=True)

    df_3 = data_bigger.sort_values('Tarih_Saat', ignore_index=True).drop_duplicates(subset=['Tarih_Saat'], keep='first',
                                                                                    ignore_index=True)

    df_3 = (df_3.dropna(how='all', subset=['Max', 'Min'])).reset_index(drop=True)

    return df_1, df_2, df_3


def preprocess(data_1_name, data_2_name, data_3_name, ask_bid_1, ask_bid_2, ask_bid_3, date_name='Timestamp',
               date='20220701', market='MetaTrader'):
    temp = read_data_three_variables(data_1_name, data_2_name, data_3_name, ask_bid_1, ask_bid_2, ask_bid_3, date_name,
                                     date, market)

    temp_1 = merge_sort_duplicate_three_variables(temp[0], temp[1], temp[2])
    temp_4 = handle_missing_points(temp_1, data_1_name, data_2_name, data_3_name)

    return get_full_rows(temp_4)


def frame_after_formula_threshold(data_frame, variable_name_1, variable_name_2, variable_name_3, ask_bid_1, ask_bid_2,
                                  ask_bid_3, min, max):
    if ask_bid_1 == 'Bid':
        formula_1 = 1 * data_frame['Kapanış %s' % variable_name_1]

    if ask_bid_1 == 'Ask':
        formula_1 = 1 / data_frame['Kapanış %s' % variable_name_1]

    if ask_bid_2 == 'Bid':
        formula_2 = 1 * data_frame['Kapanış %s' % variable_name_2]

    if ask_bid_2 == 'Ask':
        formula_2 = 1 / data_frame['Kapanış %s' % variable_name_2]

    if ask_bid_3 == 'Bid':
        formula_3 = 1 * data_frame['Kapanış %s' % variable_name_3]

    if ask_bid_3 == 'Ask':
        formula_3 = 1 / data_frame['Kapanış %s' % variable_name_3]

    formula = formula_1 * formula_2 * formula_3 - 1.000

    temp_1 = pd.DataFrame({'Tarih_Saat': data_frame['Tarih_Saat'], 'Formula': formula})

    temp_2 = find_extrema_by_threshold(temp_1, min, max)

    return temp_2

def zigzag_frame(frame):
    extrema_frame = frame.copy()

    extrema_frame['Max shift'] = extrema_frame['Max'].shift(1)
    extrema_frame['Min shift'] = extrema_frame['Min'].shift(1)

    max_empty = np.isnan(extrema_frame['Max'])
    max_full = pd.notna(extrema_frame['Max'])

    min_empty = np.isnan(extrema_frame['Min'])
    min_full = pd.notna(extrema_frame['Min'])

    max_shift_empty = np.isnan(extrema_frame['Max shift'])
    max_shift_full = pd.notna(extrema_frame['Max shift'])

    min_shift_empty = np.isnan(extrema_frame['Min shift'])
    min_shift_full = pd.notna(extrema_frame['Min shift'])

    extrema_frame.loc[((max_empty & min_full) | (max_full & min_empty)) &
                      ((max_empty & max_shift_empty) | (max_full & max_shift_full)) &
                      ((min_empty & min_shift_empty) | (min_full & min_shift_full))
    , 'False'] = 'False'

    extrema_frame = extrema_frame[extrema_frame['False'] != ('False')].reset_index(drop=True)

    time_max = []
    max = []
    time_min = []
    min = []

    if len(extrema_frame) != 0:
        if pd.notna(extrema_frame['Max'][0]):
            max.append(extrema_frame['Max'][0])
            time_max.append(extrema_frame['Tarih_Saat'][0])

    i = 0
    while i < len(extrema_frame):

        if pd.notna(extrema_frame['Min'][i]):

            time_min.append(extrema_frame['Tarih_Saat'][i])
            min.append(extrema_frame['Min'][i])

            while ((pd.notna(extrema_frame['Min'][i])) and (np.isnan(extrema_frame['Max'][i]))) and i < (
                    len(extrema_frame) - 1):
                i += 1

            if pd.notna(extrema_frame['Max'][i]):
                time_max.append(extrema_frame['Tarih_Saat'][i])
                max.append(extrema_frame['Max'][i])

        i += 1

    zig = pd.DataFrame({'Tarih_Saat': time_max, 'Max': max})
    zag = pd.DataFrame({'Tarih_Saat': time_min, 'Min': min})

    zig['Tarih_Saat'] = pd.to_datetime(zig['Tarih_Saat'])
    zag['Tarih_Saat'] = pd.to_datetime(zag['Tarih_Saat'])

    return zig, zag


def hit_ratio_combined(string, min_, max_):
    array = input_array(string)
    array_reverse = input_array_reverse(string)

    if ((array[3] == 'Bid') + (array[4] == 'Bid') + (array[5] == 'Bid')) == 2 or (
            (array[3] == 'Bid') + (array[4] == 'Bid') + (array[5] == 'Bid')) == 3:

        f_preprocess = preprocess(*array)
        f_reverse_preprocess = preprocess(*array_reverse)

        bid_array = array
        ask_array = array_reverse

    if ((array[3] == 'Bid') + (array[4] == 'Bid') + (array[5] == 'Bid')) == 0 or (
            (array[3] == 'Bid') + (array[4] == 'Bid') + (array[5] == 'Bid')) == 1:
        f_preprocess = preprocess(*array_reverse); f_reverse_preprocess = preprocess(*array)

        bid_array = array_reverse; ask_array = array
    f_preprocess = f_preprocess.loc[(f_preprocess['Tarih_Saat'].dt.day_name()!=('Sunday')) & (f_preprocess['Tarih_Saat'].dt.day_name()!=('Saturday'))]
    f_reverse_preprocess = f_reverse_preprocess.loc[(f_reverse_preprocess['Tarih_Saat'].dt.day_name() != ('Sunday')) & (
                f_reverse_preprocess['Tarih_Saat'].dt.day_name() != ('Saturday'))]

    f_reverse_preprocess.reset_index(drop = True, inplace=True);f_preprocess.reset_index(drop = True, inplace = True)

    bid_bid_formula = frame_after_formula_threshold(f_preprocess, *bid_array, min_, max_)
    ask_ask_formula = frame_after_formula_threshold(f_reverse_preprocess, *ask_array, min_, max_)

    # zig_bid_zag_ask
    df_f_0 = bid_bid_formula[0]
    df_b_0 = ask_ask_formula[0]

    df_f_0.drop('Min', inplace=True, axis=1)
    df_b_0.drop('Max', inplace=True, axis=1)
    df_f_0 = df_f_0[df_f_0['Max'].notna()]
    df_b_0 = df_b_0[df_b_0['Min'].notna()]

    merged_1 = pd.merge(df_f_0, df_b_0, how='outer', on=['Tarih_Saat'])

    df_zig_bid_zag_ask = (merged_1.sort_values("Tarih_Saat", ignore_index=True)).drop_duplicates(subset=['Tarih_Saat'],
                                                                                                 keep='first',
                                                                                                 ignore_index=True)

    f_zig_bid_zag_ask = zigzag_frame(df_zig_bid_zag_ask)

    # bid_minus_ask_down_frame
    zig = f_zig_bid_zag_ask[0]
    zag = f_zig_bid_zag_ask[1]

    max = f_preprocess
    min = f_reverse_preprocess
    max = max.rename(columns={'Kapanış %s' % bid_array[0]: '%s_zig_1' % bid_array[3],
                              'Kapanış %s' % bid_array[1]: '%s_zig_2' % bid_array[4],
                              'Kapanış %s' % bid_array[2]: '%s_zig_3' % bid_array[5]})
    min = min.rename(columns={'Kapanış %s' % ask_array[0]: '%s_zag_1' % ask_array[3],
                              'Kapanış %s' % ask_array[1]: '%s_zag_2' % ask_array[4],
                              'Kapanış %s' % ask_array[2]: '%s_zag_3' % ask_array[5]})

    df_max = pd.merge(max, zig, how='inner', on=['Tarih_Saat'])
    df_min = pd.merge(min, zag, how='inner', on=['Tarih_Saat'])

    if (len(df_max)) != 0 and (len(df_min)) != 0:

        while df_max['Tarih_Saat'][0] > df_min['Tarih_Saat'][0]:

            df_min.drop(index=df_min.index[0],
                        axis=0,
                        inplace=True)
            df_min = df_min.reset_index(drop=True)

            if (len(df_min)) == 0:
                break

    bid_ask_1, bid_ask_2, bid_ask_3 = bid_array[3:6]

    max_1, max_2, max_3 = df_max.columns[1:4]
    min_1, min_2, min_3 = df_min.columns[1:4]

    if max_1 == 'Ask_zig_1':
        bid_ask_1 = df_min[min_1] - df_max[max_1]
        bid_ask_2 = df_max[max_2] - df_min[min_2]
        bid_ask_3 = df_max[max_3] - df_min[min_3]

    if max_2 == 'Ask_zig_2':
        bid_ask_1 = df_max[max_1] - df_min[min_1]
        bid_ask_2 = df_min[min_2] - df_max[max_2]
        bid_ask_3 = df_max[max_3] - df_min[min_3]

    if max_3 == 'Ask_zig_3':
        bid_ask_1 = df_max[max_1] - df_min[min_1]
        bid_ask_2 = df_max[max_2] - df_min[min_2]
        bid_ask_3 = df_min[min_3] - df_max[max_3]

    df = pd.DataFrame({'Max_Giriş': df_max['Tarih_Saat'],  # .dt.time,
                       'Min_Çıkış': df_min['Tarih_Saat'],  # .dt.time,
                       'Formula_max': df_max['Max'],
                       'Formula_min': df_min['Min'],
                       max_1: df_max[max_1],
                       min_1: df_min[min_1],
                       'Bid_1 - Ask_1': bid_ask_1,
                       max_2: df_max[max_2],
                       min_2: df_min[min_2],
                       'Bid_2 - Ask_2': bid_ask_2,

                       max_3: df_max[max_3],
                       min_3: df_min[min_3],
                       'Bid_3 - Ask_3': bid_ask_3,

                       })

    final = get_full_rows(df)

    f_bid_minus_ask_down_frame = final.drop_duplicates(subset=['Max_Giriş'], keep='first', ignore_index=True)

    # formula_for_bid_ask

    date_ = f_preprocess['Tarih_Saat']

    if array[0][0:3] == array[1][0:3]:

        name = array[0][0:4] + array[2][0:3]

        bid_ask_1 = f_preprocess['Kapanış %s' % name]
        bid_ask_2 = f_reverse_preprocess['Kapanış %s' % name]

        if name == array[0]:
            name_1 = bid_array[3]
            name_2 = ask_array[3]
        if name == array[1]:
            name_1 = bid_array[4]
            name_2 = ask_array[4]

        frame_x = pd.DataFrame({'Max_Giriş': date_, name_1: bid_ask_1, name_2: bid_ask_2})

        merged = pd.merge(f_bid_minus_ask_down_frame['Max_Giriş'], frame_x, how='inner', on=['Max_Giriş'])
        df = pd.merge(merged, f_bid_minus_ask_down_frame, how='outer').sort_values('Max_Giriş', ignore_index=True)

        lot_3 = (df['Ask'] + df['Bid']) / 2
        lot_1 = 1
        lot_2 = 1

        kar_1 = lot_1 * df['Bid_1 - Ask_1']
        kar_2 = lot_2 * df['Bid_2 - Ask_2']
        kar_3 = lot_3 * df['Bid_3 - Ask_3']

        formula = df['Bid_1 - Ask_1'] + df['Bid_2 - Ask_2'] + df['Bid_3 - Ask_3'] * (df['Ask'] + df['Bid']) / 2

    if array[0][0:3] == array[2][0:3]:

        name = array[0][0:4] + array[1][0:3]

        bid_ask_1 = f_preprocess['Kapanış %s' % name]
        bid_ask_2 = f_reverse_preprocess['Kapanış %s' % name]

        if name == array[0]:
            name_1 = bid_array[3]
            name_2 = ask_array[3]

        if name == array[2]:
            name_1 = bid_array[5]
            name_2 = ask_array[5]

        frame_x = pd.DataFrame({'Max_Giriş': date_, name_1: bid_ask_1, name_2: bid_ask_2})

        merged = pd.merge(f_bid_minus_ask_down_frame['Max_Giriş'], frame_x, how='inner', on=['Max_Giriş'])
        df = pd.merge(merged, f_bid_minus_ask_down_frame, how='outer').sort_values('Max_Giriş', ignore_index=True)

        lot_2 = (df['Ask'] + df['Bid']) / 2

        lot_1 = 1
        lot_3 = 1

        kar_1 = lot_1 * df['Bid_1 - Ask_1']
        kar_2 = lot_2 * df['Bid_2 - Ask_2']
        kar_3 = lot_3 * df['Bid_3 - Ask_3']

        formula = df['Bid_1 - Ask_1'] + df['Bid_3 - Ask_3'] + df['Bid_2 - Ask_2'] * (df['Ask'] + df['Bid']) / 2

    if array[1][0:3] == array[2][0:3]:

        name = array[1][0:4] + array[0][0:3]

        bid_ask_1 = f_preprocess['Kapanış %s' % name]
        bid_ask_2 = f_reverse_preprocess['Kapanış %s' % name]

        if name == array[1]:
            name_1 = bid_array[4]
            name_2 = ask_array[4]
        if name == array[2]:
            name_1 = bid_array[5]
            name_2 = ask_array[5]

        frame_x = pd.DataFrame({'Max_Giriş': date_, name_1: bid_ask_1, name_2: bid_ask_2})

        merged = pd.merge(f_bid_minus_ask_down_frame['Max_Giriş'], frame_x, how='inner', on=['Max_Giriş'])
        df = pd.merge(merged, f_bid_minus_ask_down_frame, how='outer').sort_values('Max_Giriş', ignore_index=True)

        lot_1 = (df['Ask'] + df['Bid']) / 2
        lot_2 = 1
        lot_3 = 1
        kar_1 = lot_1 * df['Bid_1 - Ask_1']
        kar_2 = lot_2 * df['Bid_2 - Ask_2']
        kar_3 = lot_3 * df['Bid_3 - Ask_3']

    # df['Min_Çıkış'] = pd.to_datetime(df['Min_Çıkış'], format = '%H:%M:%S.%f')
    # df['Max_Giriş'] = pd.to_datetime(df['Max_Giriş'], format = '%H:%M:%S.%f')

    duration = (df['Min_Çıkış'] - df['Max_Giriş']).dt.total_seconds()
    kar_1 = kar_1 * 1000
    kar_2 = kar_2 * 1000
    kar_3 = kar_3 * 1000
    kar = kar_1 + kar_2 + kar_3

    final = pd.DataFrame({'Max_Giriş': df['Max_Giriş'], 'Min_Çıkış': df['Min_Çıkış'], 'Duration (seconds)': duration,
                          'Lot 1': lot_1, 'Lot 2': lot_2, 'Lot 3': lot_3,
                          'Kar 1': kar_1, 'Kar 2': kar_2, 'Kar 3': kar_3,
                          'Kar': kar})

    f_formula_for_bid_ask = final.drop_duplicates(subset=['Max_Giriş'], keep='first', ignore_index=True)

    # repeat threshold
    zig_zag_frame = f_zig_bid_zag_ask

    max_frame = bid_bid_formula[2].drop(['Min'], axis=1).dropna(subset=['Max']).reset_index(drop=True)
    min_frame = ask_ask_formula[2].drop(['Max'], axis=1).dropna(subset=['Min']).reset_index(drop=True)

    (zig_zag_frame[0]).rename(columns={'Max': 'Zig'}, inplace=True)
    (zig_zag_frame[1]).rename(columns={'Min': 'Zag'}, inplace=True)

    temp_max = pd.merge(max_frame, zig_zag_frame[0], on='Tarih_Saat', how='outer')
    temp_min = pd.merge(min_frame, zig_zag_frame[1], on='Tarih_Saat', how='outer')

    min_index = temp_min[pd.isna(temp_min['Zag'])].index
    max_index = temp_max[pd.isna(temp_max['Zig'])].index

    min_group = [list(group) for group in mit.consecutive_groups(min_index)]
    max_group = [list(group) for group in mit.consecutive_groups(max_index)]

    max_list = []; min_list = []

    for item in max_group:
        max_list.append(len(item))

    for item in min_group:
        min_list.append(len(item))

    # hit, final
    position_numbers = f_zig_bid_zag_ask
    formula = f_formula_for_bid_ask
    repeat = max_list, min_list
    dates = date_
    time = dates[len(dates) - 1] - dates[0]
    dead_time = timedelta(0)

    for i in range(len(dates) - 1):
        if dates[i + 1] - dates[i] > timedelta(minutes=2):
            dead_time += dates[i + 1] - dates[i]

    end_time = time - dead_time

    hourly = (len(formula) / end_time.total_seconds()) * 3600


    if len(formula) == 0:
        pm = 0
    else:
        pm = (len(formula.query('Kar > 0')) / + len(formula))

    zig_mean = np.nan; zig_max = np.nan; zag_mean = np.nan; zag_max = np.nan

    if len(repeat[0]) != 0:
        zig_mean = np.mean(repeat[0])
        zig_max = np.max(repeat[0])

    if len(repeat[1]) != 0:
        zag_mean = np.mean(repeat[1])
        zag_max = np.max(repeat[1])

    df_final_2 = pd.DataFrame({'Formula_max': float(bid_bid_formula[0]['Formula'].quantile(max_)), 'Formula_min': float(ask_ask_formula[0]['Formula'].quantile(min_)),
                               'Zig Baslangic Sayisi': len(position_numbers[0]),
                               'Zag Baslangic Sayisi': len(position_numbers[1]),
                               'Toplam Islem Sayisi': len(position_numbers[0]) + len(position_numbers[1]),
                               'Ortalama Zig-Zag Suresi': formula['Duration (seconds)'].mean(),
                               'Maximum Zig-Zag Suresi': formula['Duration (seconds)'].max(),
                               'Saatlik Ortalama Zig-Zag Sayisi': hourly,
                               'Pozitif Karli Islem Sayisi': len(formula.query('Kar > 0')),
                               'Negatif Karli Islem Sayisi': len(formula.query('Kar < 0')), 'Hit Ratio': pm,
                               'Zig Ortalama Tekrar Sayisi': [zig_mean], 'Zig Maximum Tekrar Sayisi': [zig_max],
                               'Zag Ortalama Tekrar Sayisi': [zag_mean], 'Zag Maximum Tekrar Sayisi': [zag_max],
                               'Kar 1 Ortalama': [formula['Kar 1'].mean()], 'Kar 1 Toplam': [formula['Kar 1'].sum()],

                               'Kar 2 Ortalama': [formula['Kar 2'].mean()], 'Kar 2 Toplam': [formula['Kar 2'].sum()],
                               'Kar 3 Ortalama': [formula['Kar 3'].mean()], 'Kar 3 Toplam': [formula['Kar 3'].sum()],
                               'Kar Ortalama': [formula['Kar'].mean()], 'Kar Toplam': [formula['Kar'].sum()]
                               })

    return df_final_2





for name in trio:
    try:
      temp = hit_ratio_combined(name, formula_min[0], formula_max[0])
      for i in range(1, len(formula_max)):
        merged = temp.append(hit_ratio_combined(name, formula_min[i], formula_max[i]))
        temp = merged

      array = input_array(name)
      temp.to_csv(r'%s\%s %s %s.csv' % (output_path, array[0], array[1], array[2]), decimal='.', sep=',', index=False)
      
    except:
      print('Error in %s ' %name)


