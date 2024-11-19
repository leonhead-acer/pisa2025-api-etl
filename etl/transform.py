# Import modules
import pandas as pd
pd.options.mode.chained_assignment = None
import re
import numpy as np
import datetime
import json

# Transform Crash Data
def transform_crash_data(crashes_df):
    crashes_df['CRASH_DATE'] = pd.to_datetime(crashes_df['CRASH_DATE'])
    crashes_df = crashes_df[crashes_df['CRASH_DATE_EST_I'] != 'Y']
    crashes_df = crashes_df[crashes_df['LATITUDE'].notnull() & crashes_df['LONGITUDE'].notnull()]
    crashes_df = crashes_df.drop(columns=['CRASH_DATE_EST_I'])
    return crashes_df

# Transform Vehicle Data
def transform_vehicle_data(vehicles_df):
    vehicles_df['VEHICLE_MAKE'] = vehicles_df['VEHICLE_MAKE'].str.upper()
    vehicles_df['VEHICLE_MODEL'] = vehicles_df['VEHICLE_MODEL'].str.upper()
    vehicles_df = vehicles_df[vehicles_df['VEHICLE_YEAR'].notnull()]
    return vehicles_df

# Transform People Data
def transform_people_data(people_df):
    people_df = people_df[people_df['PERSON_TYPE'].isin(['DRIVER', 'PASSENGER', 'PEDESTRIAN', 'BICYCLE', 'OTHER'])]
    people_df = people_df[people_df['PERSON_AGE'].notnull()]
    return people_df

# Convert json file to pandas df

def json_to_pd (filepath: str) -> pd.DataFrame:

    """
       Simple Extract Function in Python with Error Handling
       :param filepath: str, file path to JSON data
       :output: pandas dataframe, extracted from JSON data
    """
    try:
        # Read the JSON file and store it in a dataframe
        df = pd.read_json(
            filepath,
            orient = 'records'
        )

    # Handle exception if any of the files are missing
    except FileNotFoundError as e:
        print(f"File not found: {e}")

    # Handle any other exceptions
    except Exception as e:
        print(f"Error: {e}")

    return df

# Extract 'raw_data' from json

def explode_raw_data (df: object) -> pd.DataFrame:
    
    json_dat = pd.json_normalize(df['raw_data'],max_level = 0)
    json_dat.index = df.index
    df1 = (
        df
        .drop(columns = ['raw_data']).join(
            json_dat.drop(['login','last_update_date'],axis = 1)
        )
    )

    return df1

def explode_items(df: object) -> pd.DataFrame:
    df1 = pd.concat([df.drop(['items'],axis = 1),df['items'].apply(pd.Series)],axis = 1)
    all_vars = set(df.columns) - set(['items'])

    id_vars = [
        'login',
        'last_update_date',
        'test_qti_id',
        'sessionStartTime',
        'sessionEndTime',
        'language'
    ]

    value_vars = df1.drop(columns = all_vars,axis = 1).columns

    df1 = df1.set_index(id_vars)

    df2 = df1.melt(value_vars=value_vars,var_name = 'itemId',value_name = 'value',ignore_index = False).dropna().reset_index()

    return df2

def explode_values(df: object) -> pd.DataFrame:

    df1 = df.join(
        pd.json_normalize(df['value'],max_level = 0).set_index(df.index)
    )

    return df1

def rename_variables(df: object, domain: str) -> pd.DataFrame:
    if(domain == 'FLA'):
        rename_dict = {
            'qtiId': 'unit_id',
            'test_qti_id': 'testQtiLabel',
            'completionStatus': 'item_completionStatus'
        }
    elif(domain == 'SCI'):
        rename_dict = {
            'test_qti_id': 'testQtiLabel',
            'completionStatus': 'item_completionStatus'
        }

    df1 = df.drop('value',axis = 'columns').rename(
        columns = rename_dict
    )

    return df1

def check_duplicates(df: object) -> pd.DataFrame:
    df_cnt = (
        df[['login','itemId']]
        .groupby(by = ['login','itemId'])
        .size()
        .to_frame(name = 'count')
        .reset_index()
        .query("count > 1")
    )

    if(df_cnt.shape[0] > 0):
        df_cnt = df_cnt.drop(columns = ['count']).merge(df[['login','itemId','itemStartTime']],on = ['login','itemId'],how='outer')
        df_cnt['rank'] = df_cnt.groupby(['login','itemId'])['sessionEndTime'].rank(method = 'dense',ascending = False)
        df_cnt = df_cnt.loc[df_cnt['rank'] > 1,['login','itemId']]
        df_cnt['dup_std_item'] = 'duplicate'

        df = df.merge(
            df_cnt,
            on = ['login','itemId'],
            how = 'left'
        )

        df['dup_std_item'] = df['dup_std_item'].fillna('')

    else:
        df['dup_std_item'] = ''

    return df

def replace_blank_json(df: object) -> pd.DataFrame:
    invalid_cell = [(isinstance(x,list)) & (len(x)==0) for x in df['responses']]

    df.loc[invalid_cell,['responses']] = {}

    return df

def dict_to_df(x):
    if(pd.notnull(x)):
        df = pd.DataFrame(list(x.values()),index = x.keys()).reset_index().rename(columns = {'index': 'resp_cat','value': 'db_resp','correct':'db_correct'})
    else:
        df = pd.DataFrame(np.nan,index = [0],columns = ['resp_cat','db_resp','db_correct'])
        # df = pd.DataFrame({'date': k, **v} for d in x for k, v in d.items())

    return df

def explode_responses(df: object, domain: str) -> pd.DataFrame:
    
    id_vars = ['login', 'last_update_date',
        'testQtiLabel', 'sessionStartTime', 'sessionEndTime', 'language','unit_id',
        'qtiLabel','itemId',
        'score', 'duration', 'maxScore', 
        'itemEndTime', 'numAttempts', 'itemStartTime',
        'statusCorrect', 'submissionTime', 'item_completionStatus',
        'dup_std_item','resp_cat','db_resp','db_correct']

    if(domain == 'FLA'):
        id_vars.remove('qtiLabel')
    elif(domain == 'SCI'):
        id_vars.remove('unit_id')

    funct = np.vectorize(dict_to_df)
    x = funct(df.responses)
    index = df.index.values.tolist()
    x_concat = pd.concat(x,axis = 0,keys = index,names = ['joinindex','indindex']).reset_index(level = 1,drop = True)
    df.index.name = 'joinindex'
    df1 = pd.concat([df.drop(columns = ['responses']),x_concat],axis = 1).loc[:,id_vars].reset_index().drop(columns = ['joinindex'])
    df1['resp_cat'] = df1['resp_cat'].apply(lambda x: re.sub('.*(?=RESPONSE)','',x) if pd.notnull(x) else x)

    return df1

def fla_recode_FLALDTB1002(df: object) -> pd.DataFrame:
    recode_list = [
        'FLAL02.*item-19',
        'FLAL05.*item-10',
        'FLAL07.*item-7',
        'FLAL08.*item-3',
        'FLAL09.*item-16',
        'FLAL14.*item-22',
    ]

    for f in recode_list:

        recode_mask = df['itemId'].str.contains(f)

        df.loc[recode_mask,['unit_id']] = 'FLALDTB1002'

    return df

def gap_to_dict(x):
    gaps = re.findall(r'(gap_\d)+',x)
    if(len(gaps) > 0):
        choices = re.findall(r'(choice_\d)+',x)
        dict_gap = dict(zip(gaps,choices))
    else:
        dict_gap = {}

    gap_all = ['gap_' + str(i+1) for i in range(0,5)]
    gap_null = set(gap_all) - set(gaps)

    if(len(gap_null) > 0):
        for g in gap_null:
            dict_gap[g] = np.NaN

    df = pd.DataFrame(pd.Series(dict_gap)).reset_index()
    df.columns = ['resp_cat','db_resp']
    return(df)

def gap_recode(df: object, cbk: object) -> pd.DataFrame:

    gap_vars = cbk.loc[cbk['resp_cat'].str.contains("gap",na=False),:].unit_id.unique()

    split_exp = df['unit_id'].str.contains("|".join(gap_vars),na = False)

    tmp1 = df[split_exp]
    tmp2 = df[~split_exp]

    tmp1['db_resp'] = tmp1['db_resp'].fillna('[]')
    x = tmp1['db_resp'].apply(gap_to_dict).tolist()
    index = tmp1.index.values.tolist()
    x_concat = pd.concat(x,axis = 0,keys = index,names = ['joinindex','indindex']).reset_index(level = 1,drop = True)
    tmp1.index.name = 'joinindex'
    tmp4 = tmp1.drop(columns = ['resp_cat','db_resp']).join(x_concat).reset_index()

    df_tmp = pd.concat(
        [
            tmp2,
            tmp4
        ],
        axis = 0
    ).reset_index().drop(columns = ['joinindex'])

    del df
    df = df_tmp
    print(f"Total new rows: {df.shape[0]}")
    print(f"Estimate of new rows: {(tmp1.shape[0]*5) + tmp2.shape[0]}")

    return df

def rmmb_to_dict(x):
    qti_all = ['FLARMMB200101', 'FLARMMB200102', 'FLARMMB200103', 'FLARMMB200104',
        'FLARMMB200105', 'FLARMMB200106','FLARMMB200107', 'FLARMMB200108', 'FLARMMB200109',
        'FLARMMB200110']

    resp_all = ['RESPONSE']
    for i in range(0,9):
        resp_all.append('RESPONSE_' + str(i+1))

    dict_resp = dict(zip(qti_all,resp_all))

    string = pd.Series([pd.DataFrame(j) for j in json.loads(x)][2].base[0]).to_dict()

    for qti in qti_all:
        if qti not in string.keys():
            string[qti] = np.NaN

    string = dict((dict_resp[key],value) for (key, value) in string.items())

    df = pd.DataFrame(pd.Series(string)).reset_index()
    df.columns = ['resp_cat','db_resp']
    return(df)

def rmmb_recode(df: object, cbk: object) -> pd.DataFrame:

    new_vars = cbk.loc[cbk['qtiLabel2'].str.startswith("RMMB2",na=False),:].unit_id.unique()

    split_exp = (df['unit_id'].str.contains("|".join(new_vars),na = False)) & (df['db_resp'].str.contains("string"))
    split_keep = (~df['unit_id'].str.contains("|".join(new_vars),na = False))

    if(any(split_exp)):
        tmp1 = df[split_exp]
        tmp2 = df[split_keep]

        tmp1['db_resp'] = tmp1['db_resp'].fillna('[]')
        x = tmp1['db_resp'].apply(rmmb_to_dict).tolist()
        index = tmp1.index.values.tolist()
        x_concat = pd.concat(x,axis = 0,keys = index,names = ['joinindex','indindex']).reset_index(level = 1,drop = True)
        tmp1.index.name = 'joinindex'
        tmp4 = tmp1.drop(columns = ['resp_cat','db_resp']).join(x_concat).reset_index()

        df_tmp = pd.concat(
            [
                tmp2,
                tmp4
            ],
            axis = 0
        ).reset_index().drop(columns = ['joinindex'])

        del df
        df = df_tmp
        print(f"Total new rows: {df.shape[0]}")
        print(f"Estimate of new rows: {(tmp1.shape[0]*10) + tmp2.shape[0]}")
    else:
        df = df[split_keep]

    return df

def cbk_long_f(cbk: object, domain: str) -> pd.DataFrame:
    val_cols = [str(x) for x in range(1,7)]
    val_cols.extend(['A','B'])

    cbk_vars = ['qtiLabel','item_format','item_order','cq_key','domain','unit_id','resp_cat','db_key']

    if(domain == 'FLA'):
        cbk_vars.append('qtiLabel2')

    cbk_id_vars = [col for col in cbk.columns if col in cbk_vars]

    cbk_long = pd.melt(
        cbk,
        id_vars = cbk_id_vars,
        value_vars = val_cols,
        var_name = 'cq_cat',
        value_name = 'db_resp',
    ).sort_values(['qtiLabel','db_resp'])

    return cbk_long

def merge_cbk_status(df: object, cbk: object, domain: str, ams_data: object) -> pd.DataFrame:
    if(domain == 'FLA'):
        on_vars = ['unit_id','resp_cat','db_resp']
        sel_vars = ['unit_id','resp_cat','db_resp','cq_cat']
    elif(domain == 'SCI'):
        on_vars = ['qtiLabel','resp_cat','db_resp']
        sel_vars = ['qtiLabel','resp_cat','db_resp','cq_cat']

    cbk_long = cbk_long_f(cbk = cbk, domain = domain)
    cbk_merge = cbk_long.loc[pd.notnull(cbk_long['db_resp']),sel_vars]

    speak = (df['itemId'].str.contains(r"FLAS"))

    df_S = df[speak].drop(columns = ['unit_id','index'])
    df = df[~speak]
    
    df_S = df_S.iloc[:,0:6].drop_duplicates(keep = 'first')
    df_S['login'] = df_S['login'].astype(str)
    df_S['item_format'] = "Open response"

    df1 = pd.merge(
        df,
        cbk_merge,
        on = on_vars,
        how = 'left'
    )

    cbk_merge2 = cbk_long.drop(['cq_cat','db_resp'],axis = 1).assign(cbk_status='present').drop_duplicates()

    df2 = pd.merge(
        df1,
        cbk_merge2,
        on = ['unit_id','resp_cat'],
        how = 'left'
    )

    ams_data_merge = ams_data.loc[ams_data['login'].isin(list(df_S.login))].drop_duplicates(subset = ['login','unit_id'],keep = 'last')
    ams_data_merge['db_key'] = ams_data_merge['db_resp']

    qti_labels = pd.DataFrame(
        {
            'resp_cat': [
                "Part1Ver1",
                "Part1Ver2",
                "Part1Ver3",
                "Part1Ver4",
                "Part1Ver5",
                "Part2Ver1",
                "Part2Ver2",
                "Part2Ver3",
                "Part2Ver4",
                "Part2Ver5",
                "Part3Ver1",
                "Part3Ver2",
                "Part3Ver3",
                "Part3Ver4",
                "Part3Ver5",
                "Part4Ver1",
                "Part4Ver2",
                "Part4Ver3",
                "Part4Ver4",
                "Part4Ver5"
            ],
            'qtiLabel': [
                "FLAS101",
                "FLAS102",
                "FLAS103",
                "FLAS104",
                "FLAS105",
                "FLAS201",
                "FLAS202",
                "FLAS203",
                "FLAS204",
                "FLAS205",
                "FLAS301",
                "FLAS302",
                "FLAS303",
                "FLAS304",
                "FLAS305",
                "FLAS401",
                "FLAS402",
                "FLAS403",
                "FLAS404",
                "FLAS405",
            ]
        }
    )

    qti_lookup = pd.read_excel('./data/FLA test assembly list 30042024.xlsx',sheet_name='Speaking 1')
    qti_lookup = qti_lookup.melt(
        id_vars = 'form',
        value_vars= ['part1','part2','part3','part4']
    ).rename(columns = {'variable':'unit_id','value':'resp_cat'}).merge(
        qti_labels,
        how = 'left',
        on = ['resp_cat']
    )
    qti_lookup['testQtiLabel'] = qti_lookup['form'].apply(lambda x: "FLA-S-" + str(int(x[-2:])))

    qti_lookup['unit_id'] = qti_lookup['unit_id'].apply(lambda x: re.sub('part','FLA25SP',x))

    df_S = pd.merge(
        df_S,
        ams_data_merge,
        how = 'left',
        on = 'login'
    )

    rows_after_merge = df_S.shape[0]
    
    df_S = df_S.merge(
        qti_lookup.drop(columns = ['form']),
        how = 'left',
        on = ['unit_id','testQtiLabel']
    ).assign(cbk_status = 'present',domain = 'FLA-S').rename(columns = {"qtiLabel": "qtiLabel2"})

    df_S['in_cq'] = df_S['qtiLabel2'].apply(lambda x: '1' if pd.notnull(x) else '0')

    df2['cbk_status'] = df2['cbk_status'].fillna('stimulus')
    df2['in_cq'] = np.select([(df2['dup_std_item'] == '') & (df2['cbk_status'] == 'present')],'1',default = '0')

    df3 = pd.concat(
        [df2,df_S],
        axis = 0
    )
    
    print(f"Expected rows: {df.shape[0] + rows_after_merge}")
    print(f"Actual rows: {df3.shape[0]}")
    return df3

def unix_time_millis(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds() * 1000.0)

def unix_time_string(num: int):
  x = datetime.datetime.fromtimestamp(int(num)/1000).strftime("%Y-%m-%d %H:%M:%S")
  return x

def time_var_recode(df: object) -> pd.DataFrame:
    df['session_dur'] = df.apply(lambda x: None if (pd.isnull(x['sessionEndTime']) | pd.isnull(x['sessionStartTime'])) else (int(x['sessionEndTime']) - int(x['sessionStartTime'])),axis = 1)
    df['item_dur'] = df.apply(lambda x: None if (pd.isnull(x['itemEndTime']) | pd.isnull(x['itemStartTime'])) else (int(x['itemEndTime']) - int(x['itemStartTime'])),axis = 1)

    time_vars = [
        'sessionStartTime',
        'sessionEndTime',
        'itemStartTime',
        'itemEndTime'
    ]

    for t in time_vars:
        df[t] = df[t].apply(lambda x: None if pd.isnull(x) else unix_time_string(int(x)))

    return df

def score_resp_recode(df: object, domain: str) -> pd.DataFrame:
    conditions = [
        (df['domain'] == 'FLA-S'),
        (df['db_resp']==df['db_key']) & (pd.notnull(df['db_key'])) & (pd.notnull(df['db_resp'])),
        (df['db_resp']!=df['db_key']) & (pd.notnull(df['db_key'])) & (pd.notnull(df['db_resp'])),
        df['db_key'].notna()
    ]

    choices = [
        df['db_resp'],
        '1',
        '0',
        '9'
    ]

    df['db_score_code'] = np.select(conditions,choices,None)

    conditions = [
        (df['item_completionStatus'].eq('not_attempted')) & (df['statusCorrect'].eq('skipped')),
        (df['item_completionStatus'].eq('unknown') | df['item_completionStatus'].eq('completed')) & (df['statusCorrect'].eq('skipped')) & (df['item_dur'].eq(0)),
        (df['session_dur'].notna() & df['session_dur'] < 0) | (df['item_dur'].notna() & df['item_dur'] < 0),
        df['db_score_code'].eq('9')
    ]

    codes = [
        '7',
        't',
        't',
        '9'
    ]

    df['score_code'] = np.select(
        conditions,
        codes,
        df['db_score_code']
    )

    df['cq_cat'] = np.select(
        conditions,
        codes,
        df['cq_cat']
    )

    df['cq_score'] = df['score_code']

    speak_recode_dict = {
        "0" : "A",
        "0.5" : "B",
        "1" : "C",
        "1.5" : "D",
        "2" : "E",
        "2.5" : "F",
        "3" : "G",
        "3.5" : "H",
        "4" : "I",
        "4.5" : "J",
        "5" : "K",
        "5.5" : "L",
        "6" : "M",
    }

    df['cq_cat'] = np.where((df['testQtiLabel'].str.contains('FLA\\-S')),df['cq_score'].replace(speak_recode_dict),df['cq_cat'])

    if(domain == 'FLA'):
        df.rename(columns = {'qtiLabel': 'qtiLabelRemove','qtiLabel2': 'qtiLabel'},inplace=True)
        df.drop(columns = ['qtiLabelRemove'],inplace=True)

    return df

def trailing_missing(df: object) -> pd.DataFrame:
    dom_list = df.domain.unique().tolist()
    r_tab_list = []

    for doml in dom_list:

        test = df.loc[(df['in_cq'] == '1') & (pd.notnull(df['cq_cat'])) & (~df['cq_cat'].isin(['t','u'])) & (df['domain'] == doml),['login','qtiLabel','cq_cat','itemEndTime','item_order']].sort_values(['itemEndTime','item_order'])
        log_list = test.login.unique()

        for l in log_list:
            test_log = test.loc[(test['login'] == l),:].astype('string')

            var_grpd = test_log.groupby(['login'])['cq_cat']
            test_log['tmp'] = (var_grpd.shift(0) != var_grpd.shift(1)).cumsum()
            test_log['tmp'] = test_log['tmp'].fillna(0)

            speak_incomplete = (test_log.shape[0] != 4) and (doml == 'FLA-S')

            last_val = test_log.tail(1).cq_cat.iloc[0]
            if((last_val != '9') or (speak_incomplete)):
                continue
            elif(all(test_log.cq_cat == '9')):
                r_tab = test_log.assign(cq_cat_new = 'r')
                r_tab = r_tab.loc[:,['login','qtiLabel','cq_cat_new']]
                r_tab_list.append(r_tab)
            else:
                last_grp = test_log.tail(1).tmp.iloc[0]
                nine_tab = test_log.loc[test_log['tmp']==last_grp,:]
                if(nine_tab.shape[0]>1):
                    r_tab = nine_tab.iloc[1:].assign(cq_cat_new = 'r')
                    r_tab = r_tab.loc[:,['login','qtiLabel','cq_cat_new']]
                    r_tab_list.append(r_tab)
                else:
                    continue

    if(len(r_tab_list) > 0):
        r_tab_new = pd.concat(r_tab_list,axis = 0)

        df['login'] = df['login'].astype('string')

        df1 = df.merge(
            r_tab_new,
            how = 'left',
            on = ['login','qtiLabel']
        )

        df1['cq_cat_new'] = np.where(pd.notnull(df1['cq_cat_new']),df1['cq_cat_new'],df1['cq_cat'])
        df1 = df1.drop(columns=['cq_cat']).rename({'cq_cat_new':'cq_cat'},axis = 1)
        df1['cq_score'] = np.where(df1['cq_cat'].eq('r'),'r',df1['cq_score'])
        df1['score_code'] = np.where(df1['cq_cat'].eq('r'),'r',df1['score_code'])
    else:
        df1 = df

    return df1

def cmc_item_create(df: object, cbk: object, domain: str) -> pd.DataFrame:

    orig_row = df.shape[0]

    if(domain == 'FLA'):
        summ_tab = cbk.loc[(cbk['qtiLabel2'].str.endswith('T',na = False)) & (cbk['item_type'] == 'CMC'),['qtiLabel2','unit_id','domain']]
    
    if(summ_tab.shape[0] > 0):
    
        summ_tab_list = []

        for indx,row in summ_tab.iterrows():
            summ_tab_row = df.loc[(df['in_cq']=='1') & (df['unit_id'] == row['unit_id']),:]
            summ_tab_row = summ_tab_row.groupby(['language','login','session_dur','sessionEndTime'])['cq_score'].apply(lambda x: '9' if (x=='9').all() else str((x=='1').sum())).to_frame('cq_score').reset_index()
            summ_tab_row['cq_cat'] = summ_tab_row['cq_score']
            summ_tab_row['score_code'] = summ_tab_row['cq_score']
            summ_tab_row['in_cq'] = '1'
            summ_tab_row['qtiLabel'] = row['qtiLabel2']
            summ_tab_row['domain'] = row['domain']
            summ_tab_list.append(summ_tab_row)

        summ_tab_all = pd.concat(summ_tab_list,axis = 0)
        df = pd.concat([df,summ_tab_all],axis = 0)

    print(f"Expected rows: {orig_row + summ_tab_all.shape[0]}")
    print(f"Actual rows: {df.shape[0]}")

    return df

def merge_participant_info(df: object, student_participants: object) -> pd.DataFrame:
    stu_dat = student_participants.loc[~pd.isnull(student_participants['login']),['login','username','grade','gender','dob_mm','dob_yy','sen','mpop1','ppart1','isoalpha3','isoname','isocntcd','test_attendance','questionnaire_attendance','batch']].drop_duplicates('login',keep = 'last')
    stu_dat['login'] = stu_dat['login'].astype(str)
    df['login'] = df['login'].astype(str)
    df1 = df.merge(
        stu_dat,
        how = 'left',
        on = 'login'
    )

    df1['ppart1'] = df1['ppart1'].astype(str).apply(lambda x: re.sub(".0","",x)).replace('nan','')
    df1['mpop1'] = df1['mpop1'].astype(str).apply(lambda x: re.sub(".0","",x)).replace('nan','')
    df1['mpop1'] = df1['mpop1'].astype(str)

    df1['in_cq'] = np.where(pd.isnull(df1['username']),'0',df1['in_cq'])

    print(f"Expected rows: {df.shape[0]}")
    print(f"Actual rows: {df1.shape[0]}")

    return df1

def read_json_file(filepath: str) -> pd.DataFrame:
    try:
        df = pd.read_json(filepath)

    except FileNotFoundError as e:
        print(f"Error: {e}")

    # Handle any other exceptions
    except Exception as e:
        print(f"Error: {e}")

    finally:
        df['language'] = df['language'].apply(lambda x: x.rsplit('/', 1)[-1])
        df = df.sort_values(by = ['login','test_qti_id','last_update_date'])
        df = df.drop_duplicates(subset=['login','test_qti_id'],keep = 'last')
        return df
    
def sql_query_ge(nc_dat: object,cbk: object,con) -> pd.DataFrame:
    
    con.autocommit = True
    cur = con.cursor()

    print("Extracting SQL query checks...")

    domain = 'FLA'
    if(domain == 'SCI'):
        regex_dom = r'^(S\d|SA1).*'
    elif(domain == 'FLA'):
        regex_dom = '^FLA\\-.*'

    cnt_code = nc_dat['isocntcd']
    process = nc_dat['process']

    if(process == 'post' or process == 'now'):

        fetch_query = f"""
            SELECT
                b.login,
                b.itemId,
                b.unit_id,
                jsonb_object_keys(b.responses) as resp_cat
            FROM (
                SELECT
                    t.login, t.itemId,
                    t.raw_data->'items'->itemId->>'qtiLabel' as unit_id,
                    t.raw_data->'items'->itemId->'responses' as responses
                FROM (
                    SELECT 
                        login,
                        regexp_substr(login,'\\d+') as username,
                        raw_data,
                        test_qti_id,
                        jsonb_object_keys(raw_data->'items') as itemId
                    FROM oat.delivery_results
                    WHERE test_qti_id ~ '{regex_dom}'
                ) AS t
                WHERE t.username ~ '^[125]{cnt_code}'
            ) AS b
            WHERE jsonb_typeof(b.responses) = 'object'
            AND b.login IN (
                SELECT p.login FROM maple.maple_student_post_val as p WHERE p.username ~ '^[125]{cnt_code}'
            );
            """
    else:
        fetch_query = f"""
            SELECT
                b.login,
                b.itemId,
                b.unit_id,
                jsonb_object_keys(b.responses) as resp_cat
            FROM (
                SELECT
                    t.login, t.itemId,
                    t.raw_data->'items'->itemId->>'qtiLabel' as unit_id,
                    t.raw_data->'items'->itemId->'responses' as responses
                FROM (
                    SELECT 
                        login,
                        regexp_substr(login,'\\d+') as username,
                        raw_data,
                        test_qti_id,
                        jsonb_object_keys(raw_data->'items') as itemId
                    FROM oat.delivery_results
                    WHERE test_qti_id ~ '{regex_dom}'
                ) AS t
                WHERE t.username ~ '^[125]{cnt_code}'
            ) AS b
            WHERE jsonb_typeof(b.responses) = 'object'
            AND b.login ~ '^((?!9999).)*$'
            AND b.login ~ '^((?!demo).)*$';
            """
        
    cur.execute(fetch_query)

    tmp = pd.DataFrame.from_records(cur.fetchall())
    
    df = tmp.merge(
        cbk.loc[:,['unit_id','resp_cat','qtiLabel2']].drop_duplicates(),
        on = ['unit_id','resp_cat'],
        how = 'left'
    )
    
    df = df.loc[(~pd.isnull(df['qtiLabel2'])) | (df['qtiLabel2'].str.contains("RMMB",na=False)),:].rename(columns = {'qtiLabel2':'qtiLabel'}).assign(source='match')

    con.close()

    return df