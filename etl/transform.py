# Import modules
import pandas as pd
pd.options.mode.chained_assignment = None
import re
import numpy as np
import datetime

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
        df_cnt = df_cnt.drop(columns = ['count']).merge(df['login','itemId','sessionEndTime'],on = ['login','itemId'],how='outer')
        df_cnt['rank'] = df_cnt.groupby(['login','itemId'])['sessionEndTime'].rank(method = 'dense',ascending = False)
        df_cnt = df_cnt.loc[df_cnt['rank'] > 1,['login','itemId']]
        df_cnt['dup_std_item'] = 'duplicate'

        df = df.merge(
            df_cnt,
            on = ['login','itemId'],
            how = 'left'
        )

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

def merge_cbk_status(df: object, cbk: object, domain: str) -> pd.DataFrame:

    if(domain == 'FLA'):
        on_vars = ['unit_id','resp_cat','db_resp']
        sel_vars = ['unit_id','resp_cat','db_resp','cq_cat']
    elif(domain == 'SCI'):
        on_vars = ['qtiLabel','resp_cat','db_resp']
        sel_vars = ['qtiLabel','resp_cat','db_resp','cq_cat']

    cbk_long = cbk_long_f(cbk = cbk, domain = domain)
    cbk_merge = cbk_long.loc[pd.notnull(cbk_long['db_resp']),sel_vars]

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

    df2['cbk_status'] = df2['cbk_status'].fillna('stimulus')
    df2['in_cq'] = np.select([(df2['dup_std_item'] == '') & (df2['cbk_status'] == 'present')],'1',default = '0')

    return df2

def unix_time_millis(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds() * 1000.0)

def unix_time_string(num: int):
  x = datetime.datetime.fromtimestamp(int(num)/1000).strftime("%Y-%m-%d %H:%M:%S")
  return x

def time_var_recode(df: object) -> pd.DataFrame:
    df['session_dur'] = df.apply(lambda x: None if (np.isnan(x['sessionEndTime']) | np.isnan(x['sessionStartTime'])) else (int(x['sessionEndTime']) - int(x['sessionStartTime'])),axis = 1)
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
        (df['db_resp']==df['db_key']) & (pd.notnull(df['db_key'])) & (pd.notnull(df['db_resp'])),
        (df['db_resp']!=df['db_key']) & (pd.notnull(df['db_key'])) & (pd.notnull(df['db_resp'])),
        df['db_key'].notna()
    ]

    choices = [
        '1',
        '0',
        '9'
    ]

    df['db_score_code'] = np.select(conditions,choices,None)

    conditions = [
        (df['item_completionStatus'].eq('not_attempted')) & (df['statusCorrect'].eq('skipped')) & (df['session_dur'] == 0),
        (df['item_completionStatus'].eq('unknown') | df['item_completionStatus'].eq('completed')) & df['statusCorrect'].eq('skipped'),
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

    if(domain == 'FLA'):
        df.rename(columns = {'qtiLabel': 'qtiLabelRemove','qtiLabel2': 'qtiLabel'},inplace=True)
        df.drop(columns = ['qtiLabelRemove'],inplace=True)

    return df

def trailing_missing(df: object, cbk: object) -> pd.DataFrame:
    dom_list = cbk.domain.unique().tolist()
    r_index_list = []

    for doml in dom_list:

        test = df.loc[(df['in_cq'] == '1') & (df['cq_cat'].notna()) & (~df['cq_cat'].str.startswith('t|u',na = False)) & (df['domain'] == doml),['login','cq_cat','itemEndTime']].sort_values(['itemEndTime'])
        log_list = test.login.unique()

        for l in log_list:
            test_log = test.loc[(test['login'] == l),:]
            var_grpd = test_log.groupby(['login'])['cq_cat']
            test_log['tmp'] = (var_grpd.shift(0) != var_grpd.shift(1)).cumsum()

            last_val = test_log.tail(1).cq_cat.iloc[0]
            if(last_val != '9'):
                continue
            else:
                last_grp = test_log.tail(1).tmp.iloc[0]
                nine_tab = test_log.loc[test_log['tmp']==last_grp,:]
                if(nine_tab.shape[0]>1):
                    r_tab = nine_tab.iloc[1:]
                    r_indexes = r_tab.index.to_list()
                    r_index_list.extend(r_indexes)
                else:
                    continue

    if(len(r_index_list) > 0):
        df.loc[df.index[r_index_list],['cq_cat','cq_score','score_code']] = 'r'

    return df

def cmc_item_create(df: object, cbk: object, domain: str) -> pd.DataFrame:

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

    return df

def merge_participant_info(df: object, student_participants: object) -> pd.DataFrame:
    stu_dat = student_participants.loc[~pd.isnull(student_participants['login']),['login','username','grade','gender','dob_mm','dob_yy','sen','mpop1','ppart1','isoalpha3','isoname','isocntcd','test_attendance','questionnaire_attendance']].drop_duplicates('login',keep = 'last')
    stu_dat['login'] = stu_dat['login'].astype(str)
    df['login'] = df['login'].astype(str)
    df1 = df.merge(
        stu_dat,
        how = 'left',
        on = 'login'
    )

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
        return df
    
def sql_query_ge(nc_dat: object,cbk: object,con) -> pd.DataFrame:
    
    con.autocommit = True
    cur = con.cursor()

    print("Extracting SQL query checks...")

    domain = 'FLA'
    if(domain == 'SCI'):
        regex_dom = '^(S\\d|SA1).*'
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
                        regexp_substr(login,'\d+') as username,
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
                        regexp_substr(login,'\d+') as username,
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
    df = df.loc[~pd.isnull(df['qtiLabel2'])].rename(columns = {'qtiLabel2':'qtiLabel'}).assign(source='match')

    con.close()

    return df