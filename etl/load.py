# Import modules
import psycopg2
import configparser
import yaml
import pandas as pd
import datetime

# Import database configuration
with open('./config.yaml', 'r') as file:
    config_data = yaml.safe_load(file)

# Define the load process as a Bonobo graph
def load_data(df: object, create_PSQL: str, insert_PSQL: str) -> object:

    config = configparser.ConfigParser()
    config.read('config.ini')
    conn = psycopg2.connect(
        host=config['POSTGRESQL']['host'],
        port=config['POSTGRESQL']['port'],
        dbname=config['POSTGRESQL']['database'],
        user=config['POSTGRESQL']['user'],
        password=config['POSTGRESQL']['password']
    )
    cursor = conn.cursor()
    cursor.execute(create_PSQL)
    conn.commit()

    for row in df.itertuples(index=False):
        cursor.execute(insert_PSQL, row)

    conn.commit()
    cursor.close()
    conn.close()

def make_long_file(df: object, domain: str):
    df.loc[
        (df['in_cq']=='1'),
        [
            'isocntcd',
            'isoalpha3',
            'isoname',
            'username',
            'login',
            'last_update_date',
            'domain',
            'language',
            'testQtiLabel',
            'itemId',
            'qtiLabel',
            'score',
            'sessionStartTime',
            'sessionEndTime',
            'session_dur',
            'itemStartTime',
            'itemEndTime',
            'item_dur',
            'item_completionStatus',
            'statusCorrect',
            'unit_id',
            'item_format',
            'item_order',
            'db_key',
            'db_score_code',
            'db_resp',
            'score_code',
            'cq_cat',
            'cq_score'
        ]
    ].to_csv(f'P:\\VM Backup\\251003 PISA FT T6a\\Output\\For Task 6b\\CQ {domain} long [in progress]_{datetime.date.today().strftime('%Y%m%d')}.csv',index = False)

def make_wide_file(df: object, cbk: object):
    dom_list = cbk.domain.unique().tolist()

    for doml in dom_list:
        cog_vars = cbk.loc[cbk['domain'] == doml,['qtiLabel2','item_order']].sort_values(['item_order'])['qtiLabel2'].tolist()
        long_file = df.loc[(df['domain'] == doml) & (df['in_cq'] == '1') & (df['mpop1'] == '1') & (df['qtiLabel'].isin(cog_vars)),:].assign(
            SchID=lambda x: x['username'].astype(str).str.slice(4,8)
        )

        lab_list = ['-Time','-StartT','-EndT']
        dat_list = []

        for lab in lab_list:
            item_lab = 'item'+lab
            long_file[item_lab] = long_file['qtiLabel'].apply(lambda x: str(x) + lab if pd.notnull(x) else '')
            time_file = long_file.loc[long_file[item_lab]!='',:].pivot(index = ['username'],columns = [item_lab],values = ['item_dur'])
            dat_list.append(time_file)

        time_file_final = pd.concat(dat_list,axis = 1).reset_index()
        time_file_final.columns = [' '.join(col).strip().replace("item_dur ","") for col in time_file_final.columns.values]
        time_file_final = time_file_final.rename(
            columns = {
                'username': 'StdID',
            }
        )

        dat_type = ['score','cat']

        for datt in dat_type:

            dat_var = 'score_code' if datt == 'score' else 'cq_cat'

            wide_file = long_file.pivot(
                index = [
                    'isocntcd', 'isoalpha3', 'isoname', 'SchID','username',
                    'grade', 'dob_mm', 'dob_yy','gender', 'sen', 'language','mpop1', 'ppart1',
                    'test_attendance', 'questionnaire_attendance',
                    'session_dur','sessionEndTime'
                ],
                columns = ['qtiLabel'],
                values = [dat_var],
            ).reset_index()

            wide_file.columns = [' '.join(col).strip().replace(dat_var + " ","") for col in wide_file.columns.values]
            wide_file = wide_file.fillna('7')
            wide_file = wide_file.rename(
                columns = {
                    'isocntcd': 'CNT-num',
                    'isoalpha3': 'CNT',
                    'isoname': 'Country',
                    'sen': 'SEN',
                    'language': 'TestLang',
                    'gender': 'GenderSTF',
                    'username': 'StdID',
                    'sessionEndTime': 'TestDate',
                    'session_dur': 'TotSessionTime'
                }
            )

            wide_file = pd.merge(
                wide_file,
                time_file_final,
                on = ['StdID'],
                how = 'left'
            )

            wide_file.loc[wide_file['mpop1'] == '1',:].to_csv(
                f'P:\\VM Backup\\251003 PISA FT T6a\\Output\\For Task 6b\\CQ {doml} wide {datt} [in progress]_{datetime.date.today().strftime('%Y%m%d')}.csv',index = False
            )
