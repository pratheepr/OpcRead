import os

import pywintypes
import OpenOPC
import sys
import time as tm
import pandas as pd
import math
import numpy as np
from configparser import ConfigParser
from datetime import datetime
from os import path
from LucaDB import DBAccess as db


def tags_listing(DBConnObj, nof_tags_per_line, err_tag_list):
    # my_file = open(tag_file, 'r')
    # my_file_list = my_file.read().splitlines()

    OPC_Tags_Read = db.OpcTagMaster_Select(ConnObj=DBConnObj)

    Proc_OPC_Tags = list(filter(lambda OPC_Tags_Read: OPC_Tags_Read[5] == 'Y', OPC_Tags_Read))

    nof_rows = math.ceil(len(Proc_OPC_Tags) / nof_tags_per_line)
    db_tag_list = [itm[1] for itm in Proc_OPC_Tags if itm[1] not in err_tag_list]

    ret_tag_list = []
    ctr = 0

    for x in range(nof_rows):
        col = []
        for y in range(nof_tags_per_line):
            if ctr >= len(db_tag_list):
                col.append('')
            else:
                col.append(db_tag_list[ctr])
            ctr = ctr + 1

        ret_tag_list.append(col)

    return ret_tag_list


def ConnectOPC(OPC_Server_Name):
    try:
        OPC_Client = OpenOPC.client()
        ret_value = 0
        print(OPC_Client.servers())
    except Exception as e:
        print('Exception in OPC Connect' + str(e))
        ret_value: Exception = e
    finally:
        return OPC_Client, ret_value


def Cleanup_Metrics_File(opc_read_output_filename, history_duration_in_hours):
    print('Cleanup Metrics function')
    df_metrics_file = pd.read_csv(opc_read_output_filename, delimiter=',', header=0, parse_dates=['datetime'],
                                  infer_datetime_format=True)
    print(df_metrics_file)
    # df['new_datetime'] = pd.to_datetime(df['datetime'], infer_datetime_format=True)
    print(df_metrics_file.info())

    df_fil = df_metrics_file[df_metrics_file['datetime'] >= datetime.now() - pd.Timedelta(history_duration_in_hours)]
    df_fil.to_csv(opc_read_output_filename, index=False, header=True, mode='w')


def Write_To_Files(ctr, df_write, opc_read_output_filename):
    print(df_write)
    df_pivot = df_write.pivot_table(index=['datetime'], columns=['opc_tag'], values='value', aggfunc=sum)

    if ctr == 0:
        df_write.to_csv(opc_read_output_filename, index=False, header=True, mode='w')
        df_pivot.to_csv(opc_read_pivot_output_filename, index=True, header=True, mode='w')
    else:
        df_write.to_csv(opc_read_output_filename, index=False, header=False, mode='a')
        df_pivot.to_csv(opc_read_pivot_output_filename, index=True, header=False, mode='a')

    return


if __name__ == "__main__":
    # arguments = len(sys.argv) - 1

    ConfigFile = 'opcread_config.ini'
    config = ConfigParser()
    config.read(ConfigFile)

    opc_read_tags_filename = config['opc_read']['opc_read_tags_filename']
    history_duration_in_hours = config['opc_read']['history_duration_in_hours']
    opc_read_output_filename = config['opc_read']['opc_read_output_filename']
    tag_listing_per_call = int(config['opc_read']['tag_listing_per_call'])
    OPC_Server_Name = config['opc_read']['OPC_Server_Name']
    sleep_duration_in_secs = int(config['opc_read']['sleep_duration_in_secs'])
    opc_read_pivot_output_filename = config['opc_read']['opc_read_pivot_output_filename']
    alert_db_location = config['opc_read']['DB_Location']
    opc_error_retry = int(config['opc_read']['opc_error_retry'])

    OPC_Conn_Obj, ret = ConnectOPC(OPC_Server_Name)
    OPC_Conn_Obj.connect(OPC_Server_Name)
    #    print("The script is called with arguments" + str(arguments))

    print('Connect established')
    print(ret)
    print(OPC_Conn_Obj)
    pywintypes.datetime = pywintypes.TimeType

    DbConnObj = db.CrtConnObject(alert_db_location)

    print('tag function')
    # tag_list = tags_listing(opc_read_tags_filename, tag_listing_per_call)
    tag_list = tags_listing(DBConnObj=DbConnObj, nof_tags_per_line=tag_listing_per_call, err_tag_list=[])
    print(f'********************Tags Selected are: {tag_list}')
    df_error_tags = pd.DataFrame(columns=['OPC_TAG', 'READREQ_TIMESTAMP'])

    ctr = 0
    while 1:
        print('CTR is : ' + str(ctr))
        print('\n ******Read INDIVIDUAL Tags')
        OPC_Conn_Obj, ret = ConnectOPC(OPC_Server_Name)
        OPC_Conn_Obj.connect(OPC_Server_Name)
        Error_Tag_Found = 0
        for OPC_Tags in tag_list:
            try:
                Readreq_Timestamp = datetime.now()
                read_ret = OPC_Conn_Obj.read(OPC_Tags)
                df = pd.DataFrame(read_ret, columns=['opc_tag', 'value', 'quality', 'old_datetime'])
                df['old_datetime'] = df['old_datetime'].str[:19]
                df = df.loc[df['opc_tag'] != '']
                df['datetime'] = pd.to_datetime(df['old_datetime'], infer_datetime_format=True)
                df['value'].replace({'True': 1, 'False': 0})
                df = df.drop(columns=['old_datetime'])
                df['Readreq_Timestamp'] = Readreq_Timestamp

                # print(df.info())
                df_tmp = [df['opc_tag'], df['value'], df['quality'], df['datetime'], df['Readreq_Timestamp']]
                headers = ['OPC_TAG', 'TAG_VALUE', 'TAG_STATUS', 'OPC_TIMESTAMP', 'READREQ_TIMESTAMP']

                df_cp_todb = pd.concat(df_tmp, axis=1, keys=headers)

                try:
                    ret = db.OpcTransLog_MassInsert(ConnObj=DbConnObj, DF_writeToDB=df_cp_todb)
                except Exception as e:
                    print(e)

                df_tmp_err_tags = df_cp_todb.loc[df_cp_todb['TAG_STATUS'] == 'Error']
                if df_tmp_err_tags.shape[0] > 0:
                    df_t = pd.DataFrame([df_tmp_err_tags['OPC_TAG'], df_tmp_err_tags['READREQ_TIMESTAMP']])
                    df_error_tags = df_error_tags.append(df_tmp_err_tags)
                    Error_Tag_Found = 1

                if df_error_tags.shape[0] > 0:
                    df_error_tags = df_error_tags.loc[
                        df_error_tags['READREQ_TIMESTAMP'] > datetime.now() - pd.Timedelta(seconds=opc_error_retry)]

                    print(' Error Data Frame : *******************')
                    print(df_error_tags)
                    lst_err_tags = list(df_error_tags['OPC_TAG'].unique())
                    print(f'Distinct error tags are : {lst_err_tags}  ')
                    print(' ************************************** :')

                # Write_To_Files(ctr=ctr, df_write=df, opc_read_output_filename=opc_read_output_filename)

            except Exception as e:
                print('Error in OPC_Conn_Obj.read(OPC_Tags)')
                print(e)

        ctr = ctr + 1
        os.system('taskkill /f /im hcitrs.exe')
        # taskkill /f /im "devenv.exe"
        tm.sleep(sleep_duration_in_secs)

        if ctr % 5 == 0 and Error_Tag_Found == 1:
            tag_list = tags_listing(DBConnObj=DbConnObj, nof_tags_per_line=tag_listing_per_call,
                                    err_tag_list=lst_err_tags)
            print(f'********************Tags Selected are: {tag_list}')

        # if ctr % 10 == 0:
        #    Cleanup_Metrics_File(opc_read_output_filename, history_duration_in_hours)
        # Cleanup_Metrics_File(opc_read_pivot_output_filename,history_duration_in_hours)
