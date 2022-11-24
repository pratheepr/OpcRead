import sqlite3
from datetime import datetime, date


def CrtConnObject(DB_Location):
    try:
        sqliteConnection = sqlite3.connect(DB_Location, detect_types=sqlite3.PARSE_DECLTYPES)
        cursor = sqliteConnection.cursor()
        return sqliteConnection
    except sqlite3.Error as ERROR:
        print("Error while working with SQLite", ERROR)


# function  to insert data into alerting rules
def AlertingRules_Insert(ConnObj, Buss_area, Alarm_name, Alarm_desc, Tag_name, Tag_condition, Threshold_value,
                         Pcntg_Above_Threshold, Check_duration_secs, Multi_cond, Logic_flow_order,
                         Logical_operator, Alert_active, Suppress_after_alert_secs, Alert_recepients):
    db_msg = ''
    try:
        CursorObj = ConnObj.cursor()

        sqlite_alerting_rules_insert_param = "INSERT INTO ALERTING_RULES (BUSS_AREA, ALARM_DESC, ALARM_NAME, TAG_NAME, TAG_CONDITION, THRESHOLD_VALUE, PCNTG_ABOVE_THRESHOLD, CHECK_DURATION_IN_SECS," \
                                             "MULTI_COND, LOGIC_FLOW_ORDER, LOGICAL_OPERATOR, ALERT_ACTIVE, SUPPRESS_AFTR_ALERT_IN_SECS, ALERT_RECEPIENTS) " \
                                             "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
        sqlite_alerting_rules_data_tuple = (
            Buss_area, Alarm_name, Alarm_desc, Tag_name, Tag_condition, Threshold_value, Pcntg_Above_Threshold,
            Check_duration_secs, Multi_cond,
            Logic_flow_order, Logical_operator, Alert_active, Suppress_after_alert_secs, Alert_recepients)

        CursorObj.execute(sqlite_alerting_rules_insert_param, sqlite_alerting_rules_data_tuple)
        ConnObj.commit()

        CursorObj.execute("select * from ALERTING_RULES")
        for row in CursorObj:
            print(row)

        # CursorObj.close()
        db_msg = 'success'

    except sqlite3.Error as error:
        print("Error while working with SQLite at alerting rules table ", error)
        db_msg = 'failure'

    return db_msg


def AlertingRules_Select(ConnObj):
    db_msg = ''
    records = []
    try:
        CursorObj = ConnObj.cursor()

        sqlite_alertingrules_select_qry = """SELECT SID, BUSS_AREA, ALARM_NAME, ALARM_DESC, TAG_NAME, TAG_CONDITION, THRESHOLD_VALUE, PCNTG_ABOVE_THRESHOLD,
                                            CHECK_DURATION_IN_SECS, MULTI_COND, LOGIC_FLOW_ORDER, LOGICAL_OPERATOR, ALERT_ACTIVE,
                                            SUPPRESS_AFTR_ALERT_IN_SECS, ALERT_RECEPIENTS FROM ALERTING_RULES WHERE ALERT_ACTIVE='Y'"""

        print(sqlite_alertingrules_select_qry)

        ret = CursorObj.execute(sqlite_alertingrules_select_qry)
        records = CursorObj.fetchall()

        CursorObj.close()
        db_msg = 'success'

    except sqlite3.Error as error:
        print("Error while working with SQLite at Opc trans table", error)
        # db_msg = 'Failure: ' + error

    return records


# function tto insert data into alerts table
def Alerts_Insert(ConnObj, Alerting_Rules_sid, Alert_Conditiion, Actual_Value, Alert_Mail_Sent, Alert_Comments):
    db_msg = ''
    try:
        CursorObj = ConnObj.cursor()

        sqlite_alerts_insert_param = "INSERT INTO ALERTS(ALERTING_RULES_SID, ALERT_CONDITION, ACTUAL_VALUE, LOAD_DATETIME, ALERT_MAIL_SENT, ALERT_COMMENTS ) " \
                                     "VALUES(?, ?, ?, ?, ?, ?) "
        sqlite_alerts_data_tuple = (
            Alerting_Rules_sid, Alert_Conditiion, Actual_Value, datetime.now(), Alert_Mail_Sent, Alert_Comments)

        CursorObj.execute(sqlite_alerts_insert_param, sqlite_alerts_data_tuple)
        ConnObj.commit()

        CursorObj.close()
        db_msg = 'success'

    except sqlite3.Error as error:
        print("Error while working with SQLite at alerts table", error)
        db_msg = 'failure'

    return db_msg


# function to delete records less than the given input date
def Alerts_Purge(ConnObj, input_date):
    db_msg = ''
    format = '%Y-%m-%d'  # suitable  format
    input_date = datetime.strptime(input_date, format)
    Delete_Sql = "DELETE FROM ALERTS WHERE DATE(LOAD_DATETIME) <= ? "
    param = [input_date]

    try:
        CursorObj = ConnObj.cursor()
        CursorObj.execute(Delete_Sql, param)
        ConnObj.commit()
        CursorObj.close()
        db_msg = 'success'
    except sqlite3.Error as PurgeErr:
        print("Error in purging data", PurgeErr)
        db_msg = 'failure'

    return db_msg


# function to insert data into opc_trans_log table

def OpcTransLog_Insert(ConnObj, Opc_tag, Tag_value, Tag_status, Readreq_Timestamp):
    db_msg = ''
    try:
        CursorObj = ConnObj.cursor()

        sqlite_opc_log_insert_param = "INSERT INTO OPC_TRANS_LOG(OPC_TAG, TAG_VALUE, TAG_STATUS, OPC_TIMESTAMP, READREQ_TIMESTAMP) " \
                                      "VALUES(?, ?, ?, ?, ?) "
        sqlite_opc_log_data_tuple = (Opc_tag, Tag_value, Tag_status, datetime.now(), Readreq_Timestamp)

        CursorObj.execute(sqlite_opc_log_insert_param, sqlite_opc_log_data_tuple)
        ConnObj.commit()

        CursorObj.close()
        db_msg = 'success'

    except sqlite3.Error as error:
        print("Error while working with SQLite at Opc trans table", error)
        db_msg = 'Failure: ' + error

    return db_msg


def OpcTransLog_MassInsert(ConnObj, DF_writeToDB):
    db_msg = ''
    try:
        DF_writeToDB.to_sql('OPC_TRANS_LOG', con=ConnObj, if_exists='append', index=False)
        ConnObj.commit()

        db_msg = 'success'

    except sqlite3.Error as error:
        print("Error while performing mass insert into OPC_TRANS_LOG", error)
        db_msg = 'Failure: ' + error

    return db_msg


def OpcTransLog_Select(ConnObj, No_Of_Days):
    db_msg = ''
    records = []
    try:
        CursorObj = ConnObj.cursor()

        sqlite_opctranslog_select_qry = """SELECT SID, OPC_TAG, TAG_VALUE, TAG_STATUS, OPC_TIMESTAMP FROM OPC_TRANS_LOG """ \
                                        "WHERE (READREQ_TIMESTAMP) > DATETIME('now','-" + str(No_Of_Days) + " day')"""

        print(sqlite_opctranslog_select_qry)

        ret = CursorObj.execute(sqlite_opctranslog_select_qry)
        records = CursorObj.fetchall()

        CursorObj.close()
        db_msg = 'success'

    except sqlite3.Error as error:
        print("Error while working with SQLite at Opc trans table", error)
        # db_msg = 'Failure: ' + error

    return records


# function to purge data from opc_trans_table
def OpcTransLog_Purge(ConnObj, input_date):
    db_msg = ''
    format = '%Y-%m-%d'  # suitable  format
    input_date = datetime.strptime(input_date, format)
    Delete_Sql = "DELETE FROM OPC_TRANS_LOG WHERE DATE(READREQ_TIMESTAMP) <= ? "
    param = [input_date]

    try:
        CursorObj = ConnObj.cursor()
        CursorObj.execute(Delete_Sql, param)
        ConnObj.commit()
        CursorObj.close()
        db_msg = 'success'
    except sqlite3.Error as PurgeErr:
        print("Error in purging data", PurgeErr)
        db_msg = 'failure'

    return db_msg


def OpcTagMaster_Select(ConnObj):
    db_msg = ''
    records = []
    try:
        CursorObj = ConnObj.cursor()

        sqlite_opctagsmaster_select_qry = """SELECT SID, OPC_TAGNAME, OPC_TAG_DESC, INSERT_USER, INSERT_DATE, TAG_ACTIVE FROM OPC_TAGS_MASTER """

        print(sqlite_opctagsmaster_select_qry)

        ret = CursorObj.execute(sqlite_opctagsmaster_select_qry)
        records = CursorObj.fetchall()

        CursorObj.close()

        db_msg = 'success'
    except sqlite3.Error as Err:
        print("Error in selecting OpcTagMaster data", Err)
        db_msg = 'failure' + Err

    finally:
        if db_msg == 'success':
            return records
        else:
            return []

def OpcTagMaster_Insert(ConnObj, Opc_TagName, Opc_Tag_Desc, Insert_User, Tag_Active):
    db_msg = ''
    try:
        CursorObj = ConnObj.cursor()

        sqlite_opc_Tag_Mstr_insert_param = "INSERT INTO OPC_TRANS_LOG(OPC_TAGNAME, OPC_TAG_DESC, INSERT_USER, " \
                                           "INSERT_DATE, TAG_ACTIVE) " \
                                           "VALUES(?, ?, ?, ?) "
        sqlite_opc_Tag_Mstr_data_tuple = (Opc_TagName, Opc_Tag_Desc, Insert_User, datetime.now(), Tag_Active)

        CursorObj.execute(sqlite_opc_Tag_Mstr_insert_param, sqlite_opc_Tag_Mstr_data_tuple)
        ConnObj.commit()

        CursorObj.close()
        db_msg = 'success'

    except sqlite3.Error as error:
        print("Error while working with SQLite at Opc Tag Master", error)
        db_msg = 'Failure: ' + error

    return db_msg


if __name__ == "__main__":
    try:
        InsertConn = CrtConnObject()

    except sqlite3.Error as error:
        print("Error while working with SQLite", error)
    finally:
        if InsertConn:
            InsertConn.close()
            print("SQLite is closed")
