import os, sys, pyrfc

os.environ['LD_LIBRARY_PATH'] = os.path.dirname(pyrfc.__file__)

os.execv('/usr/bin/python3', ['/usr/bin/python3', '-c', """

import json
import boto3
from pyrfc import Connection
import pandas as pd
import io
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

# --------------------------------------------------------------------------
# 1. Setup Parameter - Need to change below options to meet your environment.
# --------------------------------------------------------------------------

# 1.1 AWS Secret Manager option
secret_name = "s4hanaInfo" #Adjust
region_name = "us-east-1" #Adjust

# 1.2 S3 Bucket information
dataS3Bucket = "pyrfc-s4h-table" #Adjust

# 1.3 output file type(json or parquet).
fileFormat = "parquet" #Adjust

# 1.4 extracting table name and option
tableName = "DD03L" #Adjust
delimiter = '`' #Adjust
rowCount = 100000 #Adjust

# -------------------------------------------
# 2. Default Parameter - don't need to change
# -------------------------------------------

# 2.1 RFC Variable
rfcFunction = "/SAPDS/RFC_READ_TABLE2"
resultRowCount = 0
totalRowCount = 0

# 2.2 Amazon S3 Variable
now = datetime.now() #Get current Date and time 
datetimestring = now.strftime("%Y-%m-%d-%H-%M-%S")
dataS3Folder = "glue/result/"+fileFormat+"/"+tableName+"/"+datetimestring+"/"
dataS3file = tableName+"."+fileFormat
dataS3Folder_err = "glue/result-err/"+fileFormat+"/"+tableName+"/"+datetimestring+"/"
dataS3file_err = tableName+"-err"+"."+fileFormat

# ---------------------------------
# 3.1 Function - Main Function
# ---------------------------------

def call_rfc_function():

    ## 3.1.1 Retrive SAP connection.
    sapauth = _get_sap_connection(secret_name, region_name)
    
    ## 3.1.2 Setup a connection of RFC Function
    conn = Connection(ashost=sapauth['ashost'], sysnr=sapauth['sysnr'], client=sapauth['client'], user=sapauth['user'], passwd=sapauth['passwd'])
    
    print("----Begin of RFC---")
    
    x = 0
    global resultRowCount; resultRowCount = rowCount
    while ( resultRowCount == rowCount ):
    
        ## 3.1.3 Call RFC Function
        result = conn.call(rfcFunction, QUERY_TABLE = tableName, DELIMITER = delimiter, ROWSKIPS=x, ROWCOUNT=rowCount)
        
        ## 3.1.4 Validate result data 
        data, data_err, field_name, field_data, err_count = _validate_data(result)
        
        ## 3.1.5 Create output files(json/parquet) 
        out_buffer, out_buffer_err = _result_to_output(data, data_err, field_name, field_data, err_count)
        
        ## 3.1.6 Send output files to Amazon S3
        _send_to_s3(out_buffer, out_buffer_err)
        
        ## 3.1.7 Skip rows after previous running
        x = x + resultRowCount
    
    print("----End of RFC---")

# --------------------------------------------------------------
# 3.2 Function - Get sap connection info from aws secret manager
# --------------------------------------------------------------

def _get_sap_connection(secret_name, region_name):

    ## 3.2.1 Create a Secrets Manager client and get secret value
    secretsession = boto3.session.Session()
    secretclient = secretsession.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    ## 3.2.2 Get Secret Value 
    get_secret_value_response = secretclient.get_secret_value(SecretId=secret_name)
    sapauth = json.loads(get_secret_value_response['SecretString'])

    ## 3.2.3 Exception 
    if get_secret_value_response['ResponseMetadata']['HTTPStatusCode'] != 200:
        raise Exception(f"Error occured. Please check log{get_secret_value_response}")
    
    ## 3.2.3 return
    return sapauth
    
# ------------------------------------
# 3.3 Function - Validate result data
# ------------------------------------

def _validate_data(result):

    ## 3.3.1 Get row count information
    out_table = result["OUT_TABLE"]
    print("Result Count: "+str(len(result[out_table])))
    global resultRowCount; resultRowCount = len(result[out_table])
    global totalRowCount; totalRowCount = totalRowCount + resultRowCount
    global dataS3file; dataS3file = tableName+str(totalRowCount)+"."+fileFormat
    
    ## 3.3.2 Create a field metadata array
    field_name = []
    field_type = []
    field_data = {}
    for line in result["FIELDS"]:
        field_name.append(line["FIELDNAME"])
        field_type.append(line["TYPE"])
    print("Field Count: "+str(len(field_name)))
    for i in range(len(field_name)):
        field_data[str(field_name[i])] = field_type[i]

    ## 3.3.3 Checking delimiter error
    data = []
    data_err = []
    data_count = 0
    err_count = 0

    for line in result[out_table]:
        raw_data = line["WA"].strip().split(delimiter)
        if len(raw_data) == len(field_name):
            data_count += 1
            data.append(raw_data)
        else:
            # row_data has delimiter errors
            err_count += 1
            data_err.append(raw_data)
            
    print("Data Count: "+str(data_count))             
    print("Error Count: "+str(err_count))
    print("Total Count: "+str(data_count+err_count))
    
    ## 3.3.4 return
    return [data, data_err, field_name, field_data, err_count]


# -----------------------------------------------
# 3.4 Function - Write RFC results to output file
# -----------------------------------------------

def _result_to_output(data, data_err, field_name, field_data, err_count):

    ## 3.4.1 create a pandas to make a json or parquet file
    pd_result = pd.DataFrame(data,columns=field_name)
    
    ## 3.4.2 strip string data
    pd_result = pd_result.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    ## 3.4.3 convert data type from sap to pandas
    for fieldname in field_name:
        if(field_data[fieldname] == 'N'):
          pd_result[fieldname] = pd_result[fieldname].astype('int',errors='ignore')
    
    ## 3.4.4 create a json or parquet from pandas
    out_buffer = None
    out_buffer_err = None
        
    if fileFormat == 'json':
        out_buffer = io.StringIO()
        pd_result.to_json(out_buffer,orient = 'records',lines=True)
        
        ### rows have delimiter errors
        if err_count > 0:
            pd_result_err = pd.DataFrame(data_err)
            out_buffer_err = io.StringIO()
            pd_result_err.to_json(out_buffer_err,orient = 'records',lines=True)
            
    elif fileFormat == 'parquet':
        pd_table = pa.Table.from_pandas(pd_result)
        out_buffer = io.BytesIO()
        pq.write_table(pd_table, out_buffer)
        
        ### rows have delimiter errors
        if err_count > 0:
            pd_result_err = pd.DataFrame(data_err)
            pd_table_err = pa.Table.from_pandas(pd_result_err)
            out_buffer_err = io.BytesIO()
            pq.write_table(pd_table_err, out_buffer_err)
            
    ## 3.4.5 return
    return [out_buffer, out_buffer_err] 
    

# ---------------------------------------------
# 3.5 Function - Send output files to Amazon S3
# ---------------------------------------------

def _send_to_s3(out_buffer,out_buffer_err=None):

    ## 3.5.1 Create a Amazon S3 resource
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(dataS3Bucket)
    
    ## 3.5.2 Put output file to Amazon S3
    my_bucket.put_object(Key=dataS3Folder+dataS3file, Body=out_buffer.getvalue())
    
    ## 3.5.3 Put error file to Amazon S3
    if out_buffer_err != None:
        my_bucket.put_object(Key=dataS3Folder_err+dataS3file_err, Body=out_buffer_err.getvalue())
        

# ------------------------
# 4. Start of Program
# ------------------------  

# call the main function
call_rfc_function()

"""])
