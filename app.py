#!/usr/bin/python
#
# Copyright 2015 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This example fetches data from PQL tables and creates match table files."""

from flask import Flask, Response, current_app, json, request,jsonify
from datetime import timedelta
from collections import OrderedDict,defaultdict
import pandas as pd
import csv
import tempfile
import boto
import os
import sys
from boto import ec2
import boto.manage.cmdshell
import schedule
import time
from datetime import datetime
import shutil
import logging
from boto.s3.key import Key
from dfpServices.auth import *
import gzip



# Import appropriate modules from the client library.
from googleads import dfp
from googleads import errors

app = Flask(__name__)
app.logger.addHandler(logging.StreamHandler(sys.stdout))

@app.route('/')
def index():
    return 'DFP DOMOREPORTING SERVICE'

@app.route('/health')
def health():
    return 'HEALTHY!!!'

@app.route('/getEnv')
def getEnv():
    return(os.environ['NODE_ENV'])

@app.route('/startService')
def start_service():
    print("Scheduler begun")
    return upload_file()
#     schedule.every(2).minutes.do(upload_file)
#
#
#     while True:
#         schedule.run_pending()
#         time.sleep(1)



def upload_file():
    rs_csv_file_path_gz = runReport()
    rs_csv_file_path = uncompress(rs_csv_file_path_gz)
    id_lst = getIdsAsString(rs_csv_file_path)
    pql_csv_file_path = getDfpReport(id_lst)
    fileName = mergeCSVs(rs_csv_file_path,pql_csv_file_path)

    return auth_upload(fileName)

def auth_upload(fileName):    #upload to s3
    AWS_ACCESS_KEY="INSER_ACCESS_KEY"
    AWS_ACCESS_SECRET_KEY="INSERT_SECRET"
    file = open(fileName, 'r+')

    key = file.name
    bucket = 'INSERT_BUCKET_NAME'



    if upload_to_s3(AWS_ACCESS_KEY, AWS_ACCESS_SECRET_KEY, file, bucket, key):
        print("FILE UPLOAD COMPLETE")
        return 'FILE UPLOADED'
    else:
        print("FILE UPLOAD FAILED")
        return 'FILE UPLOAD FAILED...'

def mergeCSVs(rs_csv_file_path,pql_csv_file_path):
     rs_df = pd.read_csv(rs_csv_file_path,keep_default_na=False, na_values=[""])
     rs_df.rename(columns={'DimensionAttribute.LINE_ITEM_COST_PER_UNIT': 'cost_per_unit','DimensionAttribute.LINE_ITEM_GOAL_QUANTITY':'goal_quantity','DimensionAttribute.LINE_ITEM_CONTRACTED_QUANTITY':'contracted_quantity','DimensionAttribute.LINE_ITEM_LIFETIME_IMPRESSIONS':'lifetime_impressions','DimensionAttribute.LINE_ITEM_LIFETIME_CLICKS':'lifetime_clicks','DimensionAttribute.LINE_ITEM_PRIORITY':'prioritty','DimensionAttribute.LINE_ITEM_FREQUENCY_CAP':'frequency_cap','DimensionAttribute.LINE_ITEM_DELIVERY_PACING':'delivery_pacing'}, inplace=True)
     pql_df = pd.read_csv(pql_csv_file_path,keep_default_na=False, na_values=[""])

     #delete lineItemId column.
     rs_df.drop('Dimension.LINE_ITEM_ID', axis=1, inplace=True)
     rs_df.drop('Column.AD_SERVER_IMPRESSIONS', axis=1, inplace=True)
     horizontal_stack = pd.concat([pql_df,rs_df], axis=1)

     prefix="Line_items_report_"
     suffix=".csv"
     ts = datetime.now().strftime('%Y-%m-%d-%H%M%S')
     tsfilepath = prefix+ts+suffix

     horizontal_stack.to_csv(tsfilepath,mode = 'w',index=False)
     print("MERGED FILE " + tsfilepath)
     return tsfilepath

def uncompress(gzip_fname):
    inF = gzip.GzipFile(gzip_fname, 'rb')
    s = inF.read()
    inF.close()

    outF = file(gzip_fname[:-3], 'wb')
    outF.write(s)
    outF.close()
    return(outF.name)

def runReport():
  client = dfp.DfpClient.LoadFromStorage()
  report_downloader = client.GetDataDownloader(version='v201702')
 # Filter for line items of a given order.


  curr_date = datetime.now()
  report_end_date = curr_date + timedelta(days=1700)
  report_start_date = curr_date - timedelta(days=1095)

  query_start_date = curr_date + timedelta(days=14)
  query_end_date = curr_date

  qst = query_start_date.strftime('%Y-%m-%dT%H:%M:%S')
  qedt = query_end_date.strftime('%Y-%m-%dT%H:%M:%S')

#   qst='2017-04-28T00:00:00'
#   qedt='2017-04-14T00:00:00'


  query = "WHERE LINE_ITEM_START_DATE_TIME < '"+qst+"' AND LINE_ITEM_END_DATE_TIME > '"+qedt+"' AND (LINE_ITEM_TYPE != 'NETWORK' AND LINE_ITEM_TYPE != 'PRICE_PRIORITY' AND LINE_ITEM_TYPE != 'AD_EXCHANGE')"


  filter_statement = {'query': query}
  print("RUN REPORT")
  print("QUERY " +query)
  # Create report job.
  report_job = {
      'reportQuery': {
          'dimensions': ['LINE_ITEM_ID'],
          'dimensionAttributes': ['LINE_ITEM_COST_PER_UNIT','LINE_ITEM_GOAL_QUANTITY','LINE_ITEM_CONTRACTED_QUANTITY','SALES_CONTRACT_BUDGET','LINE_ITEM_LIFETIME_IMPRESSIONS','LINE_ITEM_LIFETIME_CLICKS','LINE_ITEM_PRIORITY','LINE_ITEM_FREQUENCY_CAP','LINE_ITEM_DELIVERY_PACING'],
          'statement' : filter_statement,
          'columns': ['AD_SERVER_IMPRESSIONS'],
          'dateRangeType': 'CUSTOM_DATE',
          'startDate': {'year': report_start_date.year,
                        'month': report_start_date.month,
                        'day': report_start_date.day},
          'endDate': {'year': report_end_date.year,
                      'month': report_end_date.month,
                      'day': report_end_date.day}


      }
  }

  try:
    # Run the report and wait for it to finish.
    report_job_id = report_downloader.WaitForReport(report_job)
  except errors.DfpReportError, e:
    print 'Failed to generate report. Error was: %s' % e

  # Change to your preferred export format.
  export_format = 'CSV_DUMP'
  useGzipCompression=False

  report_file = tempfile.NamedTemporaryFile(suffix='.csv.gz', delete=False)

  # Download report data.
  report_downloader.DownloadReportToFile(
      report_job_id, export_format, report_file)

  report_file.close()

  # Display results.
  print 'Report job with id \'%s\' downloaded to:\n%s' % (
      report_job_id, report_file.name)


  return report_file.name


def getIdsAsString(csv_file_Path):
    columns = defaultdict(list) # each value in each column is appended to a list

    with open(csv_file_Path) as f:
        reader = csv.DictReader(f) # read rows into a dictionary format
        for row in reader: # read a row as {column1: value1, column2: value2,...}
            for (k,v) in row.items(): # go over each column name and value
                columns[k].append(v) # append the value into the appropriate list
                                 # based on column name k

    L = columns['Dimension.LINE_ITEM_ID']
    print(len(L))

    s1 = ",".join(str(x) for x in L)
    s1="("+s1+")"

    return(s1)


def getDfpReport(id_lst):
  client = dfp.DfpClient.LoadFromStorage()
  print(client)
  # Initialize a report downloader.
  report_downloader = client.GetDataDownloader(version='v201702')

  line_items_file = tempfile.NamedTemporaryFile(
      prefix='line_items_', suffix='.csv', mode='w', delete=False)


  dt = "'2017'"
  line_items_pql_query = ("SELECT Id,Name,ExternalId,LineItemType,OrderId,StartDateTime,EndDateTime,IsMissingCreatives, Status,CostType,UnitsBought FROM Line_Item WHERE Id IN "+id_lst)

  print("DOWNLOADING REPORT")

  report_downloader.DownloadPqlResultToCsv(
      line_items_pql_query, line_items_file)


  line_items_file.close()


  return line_items_file.name


def upload_to_s3(aws_access_key_id, aws_secret_access_key, file, bucket, key, callback=None, md5=None, reduced_redundancy=False, content_type=None):
    """
    Uploads the given file to the AWS S3
    bucket and key specified.

    callback is a function of the form:

    def callback(complete, total)

    The callback should accept two integer parameters,
    the first representing the number of bytes that
    have been successfully transmitted to S3 and the
    second representing the size of the to be transmitted
    object.

    Returns boolean indicating success/failure of upload.
    """
    try:
        size = os.fstat(file.fileno()).st_size
    except:
        # Not all file objects implement fileno(),
        # so we fall back on this
        file.seek(0, os.SEEK_END)
        size = file.tell()

    conn = boto.connect_s3(aws_access_key_id, aws_secret_access_key)
    bucket = conn.get_bucket(bucket, validate=False)
    k = Key(bucket)
    k.key = "BUCKET_NAME/"+key

    print("UPLOADING FILE WITH KEY " + k.key)
    if content_type:
        k.set_metadata('Content-Type', content_type)
    sent = k.set_contents_from_file(file, cb=callback, md5=md5, reduced_redundancy=reduced_redundancy, rewind=True)

    # Rewind for later use
    file.seek(0)

    if sent == size:
        return True
    return False


# if __name__ == '__main__':
#   # Initialize client object.
#
#   print("Scheduler begun")
#   schedule.every(2).minutes.do(main)
#
#
#   while True:
#         schedule.run_pending()
#         time.sleep(1)

if __name__ == "__main__":

    from werkzeug.serving import run_simple
    run_simple('0.0.0.0', 5000, app, use_reloader=True, use_debugger=False, use_evalex=True,enable_threads=True) #main(dfp_client)
