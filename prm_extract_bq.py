#!/usr/bin/python3
#%% 

import pandas           as pd
import numpy            as np
import sqlalchemy       as sa
from google.cloud   import bigquery
from ACSLogging     import *

import time
import datetime
import pyarrow
import uuid
import json
import sys 
import cx_Oracle

# %load_ext google.cloud.bigquery

oACSLogger = ACSLogger(name="extract_bq")

def get_uuid() -> str:
    return str(uuid.uuid4())


def get_day_id_list(sDate="20210708", iRowsLimit=2000, startTime=time.time()):

    sQuery     = f"""
    SELECT  visitId
            ,date as bq_date_raw
    FROM    `terminalx-221509.151029686.ga_sessions_{sDate}` a
    LIMIT   {iRowsLimit}
    ;
    """ 

    client    = bigquery.Client()

    print("Starting access to BQ", time.time() - startTime, datetime.datetime.now())
    print(f"Extract Date: {sDate} | Rows Limit: {iRowsLimit:,}")
    # print("Limit: \t\t", iRowsLimit)

    dStgData = (client.query(sQuery).result().to_dataframe())
    print(dStgData.shape)
    print(f"Master data load from BQ Time: {time.time() - startTime:04.2f} {datetime.datetime.now()}")

    return dStgData

def extract_day_by_visitId(sDate="20210708", iRowsLimit=2000,iLow=0, iHigh=9**999,startTime=time.time()):

    sQuery     = f"""
    SELECT visitorId
            ,visitNumber
            ,visitId
            ,visitStartTime
            ,date as bq_date_raw
            ,fullVisitorId,userId,clientId,channelGrouping,socialEngagementType
            ,to_json_string(totals)             as totals
            ,to_json_string(trafficSource)      as trafficSource
            ,to_json_string(device)             as device
            ,to_json_string(geoNetwork)         as geoNetwork
            ,to_json_string(customDimensions)   as customDimensions
            ,to_json_string(hits)               as hits
    FROM    `terminalx-221509.151029686.ga_sessions_{sDate}` a
    WHERE   visitId >= {iLow}
    AND     visitId <= {iHigh}
    LIMIT   {iRowsLimit}
    ;
    """ 

    # startTime = time.time()
    client    = bigquery.Client()

    # print("Starting access to BQ", time.time() - startTime, datetime.datetime.now())
    print(f"| Low:{iLow} | High:{iHigh}", end="")

    dStgData = (
        client.query(sQuery)
        .result()
        .to_dataframe(
            # Optionally, explicitly request to use the BigQuery Storage API. As of
            # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
            # API is used by default.
            # create_bqstorage_client=False,
        )
    )
    print(" | Rows:" f"{dStgData.shape[0]:04d} | BQ: {time.time() - startTime:04.0f} ", end="")
    oACSLogger.logger.info(f"{sDate} | Rows: {dStgData.shape[0]:04d} | BQ: {time.time() - startTime:04.0f} ")

    return dStgData

def extract_day(sDate="20210708", iRowsLimit=2000, startTime=time.time()):

    sQuery     = f"""
    SELECT visitorId
            ,visitNumber
            ,visitId
            ,visitStartTime
            ,date as bq_date_raw
            ,fullVisitorId,userId,clientId,channelGrouping,socialEngagementType
            ,to_json_string(totals)             as totals
            ,to_json_string(trafficSource)      as trafficSource
            ,to_json_string(device)             as device
            ,to_json_string(geoNetwork)         as geoNetwork
            ,to_json_string(customDimensions)   as customDimensions
            ,to_json_string(hits)               as hits
    FROM    `terminalx-221509.151029686.ga_sessions_{sDate}` a
    LIMIT   {iRowsLimit}
    ;
    """ 

    # startTime = time.time()
    client    = bigquery.Client()

    print("Starting access to BQ", time.time() - startTime, datetime.datetime.now())
    print("Extract Date:  \t", sDate)
    print("Limit: \t\t", iRowsLimit)

    dStgData = (
        client.query(sQuery)
        .result()
        .to_dataframe(
            # Optionally, explicitly request to use the BigQuery Storage API. As of
            # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
            # API is used by default.
            # create_bqstorage_client=False,
        )
    )
    print("Dataframe loading from BQ complete ", time.time() - startTime, datetime.datetime.now())

    return dStgData

def transform_dataframe(dData, startTime):
    dData['visitorId']    = dData['visitorId'].astype(str)
    dData['visitNumber']  = dData['visitNumber'].astype(str)
    dData['visitId']      = dData['visitId'].astype(str)
    # dData['visitStartTime']  = dData['visitStartTime'].astype(str)
    dData['VISIT_DATE']   = pd.to_datetime(dData['bq_date_raw'].str[:4] + '-' + dData['bq_date_raw'].str[4:6] + '-' + dData['bq_date_raw'].str[6:8] )
    dData['INSERT_DATE']  = datetime.datetime.now()
    dData.rename(columns={
        'visitorId'               : 'VISITOR_ID',
        'visitNumber'             : 'VISIT_NUMBER',
        'visitId'                 : 'VISIT_ID',
        'visitStartTime'          : 'VISIT_START_TIME',
        'fullVisitorId'           : 'FULL_VISITOR_ID',
        'userId'                  : 'USER_ID',
        'clientId'                : 'CLIENT_ID',
        'channelGrouping'         : 'CHANNEL_GROUPING',
        'socialEngagementType'    : 'SOCIAL_ENGAGEMENT_TYPE',
        'totals'                  : 'JSON_TOTALS',
        'trafficSource'           : 'JSON_TRAFFICSOURCE',
        'device'                  : 'JSON_DEVICE',
        'geoNetwork'              : 'JSON_GEONETWORK',
        'customDimensions'        : 'JSON_CUSTOMDIMENSIONS',
        'hits'                    : 'JSON_HITS',
    }, errors='raise', inplace=True)
    dData.drop(['bq_date_raw'], axis=1, inplace=True)

    return dData


def write_data_file(dData, sDate, iRowsLimit, startTime=time.time(), bWriteCSV=True, bWritePickle=True, iChunkNo=1):
    sUUID             = get_uuid()
    sChunk            = str(iChunkNo).zfill(5)

    sFileName         = f'{sDate}-{sChunk}'
    sPickleFileName   = f'data/BQ-{sFileName}.pkl'
    sCSVFileName      = f'data/BQ-CSV-{sFileName}.csv'
    sJSONFileName     = f'data/BQ-JSON-{sFileName}.json'

    if bWritePickle:
        dData.to_pickle(sPickleFileName)
        oACSLogger.logger.info("| PKL: " f" {time.time() - startTime:04.0f} {sPickleFileName}")

        # oACSLogger.logger.info("| PKL: " f"{time.time() - startTime:04.0f}")
        print("| PKL: " f"{time.time() - startTime:04.0f}", end="")

    # print(f"Writing CSV file \t {datetime.datetime.now()} {time.time() - startTime:.2f}")
    if bWriteCSV:
        dData.to_csv(sCSVFileName)
        oACSLogger.logger.info("| CSV: " f"{time.time() - startTime:04.0f}")
        print("| CSV: " f"{time.time() - startTime:04.0f}", end="")

    # dData.to_json(sJSONFileName)

    # print(f"Load from BQ complete \t {datetime.datetime.now()} {time.time() - startTime:.2f}")

    return dData


def split_dataframe_by_id(dData, sColumnName, iChunks):
    dTmpData = dData.sort_values(by=sColumnName)
    lChunks = [(list(chunk)[0], list(chunk)[-1]) for chunk in np.array_split(dTmpData[sColumnName].unique(), iChunks)]
    return lChunks

def driver(sDate, iChunks):
    MainStartTime = time.time()

    iRowsLimit    = 900000
    # iChunks       = 20
    sExistsAction = 'append'
    sTableName    = "bq_day_stg"


    dData      = get_day_id_list      (       sDate, iRowsLimit, MainStartTime)
    lChunks    = split_dataframe_by_id(dData, 'visitId', iChunks)
    print(f"Chunks: {len(lChunks)}")
    oACSLogger.logger.info(f"Chunks: {len(lChunks)} Total Daily Rows:{len(dData)}")

    for iChunk, chunk in enumerate(lChunks):
        startTime = time.time()
        print(f"{iChunk + 1:03d}/{len(lChunks)}", end="")
        oACSLogger.logger.info(f"{iChunk + 1:03d}/{len(lChunks)}")
        dData = extract_day_by_visitId(sDate, iRowsLimit, iLow=chunk[0], iHigh=chunk[1], startTime=startTime)
        dData = transform_dataframe   (dData                                           , startTime=startTime)
        dData = write_data_file       (dData, sDate, iRowsLimit                        , startTime=startTime, bWriteCSV=False, iChunkNo=(iChunk+1))
        print("")
    # dData      = extract_day        (       sDate, iRowsLimit, MainStartTime)
    # dData      = transform_dataframe(dData                   , MainStartTime)
    # dData      = write_data_file    (dData, sDate, iRowsLimit, MainStartTime)
    # print(dData.columns)
    # _          = write_to_adw       (dData, sTableName       , MainStartTime, sExistsAction=sExistsAction)

    print                 (f"Complete process time: \t {datetime.datetime.now()} {time.time() - MainStartTime:.2f} {(time.time() - MainStartTime) / 60:.2f}")
    oACSLogger.logger.info(f"Complete process time: \t {datetime.datetime.now()} {time.time() - MainStartTime:.2f} {(time.time() - MainStartTime) / 60:.2f}")

    return dData

def main(iStartDate, iEndDate, iChunks=20):
    sYear         = "2021"
    sMonth        = "07"
    sDay          = "04"
    sDate         = f"{sYear}{sMonth}{sDay}"

    # driver(sDate)
    oACSLogger.logger.info(f"{iStartDate} - {iEndDate} Chunks: {iChunks}")

    for iDay in range(iStartDate, (iEndDate + 1)):
        oACSLogger.logger.info(f"Processing {iDay}")
        driver(iDay, iChunks)


if __name__ == "__main__":
    print('--- extract_bq ---')
    if len(sys.argv) < 4:
        print("Usage:               extract_bq.py <Start Date> <End Date> <Chunks>")
        print("Example:             extract_bq.py 20210701 20210702 20"      )
        print("Recommended: nohup ./extract_bq.py 20210812 20210816 20 &"    )
        sys.exit()

    try:
        iStartDate = int(sys.argv[1])
        iEndDate   = int(sys.argv[2]) 
        iChunks    = int(sys.argv[3]) 
    except BaseException as err:
        print(f"Error in args {sys.argv[1]} | {sys.argv[2]}")
        oACSLogger.logger.error(f"Error in args {sys.argv[1]} | {sys.argv[2]}")
        sys.exit(2)

    if iStartDate > iEndDate:
        print(f"End date is before the start date {sys.argv[1]} | {sys.argv[2]}")
        oACSLogger.logger.errorjobs(f"End date is before the start date {sys.argv[1]} | {sys.argv[2]}")
        sys.exit(2)

    main(iStartDate, iEndDate, iChunks)



# %%
