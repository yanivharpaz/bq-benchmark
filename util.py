
def get_pickle_path():
    return "/python/terminalx/misc/TerminalX-ACS-IL/data/"

def get_files_list(sDate, iChunks, sPicklePath):
    lFiles = []
    for iFileCounter in range(1, iChunks + 1):
        sFileName     = str(iFileCounter).zfill(5)
        sFullFileName = f"{sPicklePath}BQ-{sDate}-{sFileName}.pkl"
        lFiles.append(sFullFileName)

    return lFiles

def pickle_driver(sDate, iChunks, sTableName):    
    DriverStartTime   = time.time()
    sPicklePath       = get_pickle_path()
    dtype             = get_schema_dtype()
    sConnectionString = get_connection_string()

    engine            = sa.create_engine(sConnectionString, arraysize=1000 ,pool_size=100, max_overflow=200)

    lFiles            = get_files_list(sDate, iChunks, sPicklePath)
    print(f"Amount:{len(lFiles)} | Path:{sPicklePath} ")
    oACSLogger.logger.info(f"Amount:{len(lFiles)} | Path:{sPicklePath} ")

    for sFile in lFiles:
        startTime = time.time()
        oACSLogger.logger.info(sFile)
        print(f"{sFile.split('/')[-1]}", end="")
        dData = pd.read_pickle(sFile)
        print(f" | Rows: {dData.shape[0]} | File Time: {time.time() - startTime:04.0f}", end="")
        dData.to_sql(sTableName, engine.connect(), if_exists='append', chunksize=1000, dtype=dtype)
        oACSLogger.logger.info(f"Rows: {dData.shape[0]} | DB Load Time: {time.time() - startTime:04.2f}")
        print(f" | DB Load Time: {time.time() - startTime:04.2f}", end="")
        print("")

    print(f"DB Total Load Time: {time.time() - DriverStartTime:04.2f}")
