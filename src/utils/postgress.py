import psycopg2
import sqlalchemy
from sqlalchemy import create_engine, exc
import os

from dotenv import load_dotenv

load_dotenv()

PG_ADDRESS = os.getenv("PG_ADDRESS")
PG_DB = os.getenv("PG_DB")
PG_USER=os.getenv("PG_USER")
PG_PW = os.getenv("PG_PW")

engine_params = f'postgresql+psycopg2://{PG_USER}:{PG_PW}@{PG_ADDRESS}/{PG_DB}'

alchemyEngine = False
dbConnection = False

def pg_connection():
    print("Getting postgress db connection")
    global alchemyEngine, dbConnection
    print(dbConnection)
    if dbConnection:
        return dbConnection
    else:
        if alchemyEngine == False:
            print("trying to initialize a new engine")
            try:
                alchemyEngine = create_engine(engine_params, pool_recycle=3600)
                dbConnection = alchemyEngine.connect()
                return dbConnection
            except exc.OperationalError as err:
                print("Error creating db")
                print(err)
                return False
        

def disconnect():
    global alchemyEngine, dbConnection
    print("disconnecting db")
    if dbConnection:
        print("Closing connection.")
        dbConnection.close()
        dbConnection = False
    if alchemyEngine:
        print("Disposing engine.")
        alchemyEngine.dispose()
        alchemyEngine = False

