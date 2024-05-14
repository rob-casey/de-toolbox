# scripts/extract_data.py

import logging
from sqlalchemy import create_engine
import pandas as pd

# set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseConnector:
    def __init__(self, username, password, host, port, database):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.engine = None


    def connect(self):
        '''Create a database connection'''
        connection_string = f'postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}'
        try:
            self.engine = create_engine(connection_string)
            logger.info('Database connection established.')
        except Exception as e:
            logger.error('Failed to connect to the data base.')
            logger.error(e)
            self.engine = None

    def fetch_data(self, query):
        '''Fetch data from the database using provided SQL query'''
        if self.engine:
            try:
                df = pd.read_sql(query, self.engine)
                logger.info('Data fetched successfully.')
                return df
            except Exception as e:
                logger.error('Failed to fetch data from the database.')
                logger.error(e)
                return None
            else:
                logger.error('No database connection.')
                return None

def main():
    # Database credentials and details
    username = 'myuser'
    password = 'mypassword'
    host = 'localhost'
    port = '5432'
    database = 'mydatabase'

    # SQL query to fetch data
    query = 'SELECT * FROM my_table'

    # create a DatabaseConnector instance
    db_connector = DatabaseConnector(username, password, host, port, database)

    # connect to the database
    db_connector.connect()

    # fetch data
    df = db_connector.fetch_data(query)
    if df is not None:
        print(df.head())
    else:
        logger.error('Terminating script due to data fetch failure.')

if __name__ == '__main__':
    main()