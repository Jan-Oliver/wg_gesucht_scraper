import pandas as pd
import json

from sqlalchemy import create_engine
import pathlib

PARENT_DIR = pathlib.Path(__file__).parent.resolve()

class DatabaseTable:
    """Wrapper to upload and retrieve data to database.

    Utillity functions to upload and download all or just specific 
    columns of table in database.

    Typical usage example:
    from util import database_table
    table_name = <Name of table in database>

    table = database_table.DatabaseTable(table_name)
    table.<Whatever function>()
    """
    def __init__(self,table_name: str):
        ''' Init with name of table in database '''
        with open(str(PARENT_DIR) + '/database_config.json') as json_file:
            data = json.load(json_file)

        db_connection_address = data["dialect_driver"] + "://" + data["user"] \
            + ":" + data["password"] + "@" + data["hostname"] + "/" + data["dbname"]
        self._sql_engine = create_engine(db_connection_address, echo=False)
        self._table_name = table_name
        pass

    def upload_df_to_database(self, df: pd.DataFrame, if_exists: str="append"):
        """Uploads df to table of database by appending or replacing.

        A dataframe is uploaded to the database and either appended to or
        replaces the existing data. If the table is empty or not existing,
        a new one is created automatically.

        Args:
            df: Dataframe to upload to the database.
            if_exists: What to do if table already contains data. Either "append" or
                "replace".
        """
        df.to_sql(self._table_name, self._sql_engine,
                  method="multi", chunksize=10000, if_exists=if_exists,index=False)
        print("Successfully appended dataframe to database")


    def get_existing_ad_ids(self) -> list:
        """Retrieves ad ids for active ads from table.

        Gets ids for all ads that are classified to be active. Converts them into a list.
        If table does not exist yet, an empty list is returned.

        Returns:
            ids_active_ads: List of ids for all active ads. If there are no active ads or
                the table is not yet created an empty list is returned.
        """
        print("Loading ids for active ads from table %s." % self._table_name)        
        try:
            df = pd.read_sql("SELECT ad_id FROM " + str(self._table_name) + " WHERE is_active = True", self._sql_engine)
            print("Success!")
        except:
            print("Datatable does not yet exist in database. Returning empty list")
            empty_list = list()
            return empty_list

        ids_active_ads = df["ad_id"].to_list()
        return ids_active_ads
        
    def get_dataframe_active(self) -> pd.DataFrame:
        """Retrieves dataframe with active ads from table.

        Gets all ads that are classified to be active. Converts them into a dataframe.
        No validation is performed if table exists.

        Returns:
            df: Retrieved pandas Dataframe with all active ads.
        """
        print("Loading data from table %s." % self._table_name)

        df = pd.read_sql("SELECT * FROM " + str(self._table_name) + " WHERE is_active = True", self._sql_engine)
        print("Success!")
        return df

    def get_dataframe(self) -> pd.DataFrame:
        """Retrieves dataframe from table.

        Gets all ads that are stored in the database. Converts them into a dataframe.
        No validation is performed if table exists.

        Returns:
            df: Retrieved pandas Dataframe with all ads.
        """
        print("Loading data from table %s." % self._table_name)

        df = pd.read_sql("SELECT * FROM " + str(self._table_name), self._sql_engine)
        print("Success!")
        return df