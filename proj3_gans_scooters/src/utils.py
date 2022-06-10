def install_pip_pkg(required : set):
    import sys
    import subprocess
    import pkg_resources

    installed = {pkg.key for pkg in pkg_resources.working_set}
    missing = required - installed

    if missing:
        print(f'Trying to install {missing}')
        python = sys.executable
        subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)
        print(f'Installation successful')

        
class PrivateKeysHandler:
    def __init__(self, relative_path_to_file : str):
        from configparser import RawConfigParser
        import os
        self.configParser = RawConfigParser()   
        configFilePath = os.path.join(os.path.dirname(''), relative_path_to_file) 
        self.configParser.read(configFilePath)

    def load_keys(self, section : str) -> dict:
        return self.configParser[section]
    
import sys, os
sys.path.append(os.path.join(os.path.dirname(''), '../..'))
from proj3_gans_scooters.src.utils import install_pip_pkg
    
class yMySQLConnection:
    from pandas import DataFrame
    from sqlalchemy.engine.base import Connection
    def __init__(
            self, 
            credentials : dict, 
            db_name : str = 'gans_scooters',
            host : str = "127.0.0.1",
            port : int = 3306):
                
        user = credentials['user']
        password = credentials['password']
        self.db_name = db_name
        
        # setup SQLAlchemy   
        from sqlalchemy import create_engine 
        install_pip_pkg({'pymysql'})
        
        dialect = 'mysql'
        driver = 'pymysql'
        cnx = f'{dialect}+{driver}://{user}:{password}@{host}:{port}/' #{db_name}'
        
        # create database if not already created
        self.alch_engine = create_engine(
            cnx, 
            connect_args={'connect_timeout': 10}, 
            echo=False
        ) 
        with self.alch_engine.begin() as con:
            cursor = con.execute("SHOW DATABASES")
            if (self.db_name,) not in list(cursor):
                con.execute(f"CREATE DATABASE {self.db_name}")
            con.execute(f"USE {self.db_name}")
                
    # tables = {table1 : ([columns], [args per column]),
    #           table2 : ([....
    # }
    # i.e
    # tables = {'Customers' : (['ID', 'Name',...], ['int NOT NULL AUTO_INCREMENT', 'varchar(255) NOT NULL']),
    #   ...}
    def create_tables(self, tables : dict):        
        with self.alch_engine.begin() as con:
            for table, (columns, args) in tables.items():
                con.exec_driver_sql(f"DROP TABLE IF EXISTS {table}")
                cols = ',\n'.join(map(lambda x: x[0] + ' ' + x[1], zip(columns, args)))
                con.execute(f"CREATE TABLE {table} ({cols});")
                 
    def execute(self, query : str):
        with self.alch_engine.begin() as con:
            return con.execute(query)
                    
    def add_table_to_db(
            self, 
            df : DataFrame, 
            tablename : str, 
            insert_mode : str,
            con : Connection = None):
        
        with con if con else self.alch_engine.begin() as con:
            # replace messes up the column types
            # instead emptying the table and then appending
            # keeps all the constraints
            if insert_mode == 'replace':
                con.execute(f"TRUNCATE TABLE {tablename}")
                insert_mode = 'append'

            df.to_sql(
                #schema="dbo"
                name= tablename,
                if_exists=insert_mode, #'replace'
                con=con, 
                schema=self.db_name,
                index=False,
                chunksize=1000
            )
            
    def add_tables_to_db(
            self, 
            dfs : list, 
            tablenames : list, 
            insert_modes : list,
            id_columns : list = None
            ):
        
        if not id_columns: # only goes in if None
            id_columns = [None] * len(dfs)
            
        assert(len(dfs) == len(tablenames))
        assert(len(dfs) == len(id_columns))
        assert(len(dfs) == len(insert_modes))
        
        # 'begin' opens a transaction and the 'with' environment cares for a rollback if something 
        # goes wrong
        with self.alch_engine.begin() as con:
            for i, df in enumerate(dfs):
                self.add_table_to_db(df, tablenames[i], insert_modes[i], con)

    #extracts foreign keys into df before adding to db
    def add_to_db_with_foreign_key(
            self, 
            df : DataFrame,
            tablename : str,
            foreigntables : list, # list of all tables with a foreign key that need to be extracted
            foreigncolumns : list, # list of lists: naming the columns to extract for each table
            matchcolumns : list, # list of lists: naming the columns to merge on for each table; 
                                 # needs to be the same name in the foreign table and df 
            insert_mode : str,                                      
            keepmatchcolumns : list = None): # list of lists: bool if matchcolumns in df should be kept; 
                                        # does not keep by default; if a column needs to be used several times
                                        # only pass False for the last occurence
             
        # check for correct type and equal lengths
        assert(len(foreigntables) == len(foreigncolumns))
        assert(len(foreigncolumns) == len(matchcolumns))
        for i in range(len(foreigncolumns)):
            if not isinstance(foreigncolumns[i], list):
                foreigncolumns[i] = list(foreigncolumns[i])
            if not isinstance(matchcolumns[i], list):
                matchcolumns[i] = list(matchcolumns[i])
            assert(len(foreigncolumns[i]) == len(matchcolumns[i]))
        
        if not keepmatchcolumns: # enters if None 
            from copy import deepcopy
            keepmatchcolumns = deepcopy(matchcolumns)
            for i in range(len(matchcolumns)):
                for j in range(len(matchcolumns[i])):
                    keepmatchcolumns[i][j] = False
        else:
            assert(len(keepmatchcolumns) == len(matchcolumns))
            for i in matchcolumns:
                assert(len(keepmatchcolumns[i]) == len(matchcolumns[i]))
                
        with self.alch_engine.begin() as con:
            for i, foreigntable in enumerate(foreigntables):
                tmp_df = pd.read_sql(f"SELECT {','.join(foreigncolumns[i])}, {','.join(matchcolumns[i])} FROM {foreigntable};", con)
                for j in range(len(foreigncolumns[i])):
                    df[foreigncolumns[i][j]] = df.merge(tmp_df, on=matchcolumns[i][j])[foreigncolumns[i][j]]
                    if not keepmatchcolumns[i][j]:
                        df = df.drop(columns=matchcolumns[i][j])
            self.add_table_to_db(df, tablename, insert_mode)
