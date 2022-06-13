def install_pip_pkg(required : set):
    import sys
    import subprocess
    import pkg_resources

    installed = {pkg.key for pkg in pkg_resources.working_set}
    missing = required - installed
    
    if missing:
        print(f'Trying to install {missing}')
        python = sys.executable
        output = None
        try:
            result = subprocess.check_call(
                [python, '-m', 'pip', 'install', *missing], 
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
        except subprocess.CalledProcessError as e:
            print(result.returncode, result.stdout, result.stderr)
            raise subprocess.CalledProcessError(e.output)
    
        
        print(f'Installation successful')

def load_or_execute_df(relative_path, func, args = None):
    import os
    from pandas import read_csv as pd_read_csv
    df = None
    if os.path.isfile(relative_path):
        df = pd_read_csv(relative_path, index_col=0)
    else:
        if args:
            df = func(**args)
        else:
            df = func()
        df.to_csv(relative_path)
    return df
        
class PrivateKeysHandler:
    def __init__(self, relative_path_to_file : str):
        from configparser import RawConfigParser
        import os
        self.configParser = RawConfigParser()   
        configFilePath = os.path.join(os.path.dirname(''), relative_path_to_file) 
        self.configParser.read(configFilePath)

    def load_keys(self, section : str) -> dict:
        return self.configParser[section]
    
class MyMySQLConnection:
    from pandas import DataFrame
    from sqlalchemy.engine.base import Connection
    def __init__(
            self, 
            credentials : dict, 
            db_name : str = 'gans_scooters'
            ):
                
        user = credentials['user']
        password = credentials['password']
        port = int(credentials['port'])
        host = credentials['hostname']
        dialect = credentials.get('dialect', 'mysql')
        driver = credentials.get('driver', 'pymysql')
        self.db_name = db_name
        
        # setup SQLAlchemy   
        from sqlalchemy import create_engine 
        if driver == 'pymysql': install_pip_pkg({'pymysql'})
        
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
            #con.execute(f"USE {self.db_name}")
            for table, (columns, args) in tables.items():
                
                #drop old table without checking for foreign keys (would throw error)
                con.execute('SET FOREIGN_KEY_CHECKS = 0')
                con.execute(f"DROP TABLE IF EXISTS {table}")
                con.execute('SET FOREIGN_KEY_CHECKS = 1')
                
                cols = ',\n'.join(map(lambda x: x[0] + ' ' + x[1], zip(columns, args)))
                con.execute(f"CREATE TABLE {table} ({cols});")
                 
    def execute(self, query : str):
        with self.alch_engine.begin() as con:
            con.execute(f'USE {self.db_name};')
            return con.execute(query)
        
        
    def get_connection(self):
        return self.alch_engine.connect()
                    
    def add_table_to_db(
            self, 
            df : DataFrame, 
            tablename : str, 
            insert_mode : str): 
             
        with self.alch_engine.begin() as con:
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
            insert_modes : list
            ):
                    
        assert(len(dfs) == len(tablenames))
        assert(len(dfs) == len(insert_modes))
        
        # 'begin' opens a transaction and the 'with' environment cares for a rollback if something 
        # goes wrong
        with self.alch_engine.begin() as con:
            remaining_calls = len(dfs)
            for i, df in enumerate(dfs):
                self.add_table_to_db(
                    df, 
                    tablenames[i], 
                    insert_modes[i]
                )

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
                
        # retrieve foreign key and send merged table to db
        with self.alch_engine.begin() as con:
            from pandas import read_sql as pd_read_sql
            from pandas import set_option as pd_set_option
            for i, foreigntable in enumerate(foreigntables):
                
                #retrieve foreign key
                tmp_df = pd_read_sql(f"SELECT {','.join(foreigncolumns[i])}, {','.join(matchcolumns[i])} FROM {foreigntable};", con)
                for j in range(len(foreigncolumns[i])):
                    
                    # suppress "A value is trying to be set on a copy of a slice from a DataFrame." warning
                    pd_set_option('mode.chained_assignment', None)
                    df.loc[:,foreigncolumns[i][j]] = df.merge(tmp_df, on=matchcolumns[i][j])[foreigncolumns[i][j]].copy()
                    pd_set_option('mode.chained_assignment', 'warn')
                    
                    # delete column that was used to match if it is not supposed to be inserted into the table
                    if not keepmatchcolumns[i][j]:
                        df = df.drop(columns=matchcolumns[i][j])
                        
            # send data to sql
            self.add_table_to_db(df, tablename, insert_mode)
