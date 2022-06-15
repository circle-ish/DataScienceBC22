from typing import AbstractSet, Text, Callable, Mapping, Dict, TypeVar, Sequence, List, Tuple, \
                    Optional, Literal, NewType
from pandas import DataFrame
Yes_No_Literal = Literal['yes','no']

def install_pip_pkg(required : AbstractSet) -> None:
    import sys
    import subprocess
    import pkg_resources

    '''
    for debugging use subprocess.run(..., [check=True])
    check=True will raise error if problem occurs
    check=False will never raise an error
        check output with output = run(..., stdout=subprocess.PIPE) and
        output.stdout.decode("utf-8")
        equally stderr

    pip uninstall needs -y ; throws exit status 2 otherwise
    '''
    installed = {pkg.key for pkg in pkg_resources.working_set}
    missing = required - installed

    if missing:
        python = sys.executable
        command = [python, '-m', 'pip', 'install', *missing]
        try:
            subprocess.check_call(command)
        except subprocess.CalledProcessError as e: 
            
            #get more detailled error output
            output = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            print('\n\n'.join([
                f'Subprocess returncode : {output.returncode}', 
                f'stdout : {output.stdout.decode("utf-8")}', 
                f'stderr : {output.stderr.decode("utf-8")}']))
            raise subprocess.CalledProcessError(e)

        #load new modules into current pkg_resources.working_set
        import imp
        imp.reload(pkg_resources)

def load_csv_or_execute(
        __relative_path : Text, 
        __func : Callable[..., DataFrame], 
        args :  Mapping = dict(), 
        overwrite : Yes_No_Literal = 'no'
) -> DataFrame:

    import os
    from pandas import read_csv as pd_read_csv
    
    df : DataFrame 
    if overwrite == 'no' and os.path.isfile(__relative_path):
        df = pd_read_csv(__relative_path, index_col=0)
    else:
        df = __func(**args)
        df.to_csv(__relative_path)
    return df
        
DfCol = TypeVar('DfCol')
X = TypeVar('X', List[DfCol], Tuple[List[DfCol],List[DfCol],List[DfCol]])
def intersect_cols(
        __df_cols : Sequence[DfCol],
        __keep_cols : Sequence[DfCol],
        return_all : Yes_No_Literal = 'no') -> X:
    
    actual_cols = [col for col in __keep_cols if col in __df_cols]
    if return_all == 'no':
        return actual_cols
    
    keep_not_found = [col for col in __keep_cols if col not in actual_cols]
    remaining_df = [col for col in __df_cols if col not in actual_cols]
    
    return actual_cols, keep_not_found, remaining_df

def reload_module(args : Mapping[Text, Text]) -> None :
    # i.e. name='.scraping', package='src'
    # partial imports a la : from ... import ... are implicitely repeated
    from importlib import import_module, reload
    module = import_module(**args)
    reload(module)

class ConfigHandler:
    def __init__(self, relative_path_to_file : Text) -> None :
        from configparser import RawConfigParser
        import os
        
        self.configParser = RawConfigParser()   
        configFilePath = os.path.join(os.path.dirname(''), relative_path_to_file) 
        self.configParser.read(configFilePath)

    def load_config(self, section : Text) -> Dict[Text, Text] :
        return self.configParser[section]
    
class MyMySQLConnection:
    from pandas import DataFrame
    from sqlalchemy.engine.base import Connection
    
    def __init__(
            self, 
            __credentials : Mapping[Text, Text], 
            __db_name : Text ) -> None :
                
        user = __credentials['user']
        password = __credentials['password']
        port = __credentials['port']
        host = __credentials['hostname']
        dialect = __credentials.get('dialect', 'mysql')
        driver = __credentials.get('driver', 'pymysql')
        self.db_name = __db_name
        
        # setup SQLAlchemy   
        from sqlalchemy import create_engine 
        if driver == 'pymysql': install_pip_pkg({'pymysql'})
        cnx = f'{dialect}+{driver}://{user}:{password}@{host}:{port}/' #{__db_name}'
        self.alch_engine = create_engine(
            cnx, 
            connect_args={'connect_timeout': 10}, 
            echo=False
        ) 
        
        # create database if not already created
        with self.alch_engine.begin() as con:
            cursor = con.execute("SHOW DATABASES")
            if (self.db_name,) not in list(cursor):
                con.execute(f"CREATE DATABASE {self.db_name}")
            con.execute(f"USE {self.db_name}")
         
        
    MyComplicated = NewType('MyComplicated', Mapping[Text, Tuple[Sequence[Text], Sequence[Text]]])
    # tables = {table1 : ([columns], [args per column]),
    #           table2 : ([....
    # }
    # i.e
    # tables = {'Customers' : (['ID', 'Name',...], ['int NOT NULL AUTO_INCREMENT', 'varchar(255) NOT NULL']),
    #   ...}
    def create_tables(self, tables : MyComplicated) -> None:        
        with self.alch_engine.begin() as con:
            for table, (columns, args) in tables.items():
                
                #drop old table without checking for foreign keys (would throw error)
                con.execute('SET FOREIGN_KEY_CHECKS = 0', use_db_first = 'yes')
                con.execute(f"DROP TABLE IF EXISTS {table}")
                con.execute('SET FOREIGN_KEY_CHECKS = 1')
                
                cols = ',\n'.join(map(lambda x: x[0] + ' ' + x[1], zip(columns, args)))
                con.execute(f"CREATE TABLE {table} ({cols});")
                 
    def execute(self, query : Text, use_db_first : Yes_No_Literal = 'no'):
        with self.alch_engine.begin() as con:
            if use_db_first == 'yes':
                con.execute(f'USE {self.db_name};')
            return con.execute(query)
             
    def begin_connection(self) -> Connection:
        return self.alch_engine.begin()
                 
    def add_table_to_db(
            self, 
            df : DataFrame, 
            tablename : Text, 
            insert_mode : Text,
            external_con : Connection
    ) -> None : 
             
        # replace messes up the column types
        # instead emptying the table and then appending
        # keeps all the constraints
        if insert_mode == 'replace':
            external_con.execute(f"TRUNCATE TABLE {tablename}", use_db_first = 'yes')
            insert_mode = 'append'

        df.to_sql(
            name= tablename,
            if_exists=insert_mode, #'replace'
            con=external_con, 
            schema=self.db_name,
            index=False,
            chunksize=1000
        )           
                
    def add_tables_to_db(
            self, 
            dfs : Sequence[DataFrame], 
            tablenames : Sequence[Text], 
            insert_modes : Sequence[Text]
            ) -> None :
                    
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
                    insert_modes[i],
                    con
                )

    SeqSeqTex = NewType('SeqSeqTex', Sequence[Sequence[Text]])
    #extracts foreign keys into df before adding to db
    def add_to_db_with_foreign_key(
            self, 
            df : DataFrame,
            tablename : Text,
            foreigntables : Sequence[Text], # list of all tables with a foreign key that need to be extracted
            foreigncolumns : SeqSeqTex, # list of lists: naming the columns to extract for each table
            matchcolumns : SeqSeqTex, # list of lists: naming the columns to merge on for each table; 
                                 # needs to be the same name in the foreign table and df 
            insert_mode : Text,                                      
            dropcolumns : Optional[SeqSeqTex] = None) -> None : # list of lists: list of column names or empty 
                                        # list for each table that should be dropped after merging before
                                        # pushing to sql; does not drop by default
             
        # check for correct type and equal lengths
        assert(len(foreigntables) == len(foreigncolumns))
        assert(len(foreigncolumns) == len(matchcolumns))
        for i in range(len(foreigncolumns)):
            assert(len(foreigncolumns[i]) == len(matchcolumns[i]))
        
        if dropcolumns: # enters if not None
            assert(len(dropcolumns) == len(matchcolumns))
            for i in range(len(matchcolumns)):
                assert(len(dropcolumns[i]) == len(matchcolumns[i]))
                
        # retrieve foreign key and send merged table to db
        with self.alch_engine.begin() as con:
            from pandas import read_sql as pd_read_sql
            from pandas import set_option as pd_set_option
            
            # got random errors on AWS Lambda about unknown database; this solved it
            con.execute(f'USE {self.db_name};')
            new_df = df.copy()
            for i, foreigntable in enumerate(foreigntables):
                
                #retrieve foreign key
                tmp_df = pd_read_sql(f"SELECT {','.join(foreigncolumns[i])}, {','.join(matchcolumns[i])} FROM {foreigntable};", con)
                for j in range(len(foreigncolumns[i])):
                    
                    # suppress "A value is trying to be set on a copy of a slice from a DataFrame." warning
                    # since it does not apply here
                    pd_set_option('mode.chained_assignment', None)
                    new_df[foreigncolumns[i][j]] = df.merge(tmp_df, on=matchcolumns[i][j])[foreigncolumns[i][j]].copy()
                    pd_set_option('mode.chained_assignment', 'warn')
                    
                # delete column that was used to match if it is not supposed to be inserted into the table
                if dropcolumns and dropcolumns[i]: # enters if not None and the list not empty
                    new_df.drop(columns=dropcolumns[i], inplace=True)
                        
            # send data to sql
            self.add_table_to_db(new_df, tablename, insert_mode, con)
