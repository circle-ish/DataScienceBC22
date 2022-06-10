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
    
class MyMySQLConnection:
    def __init__(
            self, 
            credentials : dict, 
            db_name : str = 'gans_scooters',
            host : str = "127.0.0.1",
            port : int = 3306):
                
        user = credentials['user']
        password = credentials['password']
        
        # setup SQLAlchemy   
        from sqlalchemy import create_engine 
        install_pip_pkg({'pymysql'})
        
        dialect = 'mysql'
        driver = 'pymysql'
        cnx = f'{dialect}+{driver}://{user}:{password}@{host}:{port}/' #{db_name}'
        self.alch_engine = create_engine(
            cnx, 
            connect_args={'connect_timeout': 10}, 
            echo=False
        ) 
        
        # test if database is already created
        with self.alch_engine.begin() as con:
            cursor = con.execute("SHOW DATABASES")
            if (db_name,) not in list(cursor):
                con.execute(f"CREATE DATABASE {db_name}")
                
    def add_tables_to_db(
            self, 
            dfs : list, 
            tablenames : list, 
            insert_modes : list,
            id_columns : list = None
            ):
        
        assert(len(dfs) == len(tablenames))
        assert(len(dfs) == len(id_columns))
        assert(len(dfs) == len(insert_modes))
        
        if not id_columns: # only goes in if None
            id_columns = [None] * len(dfs)
        
        # 'begin' opens a transaction and the 'with' environment cares for a rollback if something 
        # goes wrong
        with self.alch_engine.begin() as con:
            for i, df in enumerate(dfs):
                df.to_sql(
                    name= tablenames[i],
                    if_exists=insert_modes[i], #'append', #'replace'
                    con=con, 
                    index=False,
                    chunksize=1000
                )

                if id_columns[i]: #only enter if not None
                    con.execute(f'ALTER TABLE `{tablenames[i]}` ADD PRIMARY KEY (`{id_columns[i]}`);')
          
    
    from pandas import DataFrame
    def add_table_to_db(self, df : DataFrame, tablename : str, id_column : str):
        with self.alch_engine.begin() as con:
            df.to_sql(
                #schema="dbo"
                name= tablename,
                if_exists='append', #'replace'
                con=con, 
                index=False,
                chunksize=1000
            )
    
            con.execute(f'ALTER TABLE `{tablename}` ADD PRIMARY KEY (`{id_column}`);')
              #not null, unique, default, primary key, foreign key
            
            