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
        
        install_pip_pkg({'mysql-connector-python'})
        import mysql.connector
        
        self.db_name = db_name
        user = credentials['user']
        password = credentials['password']
        
        #create database 
        self.connecter_cnx  = mysql.connector.connect(
          host="localhost",
          user=user,
          password=password
        )

        cursor = self.connecter_cnx .cursor()
        cursor.execute("SHOW DATABASES")
        if (self.db_name,) not in list(cursor):
            cursor.execute(f"CREATE DATABASE {self.db_name}")
        cursor.close()
    
        # setup SQLAlchemy      
        self.alch_cnx = f'mysql+pymysql://{user}:{password}@{host}:{port}/{self.db_name}'
        
    def __del__(self):
        self.connecter_cnx.close()
        
    from pandas import DataFrame
    def add_table_to_db(self, df : DataFrame):
        df.to_sql(self.db_name, 
              if_exists='append', 
              con=self.alch_cnx, 
              index=False)