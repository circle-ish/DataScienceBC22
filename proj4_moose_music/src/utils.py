from typing import AbstractSet, Text, Callable, Mapping, Dict, TypeVar, Sequence, List, Tuple, \
                    Optional, Literal, NewType
from pandas import DataFrame
Yes_No_Literal = Literal['yes','no']

#########################################################################
##########     REGARDING MODULES ###########################
#########################################################################
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

def reload_module(args : Mapping[Text, Text]) -> None :
    # i.e. name='.scraping', package='src'
    # partial imports a la : from ... import ... are implicitely repeated
    from importlib import import_module, reload
    module = import_module(**args)
    reload(module)
    

#########################################################################
##########     REGARDING LOADING FILES ###########################
#########################################################################

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
        
class ConfigHandler:
    def __init__(self, relative_path_to_file : Text) -> None :
        from configparser import RawConfigParser
        import os
        
        self.configParser = RawConfigParser()   
        configFilePath = os.path.join(os.path.dirname(''), relative_path_to_file) 
        self.configParser.read(configFilePath)

    def load_config(self, section : Text) -> Dict[Text, Text] :
        return self.configParser[section]

    
#########################################################################
##########     REGARDING PANDAS ###########################
#########################################################################

# takes to lists of columns and either returns the intersection or
# for return_all = 'yes' also the columns that were in either dataset but not the other
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

def get_numerical_columns(df):
    types = df.dtypes.astype('str')
    mask = (types.str.contains('float') | types.str.contains('int')).values
    return types.loc[mask].index.tolist()