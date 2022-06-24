# tests if min/max-values of numerical columns are within limits passed by 
# scale_dict = {'column name 1': (min, max), 'column name 2 : ...}
# all columns not named explicitly are checked against key 'default'
from pandas import DataFrame as pd_DataFrame
from typing import Mapping
def test_num_data_scale(df_num : pd_DataFrame, scale_dict : Mapping) -> None:
    #desired min/max
    _df_scale = pd_DataFrame(data = scale_dict,
                          index=['min', 'max'])
  
    # actual min/max of data
    _df_num_minmax = df_num.describe().loc[['min', 'max'],:]

    # set min/max default for columns not explicitly named in scale_dict 
    # implicitly creates new columns via new_cols
    new_cols = _df_num_minmax.columns[~_df_num_minmax.columns.isin(_df_scale.columns)]
    _df_scale.loc[:, new_cols] = pd_DataFrame(
                                        data = [[0] * len(new_cols)] * 2, 
                                        columns = new_cols, 
                                        index=['min','max']
                                ).add(_df_scale.loc[:, 'default'], axis=0)
    
    _df_scale.drop(columns = 'default', inplace=True)
    
    from src.utils import intersect_cols
    cols, _, remaining_df = intersect_cols(_df_scale.columns, _df_num_minmax.columns, return_all = 'yes')
    if remaining_df:
        print(f'test_num_data_scale: Columns {remaining_df} not found in passed df_num.')

    if _df_scale[cols].shape[1] != _df_num_minmax[cols].shape[1]:
            raise RuntimeError('test_num_data_scale: DataFrames do not have the same length')
            
    if not all(_df_scale.loc['min',cols] <= _df_num_minmax.loc['min', cols]):
            raise RuntimeError(f"test_num_data_scale: columns {pd_DataFrame(_df_num_minmax.loc['min', (_df_scale.loc['min',cols] > _df_num_minmax.loc['min', cols])].to_frame().T).columns.tolist()} have forbidden min values")
            
    if not all(_df_scale.loc['max',cols] >= _df_num_minmax.loc['max', cols]):
            raise RuntimeError(f"test_num_data_scale: columns {pd_DataFrame(_df_num_minmax.loc['max', (_df_scale.loc['max',cols] < _df_num_minmax.loc['max', cols])].to_frame().T).columns.tolist()} have forbidden max values")
