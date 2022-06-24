# plots a spider plot for each cluster
# df_clusters = pd.DataFrame(kmeans.cluster_centers_)
def spider_plot(df_clusters):
    from pandas import melt as pd_melt
    from pandas import Series as pd_Series
    from seaborn import FacetGrid as sns_FacetGrid
    from matplotlib.pyplot import plot as plt_plot
    from math import pi
    
    df_polar = df_clusters.reset_index().rename(columns = {'index':'clusters'})

    # number of variable
    categories= df_polar.columns.tolist()[1:]
    N = len(categories)
    num_clusters = df_clusters.shape[0]
    
    # in order to have a line plot that goes full 360° we need to add the first value 
    # to the end as well; name of this last feature is chosen to stay last in the sorting below
    first_col = sorted(df_polar.columns.values[df_polar.columns.values != 'clusters'])[0]
    df_polar['z_endpoint'] = df_polar.loc[:,first_col]
    
    # all data points need to be in just one column -> df needs to be pivoted
    # after that df_polar_melted has the columns: clusters, originally (what feature 
    # the data point of that row originally belonged to), and data
    df_polar_melted = (pd_melt(
                          df_polar, 
                          id_vars=['clusters'], 
                          var_name='originally', 
                          value_name='data')
                       .sort_values(by = ['clusters', 'originally'])
                       .reset_index(drop=True)
                      )

    # What will be the angle of each axis in the plot? (we divide the plot / number of variable)
    # number of angles is number of feature columns + 1 so that the lineplot goes the full 360°
    # adding the angles to df_polar_melted relies upon that the dataframe is sorted by ['clusters', 'originally']
    angles = [n / float(N) * 2 * pi for n in range(N)] + [0]
    theta = pd_Series(angles * num_clusters, index = df_polar_melted.index)
    df_polar_melted['theta'] = theta

    # plot
    g = sns_FacetGrid(df_polar_melted, col="clusters", hue="clusters",
                      subplot_kws=dict(projection='polar'), height=4.5,
                      sharex=False, sharey=False, despine=False)
    g.map(plt_plot, 'theta', 'data', linewidth=1, linestyle=None)
    
    # customise each plot
    for i, ax in enumerate(g.axes.flat):
        
        # rotate by 90° counterclockwise
        ax.set_theta_offset(pi / 2)
        ax.set_theta_direction(-1)
        
        # add labels
        ax.set_xticks(angles[:-1])
        ax.set_xticklabels(sorted(categories))

        # fill in the area
        mask = df_polar_melted['clusters'] == i
        colour = ax.get_lines()[0].get_color()
        ax.fill(angles[:N+1], df_polar_melted.loc[mask, 'data'], color = colour, alpha=0.1)     
        
# show correlations of songs of a cluster in a heatmap
# pass all songs with a numerical column 'clusters'
def corr_heatmap(df):
    from seaborn import heatmap as sns_heatmap
    from seaborn import FacetGrid as sns_FacetGrid
    from pandas import concat as pd_concat
    
    num_clusters = max(df['clusters'])
    list_of_df = []
    for i in range(num_clusters + 1):
        tmp_df = df[df['clusters'] == i].drop(columns='clusters').corr()
        list_of_df.append(tmp_df)
    df_corr = pd_concat(list_of_df)
    df_corr = df_corr.assign(clusters = sorted(df_corr.shape[1] * list(range(num_clusters + 1))))
    
    def draw_heatmap(*args, **kwargs):
        data = kwargs.pop('data')
        sns_heatmap(data, vmin = -1, vmax = 1, cmap = "PiYG", square=True, annot= True)
        
    g = sns_FacetGrid(data = df_corr, col='clusters', height=7)#, aspect=1.5)
    g.map_dataframe(draw_heatmap,)

    for ax in g.axes.flat:
        # set aspect of all axis
        ax.set_aspect('equal','box')
        
    # calculate sums
    cols = df_corr.columns[~df_corr.columns.isin(['clusters', 'sum'])]
    df_corr['sum'] = df_corr.loc[:, cols].sum(axis=1)

    return df_corr