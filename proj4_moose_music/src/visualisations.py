# plots a spider plot for each cluster
# df_clusters = pd.DataFrame(kmeans.cluster_centers_)
def spider_plot(df_clusters):
    from math import pi
    df_polar = df_clusters.reset_index().rename(columns = {'index':'clusters'})

    # number of variable
    categories= df_polar.columns.tolist()[1:]
    N = len(categories)

    first_col = sorted(df_polar.columns.values[df_polar.columns.values != 'clusters'])[0]
    df_polar['z_endpoint'] = df_polar.loc[:,first_col]
    #df_polar['zz_endpoint'] = df_polar.loc[:,first_col]
    df_polar_melted = pd.melt(
                              df_polar, 
                              id_vars=['clusters'], 
                              var_name='originally', 
                              value_name='data').sort_values(by = ['clusters', 'originally']).reset_index(drop=True)

    # What will be the angle of each axis in the plot? (we divide the plot / number of variable)
    angles = [n / float(N) * 2 * pi for n in range(N)] 
    angles += angles[:1]

    theta = pd.Series(angles * num_clusters, index = df_polar_melted.index)
    df_polar_melted['theta'] = theta

    g = sns.FacetGrid(df_polar_melted, col="clusters", hue="clusters",
                      subplot_kws=dict(projection='polar'), height=4.5,
                      sharex=False, sharey=False, despine=False)

    # Draw a scatterplot onto each axes in the grid
    g.map(plt.plot, 'theta', 'data', linewidth=1, linestyle=None)
    #g.map(sns.lineplot, 'theta', 'data')

    for i, ax in enumerate(g._axes[0]):
        ax.set_theta_offset(pi / 2)
        ax.set_theta_direction(-1)
        #ax.set_rlabel_position(0)
        ax.set_xticks(angles[:-1])
        ax.set_xticklabels(sorted(categories))
        #ax.set_yticks(ticks=None)#, labels=None)

        mask = df_polar_melted['clusters'] == i
        colour = ax.get_lines()[0].get_color()
        ax.fill(angles[:N+1], df_polar_melted.loc[mask, 'data'], color = colour, alpha=0.1)