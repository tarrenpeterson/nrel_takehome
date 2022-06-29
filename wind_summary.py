import sys
import os
import h5pyd
import numpy as np
import pandas as pd
import geopandas as gpd
import dask.dataframe as dd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.lines import Line2D
from scipy.spatial import cKDTree
from shapely.geometry import Point
from dask.distributed import Client, LocalCluster
from dask_jobqueue import SLURMCluster
from prettytable import PrettyTable


def extract_points(geo_type, geo_file):
    '''
    A function that takes an input geojson file and extracts points from that file

    Inputs:
        geo_type: (str) a string that dictates whether the file contains information on a point or on a polygon
        geo_file: (str) file path to the geojson file that contains the geometry of interest
    
    Outputs:
        (list of tuples) a list of tuples representing x, y coordinates for all points generated in this function
    '''
    geometry = gpd.read_file(geo_file)

    if geo_type == 'point':
        lst_pts = list(zip(geometry['geometry'].x, geometry['geometry'].y))

    elif geo_type == 'polygon':
        bounds = geometry.bounds
        xmin = float(bounds['minx'])
        ymin = float(bounds['miny'])
        xmax = float(bounds['maxx'])
        ymax = float(bounds['maxy'])        
        xc = (xmax - xmin) * np.random.random(2000) + xmin
        yc = (ymax - ymin) * np.random.random(2000) + ymin
        pts = gpd.GeoSeries([Point(x, y) for x, y in zip(xc, yc)])
        pts = pts.unary_union
        relevant_pts = geometry['geometry'].intersection(pts)
        lst_pts = list(zip(relevant_pts.explode().x, relevant_pts.explode().y))
    
    else:
        raise Exception('The geometry type must be specified as either a point or polygon')

    return lst_pts


def nearest_site(tree, lat_coord, lon_coord):
    '''
    A function to find the nearest site with data from the wind data set

    Inputs:
        tree: cKD tree using wind data set
        lat_coord: (int) latitude of point of interest
        lon_coord: (int) longitude of point of interest
    
    Outputs:
        (int) The index corresponding to the closest location of data
    '''
    lat_lon = np.array([lat_coord, lon_coord])
    dist, pos = tree.query(lat_lon)
    return pos


def nearest_points(wind_file, pts):
    '''
    A function that iterates through each point from the geojson file and finds the nearest data location

    Inputs:
        wind_file: (h5 file) the h5 file downloaded from the NREL API
        pts: (list of tuples) the coordinates of all points from the geojson file
    
    Output:
        (set) the indices of all data locations in the defined area
    '''
    wind_coords = wind_file['coordinates'][...]
    tree = cKDTree(wind_coords)

    nearest_set = set()

    for pt in pts:
        x, y = pt
        idx = nearest_site(tree, y, x)
        nearest_set.add(idx)
    print('length of set')
    print(len(nearest_set))

    return nearest_set


def extract_dataset(wind_file, nearest_pts):
    '''
    Iterate through data points near area of interest and extract relevant data from wind dataset

    Inputs:
        wind_file: (h5 file) the h5 file downloaded from the NREL API
        nearest_pts: (set) indices of all data points in the area of interest
    
    Output:
        (pd DataFrame) the relevant data for the area of interest
    '''
    time_index = pd.to_datetime(wind_file['time_index'][...].astype(str))
    dset = wind_file['windspeed_100m']

    for i, idx in enumerate(nearest_pts):
        if i == 0:
            tseries = pd.Series(dset[:, idx] / dset.attrs['scale_factor'])
        else:
            single = pd.Series(dset[:, idx] / dset.attrs['scale_factor'])
            tseries = pd.concat([tseries, single], axis=1, ignore_index=True)
    
    tseries.set_index(time_index, inplace=True)

    return tseries


def group_data(dask_data, grouping):
    '''
    Function to group data by the time grouping of interest

    Inputs:
        dask_data: (dask dataframe) the dask dataframe with all wind station readings over the year and area of interest
        grouping: (str) the time period by which the data should be grouped. Options allowed are 'month' or 'day'
    
    Outputs:
        (dask dataframe) the grouped dask dataframe with 
    '''
    dask_data['month'] = dask_data.index.month
    dask_data['day'] = dask_data.index.day

    if grouping == 'month':
        agg_data = dask_data.groupby([grouping], as_index=False).mean()
        agg_data['time_key'] = agg_data.month
    elif grouping == 'day':
        agg_data = dask_data.groupby(['month', grouping], as_index=False).mean()
        agg_data['time_key'] = agg_data.month.astype(str) + '/' + agg_data.day.astype(str)
    else:
        raise Exception('The grouping timeframe must either be "month" or "day"')

    agg_data.drop(labels=['day', 'month'], axis=1, inplace=True)
    
    melted_data = pd.melt(agg_data, id_vars=['time_key'], value_name='station_avg')
    melted_data.drop(labels=['variable'], axis=1, inplace=True)

    return agg_data, melted_data


def make_density_plot(data, geometry_file, grouping):
    '''
    A function to make a density plot of the data and save it in the graphs directory

    Inputs:
        data: (pandas DataFrame) The dataframe from which the density plot will be constructed
        geometry_file: (str) The name of the input file, will be used for naming of output file
        grouping: (str) The grouping by which the data is rolled up, used for naming
    
    Outputs:
        None, will save image to the graphs/ directory
    '''
    if not os.path.isdir('graphs'):
        os.mkdir('graphs')
    
    split_one = geometry_file.split('/')[1]
    place_name = split_one.split('.')[0]
    file_name = '_'.join([place_name, grouping, 'density'])
    
    sns.kdeplot(data['station_avg'])
    plt.axvline(data['station_avg'].mean(), color='red')
    plt.axvline(data['station_avg'].mean() - 2*data['station_avg'].std(), color='orange', linestyle='--')
    plt.axvline(data['station_avg'].mean() + 2*data['station_avg'].std(), color='orange', linestyle='--')

    lines = [Line2D([0], [0], color='red'), 
        Line2D([0], [0], color='orange', linestyle='--')]
    labels = ['Avg Wind Speed', '95% Confidence Interval']

    plt.title(f'Density of Average Hourly Wind Speeds by {grouping}')
    plt.xlabel('Wind Speed in MPH at 100 Meters')
    plt.legend(lines, labels, bbox_to_anchor=(1.04,0.5), loc="center left", borderaxespad=0)
    plt.savefig(f'graphs/{file_name}.png')

    print(f'Check graphs/{file_name} for the finished density plot')
    plt.close()


def make_time_plot(data, geometry_file, grouping, year):
    '''
    A function to make a plot of the data over time and save it in the graphs directory

    Inputs:
        data: (pandas DataFrame) The dataframe from which the time plot will be constructed
        geometry_file: (str) The name of the input file, will be used for naming of output file
        grouping: (str) The grouping by which the data is rolled up, used for naming
        year: (str) The year of data, will be used for naming
    
    Outputs:
        None, will save image to the graphs/ directory
    '''
    if not os.path.isdir('graphs'):
        os.mkdir('graphs')
    
    split_one = geometry_file.split('/')[1]
    place_name = split_one.split('.')[0]
    file_name = '_'.join([place_name, grouping, 'time'])
    
    sns.pointplot(data=data, x='time_key', y='station_avg')
    plt.title(f'Average Hourly Wind Speeds by {grouping} in {year}')
    plt.xlabel('Time')
    plt.ylabel('Average Hourly Wind Speed in MPH')
    plt.savefig(f'graphs/{file_name}.png')

    print(f'Check graphs/{file_name} for the finished time plot')
    plt.close()


def report_summary_stats(data):
    '''
    A function that prints out the summary statistics of the data of interest

    Inputs:
        data: (pandas DataFrame) the dataframe from which the summary stats will be constructed
    
    Outputs:
        None, will print out to terminal
    '''
    table = PrettyTable(['Statistic', 'Value'])
    table.add_row(['Average Wind Speed (mph)', data['station_avg'].mean()])
    table.add_row(['Standard Deviation', data['station_avg'].std()])
    table.add_row(['Median Wind Speed (mph)', data['station_avg'].median()])
    table.add_row(['Coefficient of Variation', data['station_avg'].std() / data['station_avg'].mean()])
    print(table)


if __name__ == "__main__":
    year_str = sys.argv[1]
    geometry_type = sys.argv[2]
    geometry_file = sys.argv[3]
    grouping = sys.argv[4]

    try:
        ## This code is from a previous HPC configuration I have used, it could be updated to use other dask - HPC interfaces
        ## like PBSCluster, YarnCluster, or Coiled
        cluster = SLURMCluster(queue='broadwl', cores=10, memory='40GB', 
                       processes=10, walltime='01:00:00', interface='ib0',
                       job_extra=['--account=macs30123'])
        client = Client(cluster)
        print('Created SLURM Cluster')
    except:
        cluster= LocalCluster(n_workers=4)
        client = Client(cluster)
        print('Created Local Cluster with 4 workers')

    f = h5pyd.File(f"/nrel/wtk/conus/wtk_conus_{year_str}.h5", 'r')

    print('Extracting points')
    pts = extract_points(geometry_type, geometry_file)
    print('Finding Nearest Points')
    nearest_pts = nearest_points(f, pts)
    print('Building Dataframe')
    data = extract_dataset(f, nearest_pts)
    
    print('Manipulating Data with Dask')
    # dask_data = dd.from_pandas(data, npartitions=2)
    grouped_data, melted_data = group_data(data, grouping)
    
    print('Making Graphs')
    make_density_plot(melted_data, geometry_file, grouping)
    make_time_plot(melted_data, geometry_file, grouping, year_str)

    print('Calculating Summary Statistics')
    report_summary_stats(melted_data)

# EOF