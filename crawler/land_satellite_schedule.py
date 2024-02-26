import os
import uuid
import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from shapely.geometry import shape

from shapely.ops import split
from itertools import groupby
import numpy as np
from sklearn.cluster import DBSCAN
from sqlalchemy import create_engine, exc
import urllib.parse
import json
# 清理重複的導入
import geopandas as gpd
import pandas as pd

scheduler_01_DBSCAN_MIN_PTS = 1
scheduler_01_GRID_SIZE = 1000
ENV_API_33_URL = 'http://192.168.1.104:8886/33FieldAvgCropIndex'
ENV_API_33_TOKEN = 'z3tw3rm6e5DxA2aY'
ENV_API_33_CC_BOUND = 0.3
ENV_API_33_SOURCE = 'sen2'
DB_US = "datayoo"
DB_PW = urllib.parse.quote_plus("*@(!)@&#")
DB_HT = "192.168.1.103"
DB_PORT = "3306"
DB_NAME = "taft"
DB_CONN_STR = f"mysql+pymysql://{DB_US}:{DB_PW}@{DB_HT}:{DB_PORT}/{DB_NAME}"
conn_taft = create_engine(DB_CONN_STR).connect()


field_land_sf_4326 = field_land_geometry_sf
def cluster_farmi_space_sf(field_land_sf_4326, scheduler_01_GRID_SIZE = 1000, scheduler_01_DBSCAN_MIN_PTS = 1, bbox_tolerance, new_grid_size):
    print(f"Using DBSCAN clustering algorithm to initially cluster FarmiSpace fields (received {len(field_land_sf_4326)} polygons) ...")
    print("Converting projection to EPSG:3857 and computing centroid coordinates under the new crs ...")
    
    # Convert projection to EPSG:3857
    field_land_sf_3857_valid = field_land_sf_4326.to_crs(epsg=3857)
    field_land_sf_3857_valid = field_land_sf_3857_valid[field_land_sf_3857_valid.geometry.is_valid]
    
    # Compute centroid coordinates
    field_land_sf_3857_valid['cent'] = field_land_sf_3857_valid.centroid
    field_land_sf_3857_cent_mat = np.array([np.array(cent.coords)[0] for cent in field_land_sf_3857_valid['cent']])
    
    print("Centroid coordinates computed! Performing DBSCAN clustering ...")
    
    # Perform DBSCAN clustering
    dbscan_res = DBSCAN(eps=1000, min_samples=1).fit(field_land_sf_3857_cent_mat)
    print(f"DBSCAN completed! {len(np.unique(dbscan_res.labels_[dbscan_res.labels_ > 0]))} clusters detected, with {np.sum(dbscan_res.labels_ == 0)} outliers.")
    
    # Grouping based on DBSCAN results
    grouped = field_land_sf_3857_valid.groupby(dbscan_res.labels_)
    
    print("Grouping the results of DBSCAN into outliers, clusters needing further processing, and clusters of appropriate size ...")
    
    # Outliers
    outliers = grouped.get_group(0)
    print(f"Outliers grouped. Total {len(outliers)} outliers.")
    
    # Clusters needing further processing
    large_clusters = [group for label, group in grouped if (abs(group.total_bounds[2] - group.total_bounds[0]) > bbox_tolerance) or 
                                                            (abs(group.total_bounds[3] - group.total_bounds[1]) > bbox_tolerance)]
    print(f"{len(large_clusters)} clusters identified for further processing.")
    
    # Split large clusters into smaller ones
    small_clusters = []
    for cluster in large_clusters:
        split_polygons = [split(cluster.iloc[i], [point for point in outliers.centroid]) for i in range(len(cluster))]
        small_clusters.extend([gpd.GeoDataFrame(geometry=[poly for sublist in split_polygons for poly in sublist])])
    print(f"{len(small_clusters)} clusters obtained after further splitting.")
    
    # Clusters of appropriate size
    ok_clusters = [group for label, group in grouped if (abs(group.total_bounds[2] - group.total_bounds[0]) <= bbox_tolerance) and 
                                                        (abs(group.total_bounds[3] - group.total_bounds[1]) <= bbox_tolerance)]
    print(f"{len(ok_clusters)} clusters of appropriate size.")
    
    # Combine all clusters
    all_clusters = pd.concat([outliers] + ok_clusters + small_clusters, ignore_index=True)
    print(f"DBSCAN completed! Total {len(all_clusters)} clusters obtained.")
    
    # Assign new group IDs
    all_clusters['new_group_id'] = range(1, len(all_clusters) + 1)
    
    return all_clusters['new_group_id']


field_land_geometry_raw = pd.read_sql_query("select * from land_info", conn_taft)
land_satellite_index_data_raw = pd.read_sql_query("select distinct(land_id) as land_id from land_satellite_index_data", conn_taft)

conn_taft.close()
field_land_geometry_raw = field_land_geometry_raw[~field_land_geometry_raw['land_id'].isin(land_satellite_index_data_raw['land_id'])]

# Convert coordinates to GeoDataFrame
field_land_geometry_sf = gpd.GeoDataFrame()

# Convert coordinates to GeoJSON format and create geometry column
field_land_geometry_sf['geometry'] = [shape({
    "type": row['geometry_type'],
    "coordinates": eval(row['coordinates'])
}) for _, row in field_land_geometry_raw.iterrows()]

# Remove 'geometry_type' and 'coordinates' columns
field_land_geometry_otr_cols = field_land_geometry_raw.drop(columns=['geometry_type', 'coordinates']).reset_index(drop=True)

field_land_geometry_sf = pd.concat([field_land_geometry_otr_cols, field_land_geometry_sf], axis = 1)

field_land_geometry_sf = gpd.GeoDataFrame(field_land_geometry_sf).set_crs(crs=4326)

field_land_geometry_sf['query_API_33_group_id'] = cluster_farmi_space_sf(field_land_geometry_sf, dbscan_eps=1000, dbscan_min_pts=scheduler_01_DBSCAN_MIN_PTS, bbox_tolerance=2000, new_grid_size=2000)

Group the GeoDataFrame by the cluster IDs
field_land_geometry_sf_grouped_list = [group for _, group in field_land_geometry_sf.groupby('query_API_33_group_id', as_index=False)]

Loop through each group
for i, group in enumerate(field_land_geometry_sf_grouped_list):
    api_33_analyzed_result_list = []
    for index_i in ["NDRE", "NDVI", "NDWI", "NDWI2"]:
        # Call API 33 for each index
        # api_33_analyzed_result = get_api_33_result(group, start_time="", index_name=index_i)
        # Append the result to the list
        # api_33_analyzed_result_list.append(api_33_analyzed_result)
    # Concatenate the results into a single DataFrame
    # result_df = pd.concat(api_33_analyzed_result_list, ignore_index=True)
    # Get the current time for file naming
    # now_time = pd.Timestamp.now().strftime('%Y%m%d%d%H%M%S')
    # Save the DataFrame to a CSV file
    # result_df.to_csv(f"/mnt/pic_satellite_98/taft_data/land_satellite_data/land_satellite_index_data/data_{now_time}.csv", index=False)
