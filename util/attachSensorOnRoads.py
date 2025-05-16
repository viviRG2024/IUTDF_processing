import os
import json
import pandas as pd
import geopandas as gpd
from shapely.geometry import MultiLineString, LineString

def load_config(config_path):
    with open(config_path, 'r') as config_file:
        return json.load(config_file)

# Define a function to handle MultiLineString
def convert_multilinestring(geometry):
    if isinstance(geometry, MultiLineString):
        return [LineString(line) for line in geometry.geoms]
    return geometry

if __name__ == "__main__":
    # config = load_config(r'config.json')
    # cities = config['cities']
    base_folder = r'data\debug\input'

    for city in os.listdir(base_folder):
        print(f"Processing city: {city}")
        city_path = os.path.join(base_folder, city)
        road_file = os.path.join(city_path, 'roads_centroidline.geojson')
        detector_file = os.path.join(city_path, 'detectors_public.csv')

        if not os.path.exists(road_file) or not os.path.exists(detector_file):
            print(f"Files do not exist: {road_file} or {detector_file}")
            continue

        ori_road = gpd.read_file(road_file, driver='GeoJSON')
        ori_road.set_crs('epsg:32650', inplace=True, allow_override=True)  # Explicitly set CRS
        ori_road['geometry'] = ori_road['geometry'].apply(convert_multilinestring)

        # Correctly handle exploded GeoDataFrame
        if isinstance(ori_road.explode(), gpd.GeoDataFrame):
            ex_ori_road = ori_road.explode().reset_index(drop=True)
        else:
            ex_ori_road = gpd.GeoDataFrame(ori_road.explode(), geometry='geometry')

        ex_ori_road['road_length'] = ex_ori_road.length.round(2)  # Calculate the length of each road
        ex_ori_road.set_crs('epsg:32650', inplace=True)
        # roads_gdf = ex_ori_road.to_crs(epsg=4326)  # Convert to EPSG:4326
        roads_gdf = ex_ori_road.copy()
        roads_gdf.reset_index(drop=True, inplace=True)
        roads_gdf['road_id'] = roads_gdf.index

        ex_detectors = pd.read_csv(detector_file)
        ex_detectors['geometry'] = gpd.points_from_xy(ex_detectors['long'], ex_detectors['lat'])
        ex_detectors_gdf = gpd.GeoDataFrame(ex_detectors, geometry='geometry')
        ex_detectors_gdf.set_crs('epsg:4326', inplace=True)
        detectors_gdf = ex_detectors_gdf.to_crs('epsg:32650')
        # Initialize detid column
        roads_gdf['detid'] = -1

        # Find the nearest road for each detector
        for index, detector in detectors_gdf.iterrows():
            # print(roads_gdf.distance(detector.geometry))
            # print(f"Processing detector: {detector['detid']}")
            nearest_road = roads_gdf.distance(detector.geometry).idxmin()
            roads_gdf.at[nearest_road, 'detid'] = detector['detid']

        # Save the updated GeoJSON file
        final_road_gdf = roads_gdf.to_crs('epsg:4326')
        updated_road_file = os.path.join(city_path, 'selected_network_4326.geojson')
        final_road_gdf.to_file(updated_road_file, driver='GeoJSON')
        print(f"Successfully updated and saved: {city}---{updated_road_file}")
        print(f"---------------------------------------------------")

    print("All cities processed")