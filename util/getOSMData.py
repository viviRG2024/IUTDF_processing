import os
import osmnx as ox
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, box

def get_bounding_box_from_links(links_df):
    """从links.csv中获取所有点的边界框"""
    # 创建一个几何点列表
    points = [Point(row['long'], row['lat']) for _, row in links_df.iterrows()]
    # 创建GeoDataFrame
    points_gdf = gpd.GeoDataFrame(geometry=points, crs="EPSG:4326")
    # 获取边界框
    minx, miny, maxx, maxy = points_gdf.total_bounds
    # 扩大边界框（比如扩大10%），确保包含周边道路
    dx = (maxx - minx) * 0.1
    dy = (maxy - miny) * 0.1
    bbox = box(minx - dx, miny - dy, maxx + dx, maxy + dy)
    return bbox

if __name__ == '__main__':
    city_folder_path = r'data\debug\input'
    cities = [name for name in os.listdir(city_folder_path) if os.path.isdir(os.path.join(city_folder_path, name))]

    # download road data for each city
    for city in cities:
        # create folder for each city
        folder_name = os.path.join(city_folder_path, city)
        file_path = os.path.join(folder_name, 'roads.gpkg')
        print("processing", city)
        if city == "losangeles":
            city = "los angeles"
            
        # 下载道路数据（如果不存在）
        if not os.path.exists(file_path):
            try:
                G = ox.graph_from_place(city, network_type='all')
                ox.save_graph_geopackage(G, filepath=file_path, encoding='utf-8')
                print(f"{city} saved to {file_path}")
            except Exception as e:
                print(f"can not get {city}'s road data: {e}")
                continue
        else:
            print(f"{city} saved to {file_path}")

        try:
            # 读取links.csv文件
            links_path = os.path.join(folder_name, 'links.csv')
            if not os.path.exists(links_path):
                print(f"links.csv not found for {city}")
                continue
                
            links_df = pd.read_csv(links_path)
            
            # 获取边界框
            bbox = get_bounding_box_from_links(links_df)
            
            # 读取道路数据
            roads_gdf = gpd.read_file(file_path, layer='edges')
            roads_gdf.set_crs(epsg=4326, inplace=True)
            
            # 使用边界框筛选道路
            selected_roads = roads_gdf[roads_gdf.geometry.intersects(bbox)].copy()
            
            # 保存筛选后的道路数据
            selected_file_path = os.path.join(folder_name, 'selected_roads.gpkg')
            selected_roads.to_file(selected_file_path, driver='GPKG')
            print(f"Selected roads saved to {selected_file_path}")
            
            # 转换坐标系并保存为shp文件
            out_dir = os.path.join(folder_name, "selected_roads_32650")
            if not os.path.exists(out_dir):
                os.makedirs(out_dir)
                
            # 转换坐标系
            selected_roads.to_crs(epsg=32650, inplace=True)
            
            # 保存为shp文件
            shp_path = os.path.join(out_dir, 'selected_roads_32650.shp')
            if not os.path.exists(shp_path):
                selected_roads.to_file(shp_path, driver='ESRI Shapefile')
                print(f"Shapefile saved to {shp_path}")
            else:
                print(f"{city} shapefile already exists")
                
        except Exception as e:
            print(f"Error processing {city}: {e}")

