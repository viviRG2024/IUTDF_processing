import pandas as pd
import geopandas as gpd
import os
import glob
from pathlib import Path
from shapely.geometry import Point
import numpy as np
import time
import warnings
warnings.filterwarnings("ignore")

def find_nearest_grid(point, grid_gdf):
    """找到距离点最近的网格单元"""
    # 计算点到所有网格中心的距离
    distances = grid_gdf.geometry.distance(point)
    # 返回距离最小的网格ID
    return grid_gdf.iloc[distances.argmin()]['grid_id']

def find_containing_grid(point, grid_gdf):
    """找到包含点的网格单元"""
    # 找到包含该点的所有网格
    containing = grid_gdf[grid_gdf.contains(point)]
    if len(containing) > 0:
        # 如果有网格包含该点，返回第一个
        return containing.iloc[0]['grid_id']
    else:
        # 如果没有网格包含该点，找最近的
        return find_nearest_grid(point, grid_gdf)

def process_road_data(city_folder, grid_data_path):
    """处理单个城市的道路数据并关联到网格，同时保存为Parquet格式"""
    city_name = os.path.basename(city_folder)
    print(f"Processing {city_name} road data...")
    
    # 路径定义
    roads_gpkg_path = os.path.join(city_folder, "selected_roads.gpkg")
    network_geojson_path = os.path.join(city_folder, "selected_network_4326.geojson")
    grid_parquet_path = os.path.join(grid_data_path, city_name, "weather", "grid_info.parquet")
    
    # 输出Parquet文件路径
    roads_parquet_path = os.path.join(city_folder, "roads.parquet")
    network_parquet_path = os.path.join(city_folder, "selected_network.parquet")
    
    # 检查文件是否存在
    files_exist = True
    if not os.path.exists(roads_gpkg_path):
        print(f"Warning: roads.gpkg not found for {city_name}")
        files_exist = False
    
    if not os.path.exists(network_geojson_path):
        print(f"Warning: selected_network_4326.geojson not found for {city_name}")
        files_exist = False
    
    if not os.path.exists(grid_parquet_path):
        print(f"Warning: grid_parquet not found for {city_name}")
        files_exist = False
    
    if not files_exist:
        return False
    
    try:
        start_time = time.time()
        
        # 读取网格数据
        print(f"Loading grid data for {city_name}...")
        grid_df = pd.read_parquet(grid_parquet_path)
        
        # 将网格数据转换为地理数据框
        grid_gdf = gpd.GeoDataFrame(
            grid_df, 
            geometry=[Point(xy) for xy in zip(grid_df['longitude'], grid_df['latitude'])],
            crs="EPSG:4326"  # 假设坐标是WGS84
        )
        
        # 处理 roads.gpkg
        if os.path.exists(roads_gpkg_path):
            print(f"Processing roads.gpkg for {city_name}...")
            # roads_gdf = gpd.read_file(roads_gpkg_path, layer="edges")
            roads_gdf = gpd.read_file(roads_gpkg_path)
            
            # 确保两个数据集的坐标系统一致
            if roads_gdf.crs != grid_gdf.crs:
                roads_gdf = roads_gdf.to_crs(grid_gdf.crs)
            
            # 方法1: 使用空间连接找到每个道路线段的中心点所在的网格
            roads_gdf['centroid'] = roads_gdf.geometry.centroid
            roads_gdf['grid_id'] = roads_gdf['centroid'].apply(lambda point: find_containing_grid(point, grid_gdf))
            
            # 删除临时的中心点列
            roads_gdf = roads_gdf.drop(columns=['centroid'])
            
            # 更新原始文件（保持兼容性）
            # roads_gdf.to_file(roads_gpkg_path, driver="GPKG")
            
            # 保存为Parquet格式（节省空间）
            roads_gdf.to_parquet(roads_parquet_path, index=False)
            print(f"Saved road data with grid_id to {roads_parquet_path}")
            
            # 计算空间节省
            gpkg_size = os.path.getsize(roads_gpkg_path) / (1024*1024)  # MB
            parquet_size = os.path.getsize(roads_parquet_path) / (1024*1024)  # MB
            space_saved = gpkg_size - parquet_size
            print(f"Space saved: {space_saved:.2f} MB ({(space_saved/gpkg_size)*100:.1f}% reduction)")
        
        # 处理 selected_network_4326.geojson
        if os.path.exists(network_geojson_path):
            print(f"Processing selected_network_4326.geojson for {city_name}...")
            network_gdf = gpd.read_file(network_geojson_path)
            
            # 确保两个数据集的坐标系统一致
            if network_gdf.crs != grid_gdf.crs:
                network_gdf = network_gdf.to_crs(grid_gdf.crs)
            
            # 使用空间连接找到每个网络要素的中心点所在的网格
            network_gdf['centroid'] = network_gdf.geometry.centroid
            network_gdf['grid_id'] = network_gdf['centroid'].apply(lambda point: find_containing_grid(point, grid_gdf))
            
            # 删除临时的中心点列
            network_gdf = network_gdf.drop(columns=['centroid'])
            
            # 更新原始文件（保持兼容性）
            # network_gdf.to_file(network_geojson_path, driver="GeoJSON")
            
            # 保存为Parquet格式（节省空间）
            network_gdf.to_parquet(network_parquet_path, index=False)
            print(f"Saved network data with grid_id to {network_parquet_path}")
            
            # 计算空间节省
            geojson_size = os.path.getsize(network_geojson_path) / (1024*1024)  # MB
            parquet_size = os.path.getsize(network_parquet_path) / (1024*1024)  # MB
            space_saved = geojson_size - parquet_size
            print(f"Space saved: {space_saved:.2f} MB ({(space_saved/geojson_size)*100:.1f}% reduction)")
        
        elapsed_time = time.time() - start_time
        print(f"Processed {city_name} in {elapsed_time:.2f} seconds")
        
        return True
    
    except Exception as e:
        print(f"Error processing {city_name}: {str(e)}")
        return False

def main():
    # 根目录
    data_root = r"data\debug\input"
    grid_data_path = r"data\debug\output\city_whole"
    
    # 获取所有城市文件夹
    city_folders = [d for d in glob.glob(os.path.join(data_root, "*")) if os.path.isdir(d)]
    
    print(f"Found {len(city_folders)} city folders")
    
    # 创建进度文件路径
    progress_file = os.path.join(data_root, "road2grid_progress.txt")
    
    # 从进度文件加载已处理的城市(如果存在)
    processed_cities = set()
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            processed_cities = set(line.strip() for line in f.readlines())
        print(f"Loaded progress: {len(processed_cities)} cities already processed")
    
    # 记录总体进度
    total_cities = len(city_folders)
    successful_count = len(processed_cities)
    
    # 处理每个城市
    for city_folder in city_folders:
        city_name = os.path.basename(city_folder)
        
        # 如果城市已经处理过，跳过
        if city_name in processed_cities:
            print(f"Skipping {city_name} - already processed")
            continue
        
        # 处理城市数据
        success = process_road_data(city_folder, grid_data_path)
        
        # 如果处理成功，记录进度
        if success:
            successful_count += 1
            processed_cities.add(city_name)
            
            # 更新进度文件
            with open(progress_file, 'w') as f:
                for city in processed_cities:
                    f.write(f"{city}\n")
        
        # 显示当前进度
        print(f"Progress: {successful_count}/{total_cities} cities processed ({successful_count/total_cities*100:.1f}%)")
    
    print(f"Processing complete! {successful_count}/{total_cities} cities successfully processed.")
    
    # 汇总空间节省情况
    total_original_size = 0
    total_parquet_size = 0
    for city_folder in city_folders:
        roads_gpkg = os.path.join(city_folder, "roads.gpkg")
        roads_parquet = os.path.join(city_folder, "roads.parquet")
        network_geojson = os.path.join(city_folder, "selected_network_4326.geojson")
        network_parquet = os.path.join(city_folder, "selected_network.parquet")
        
        if os.path.exists(roads_gpkg) and os.path.exists(roads_parquet):
            total_original_size += os.path.getsize(roads_gpkg)
            total_parquet_size += os.path.getsize(roads_parquet)
        
        if os.path.exists(network_geojson) and os.path.exists(network_parquet):
            total_original_size += os.path.getsize(network_geojson)
            total_parquet_size += os.path.getsize(network_parquet)
    
    # 转换为MB进行显示
    total_original_mb = total_original_size / (1024*1024)
    total_parquet_mb = total_parquet_size / (1024*1024)
    total_saved_mb = total_original_mb - total_parquet_mb
    
    print(f"\nTotal space usage:")
    print(f"Original formats (GPKG/GeoJSON): {total_original_mb:.2f} MB")
    print(f"Parquet format: {total_parquet_mb:.2f} MB")
    print(f"Total space saved: {total_saved_mb:.2f} MB ({(total_saved_mb/total_original_mb)*100:.1f}% reduction)")

if __name__ == "__main__":
    main()
