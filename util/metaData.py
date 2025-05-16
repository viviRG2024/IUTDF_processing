import os
import json
import pandas as pd
import geopandas as gpd
import glob
from pathlib import Path
import numpy as np
from datetime import datetime

def generate_city_metadata(city_folder):
    """为指定城市生成元数据JSON文件"""
    
    city_name = os.path.basename(city_folder)
    print(f"Generating metadata for {city_name}...")
    
    # 定义输入文件路径
    network_geojson_path = os.path.join(city_folder, "selected_network_4326.geojson")
    rainfall_csv_path = os.path.join(city_folder, "rainfall_data.csv")
    detectors_csv_path = os.path.join(city_folder, "detectors_public.csv")
    readings_parquet_path = os.path.join(city_folder, "5min_readings.parquet")
    
    # 检查必要文件是否存在
    if not os.path.exists(network_geojson_path):
        print(f"Error: selected_network_4326.geojson not found for {city_name}")
        return None
    
    if not os.path.exists(rainfall_csv_path):
        print(f"Error: rainfall_data.csv not found for {city_name}")
        return None
    
    metadata = {
        "city": city_name,
        "time_range": {
            "start": "",
            "end": "",
            "resolutions": {
                "weather": "1h",
                "traffic": "5min"
            }
        },
        "spatial_bounds": {
            "bbox": [],
            "projection": "EPSG:4326"
        },
        "data_summary": {
            "num_roads": 0,
            "num_sensors": 0,
            "num_timepoints": 0
        },
        "relationships": {
            "road_to_detector": "one_to_one",
            "road_to_weather": "one_to_many"
        }
    }
    
    # 从network geojson获取空间范围和道路数量
    try:
        network_gdf = gpd.read_file(network_geojson_path)
        bounds = network_gdf.total_bounds  # [minx, miny, maxx, maxy]
        metadata["spatial_bounds"]["bbox"] = bounds.tolist()
        metadata["data_summary"]["num_roads"] = len(network_gdf)
        print(f"Extracted spatial bounds and road count from network geojson")
    except Exception as e:
        print(f"Error processing network geojson: {str(e)}")
        return None
    
    # 从rainfall_data.csv获取时间范围
    try:
        rainfall_df = pd.read_csv(rainfall_csv_path)
        if 'date' in rainfall_df.columns:
            # 确保日期格式一致
            rainfall_df['date'] = pd.to_datetime(rainfall_df['date'])
            start_date = rainfall_df['date'].min().strftime('%Y-%m-%d')
            end_date = rainfall_df['date'].max().strftime('%Y-%m-%d')
            metadata["time_range"]["start"] = start_date
            metadata["time_range"]["end"] = end_date
            print(f"Extracted time range from rainfall data: {start_date} to {end_date}")
        else:
            print("Warning: 'date' column not found in rainfall data")
    except Exception as e:
        print(f"Error processing rainfall data: {str(e)}")
        return None
    
    # 从detectors_public.csv获取传感器数量
    if os.path.exists(detectors_csv_path):
        try:
            detectors_df = pd.read_csv(detectors_csv_path)
            metadata["data_summary"]["num_sensors"] = len(detectors_df)
            print(f"Extracted sensor count: {len(detectors_df)}")
        except Exception as e:
            print(f"Error processing detectors data: {str(e)}")
    else:
        print(f"Warning: detectors_public.csv not found for {city_name}")
    
    # 从5min_readings.parquet获取时间点数量
    if os.path.exists(readings_parquet_path):
        try:
            # 直接尝试读取整个文件，不使用nrows参数
            readings_df = pd.read_parquet(readings_parquet_path)
            
            # 尝试找到时间列（可能是'datetime'或其他名称）
            time_column = None
            for potential_col in ['datetime', 'timestamp', 'time', 'date']:
                if potential_col in readings_df.columns:
                    time_column = potential_col
                    break
            
            if time_column:
                num_timepoints = readings_df[time_column].nunique()
                metadata["data_summary"]["num_timepoints"] = int(num_timepoints)
                print(f"Extracted timepoint count: {num_timepoints}")
            else:
                print("Warning: No time column found in readings data")
        except Exception as e:
            print(f"Error processing readings data: {str(e)}")
    else:
        print(f"Warning: 5min_readings.parquet not found for {city_name}")
    
    # 保存元数据到JSON文件
    metadata_path = os.path.join(city_folder, f"{city_name}_metadata.json")
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"Saved metadata to {metadata_path}")
    return metadata

def main():
    # 根目录
    data_root = r"data\debug\input"
    
    # 获取所有城市文件夹
    city_folders = [d for d in glob.glob(os.path.join(data_root, "*")) if os.path.isdir(d)]
    
    print(f"Found {len(city_folders)} city folders")
    
    # 处理每个城市
    for city_folder in city_folders:
        generate_city_metadata(city_folder)
    
    print("Metadata generation complete!")

if __name__ == "__main__":
    main() 