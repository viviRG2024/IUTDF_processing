import os
import glob
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import time
from pathlib import Path

def convert_detector_csv_to_parquet(city_folder):
    """
    将城市文件夹中的detectors_public.csv转换为parquet格式，
    并将经纬度转换为点几何数据
    
    参数:
    - city_folder: 城市数据文件夹路径
    
    返回:
    - bool: 是否成功转换
    """
    city_name = os.path.basename(city_folder)
    print(f"Processing {city_name} detector data...")
    
    # 文件路径定义
    csv_file = os.path.join(city_folder, "detectors_public.csv")
    parquet_file = os.path.join(city_folder, "detectors.parquet")
    
    # 检查文件是否已存在（断点续传）
    if os.path.exists(parquet_file):
        print(f"Skipping {city_name} - detector parquet already exists")
        return True
    
    # 检查源文件是否存在
    if not os.path.exists(csv_file):
        print(f"Warning: detectors_public.csv not found for {city_name}")
        return False
    
    try:
        start_time = time.time()
        
        # 读取CSV文件
        detector_df = pd.read_csv(csv_file)
        
        # 确保long和lat列存在
        if 'long' not in detector_df.columns or 'lat' not in detector_df.columns:
            print(f"  Warning: long or lat columns missing in {city_name}, skipping geometry creation")
            detector_df.to_parquet(parquet_file, index=False)
        else:
            # 创建点几何对象
            print(f"  Creating point geometry from long/lat coordinates")
            geometry = [Point(x, y) for x, y in zip(detector_df['long'], detector_df['lat'])]
            
            # 转换为GeoDataFrame
            geo_detector_df = gpd.GeoDataFrame(
                detector_df, 
                geometry=geometry, 
                crs="EPSG:4326"  # WGS84坐标系
            )
            
            # 打印一些基本信息
            print(f"  Successfully created geometry for {len(geo_detector_df)} detectors in {city_name}")
            
            # 保存为Parquet格式
            geo_detector_df.to_parquet(parquet_file, index=False)
        
        # 计算文件大小减少
        csv_size = os.path.getsize(csv_file) / 1024  # KB
        parquet_size = os.path.getsize(parquet_file) / 1024  # KB
        reduction = (1 - parquet_size / csv_size) * 100 if csv_size > 0 else 0
        
        elapsed_time = time.time() - start_time
        print(f"  Converted in {elapsed_time:.2f} seconds")
        print(f"  File size: CSV={csv_size:.2f}KB, Parquet={parquet_size:.2f}KB ({reduction:.1f}% reduction)")
        
        return True
    
    except Exception as e:
        print(f"Error converting {city_name} detectors: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """处理所有城市的detector数据转换"""
    
    # 根目录
    data_root = "data\debug\input"
    
    # 获取所有城市文件夹
    city_folders = [d for d in glob.glob(os.path.join(data_root, "*")) if os.path.isdir(d)]
    print(f"Found {len(city_folders)} city folders")
    
    # 创建进度文件路径
    progress_file = os.path.join(data_root, "detector_conversion_progress.txt")
    
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
            print(f"Skipping {city_name} - already in progress file")
            continue
        
        # 处理城市数据
        success = convert_detector_csv_to_parquet(city_folder)
        
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
    
    print(f"Conversion complete! {successful_count}/{total_cities} cities successfully processed.")
    
    # 统计总体文件大小变化
    total_csv_size = 0
    total_parquet_size = 0
    
    for city_folder in city_folders:
        csv_file = os.path.join(city_folder, "detectors_public.csv")
        parquet_file = os.path.join(city_folder, "detectors_info.parquet")
        
        if os.path.exists(csv_file):
            total_csv_size += os.path.getsize(csv_file)
        
        if os.path.exists(parquet_file):
            total_parquet_size += os.path.getsize(parquet_file)
    
    if total_csv_size > 0:
        total_reduction = (1 - total_parquet_size / total_csv_size) * 100
        print(f"\nTotal file size:")
        print(f"  CSV: {total_csv_size/1024:.2f}KB")
        print(f"  Parquet: {total_parquet_size/1024:.2f}KB")
        print(f"  Reduction: {total_reduction:.1f}%")
    
    # 提供使用示例
    print("\n如何使用转换后的数据:")
    print("```python")
    print("import geopandas as gpd")
    print("")
    print("# 读取包含几何数据的Parquet文件")
    print("detectors = gpd.read_parquet('data/001_Integrated Urban Traffic-Flood Dataset/augsburg/detectors.parquet')")
    print("")
    print("# 访问点几何数据")
    print("for idx, detector in detectors.iterrows():")
    print("    print(f\"Detector {detector['detid']} at {detector.geometry}\")")
    print("")
    print("# 绘制传感器点位置")
    print("import matplotlib.pyplot as plt")
    print("detectors.plot()")
    print("plt.title('Detector Locations')")
    print("plt.show()")
    print("```")

if __name__ == "__main__":
    main() 