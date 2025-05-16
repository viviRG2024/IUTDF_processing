import pandas as pd
import os
import glob
from pathlib import Path
import time

def convert_csv_to_parquet(csv_file, output_file):
    """
    将传感器CSV文件转换为Parquet格式
    
    Parameters:
    - csv_file: CSV文件路径
    - output_file: 输出Parquet文件路径
    
    Returns:
    - bool: 是否成功转换
    """
    try:
        # 读取CSV文件
        df = pd.read_csv(csv_file)
        
        # 确保所需的列存在
        required_cols = ['day', 'interval', 'detid', 'flow', 'occ', 'city']
        for col in required_cols:
            if col not in df.columns:
                print(f"Error: Required column '{col}' not found in {csv_file}")
                return False
        
        # 添加缺失的列(如果需要)
        if 'error' not in df.columns:
            df['error'] = None
        if 'speed' not in df.columns:
            df['speed'] = None
        
        # 转换日期时间
        df['day'] = pd.to_datetime(df['day'])
        df['datetime'] = df['day'] + pd.to_timedelta(df['interval'], unit='s')
        
        # 格式化日期时间为DD/MM/YYYY HH:MM:SS
        df['datetime'] = df['datetime'].dt.strftime('%d/%m/%Y %H:%M:%S')
        
        # 选择并重命名列
        result_df = df[['datetime', 'detid', 'flow', 'occ', 'speed', 'error', 'city']]
        
        # 保存为Parquet文件
        result_df.to_parquet(output_file, index=False)
        
        return True
    
    except Exception as e:
        print(f"Error converting {csv_file}: {str(e)}")
        return False

def process_city_folders(data_root):
    """处理所有城市文件夹，转换CSV到Parquet"""
    # 获取所有城市文件夹
    city_folders = [d for d in glob.glob(os.path.join(data_root, "*")) if os.path.isdir(d)]
    
    print(f"Found {len(city_folders)} city folders")
    
    # 创建进度文件路径
    progress_file = os.path.join(data_root, "csv2parquet_progress.txt")
    
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
        
        # 构建文件路径
        csv_file = os.path.join(city_folder, f"{city_name}.csv")
        parquet_file = os.path.join(city_folder, f"5min_readings.parquet")
        
        if not os.path.exists(csv_file):
            print(f"No sensor CSV file found for {city_name}")
            continue
        
        print(f"Processing {city_name}...")
        start_time = time.time()
        
        # 转换文件
        success = convert_csv_to_parquet(csv_file, parquet_file)
        
        if success:
            elapsed_time = time.time() - start_time
            print(f"Converted {city_name} in {elapsed_time:.2f} seconds")
            
            # 更新进度
            successful_count += 1
            processed_cities.add(city_name)
            
            # 保存进度
            with open(progress_file, 'w') as f:
                for city in processed_cities:
                    f.write(f"{city}\n")
        else:
            print(f"Failed to convert {city_name}")
        
        # 显示进度
        print(f"Progress: {successful_count}/{total_cities} cities ({successful_count/total_cities*100:.1f}%)")
    
    print(f"Conversion complete. {successful_count}/{total_cities} cities successfully processed.")
    
    # 验证文件
    actual_files = len([f for f in city_folders if os.path.exists(os.path.join(f, "sensor_readings.parquet"))])
    print(f"Verified {actual_files} parquet files in city folders")

def main():
    # 根目录
    data_root = "data\debug\input"
    
    # 处理所有城市文件夹
    process_city_folders(data_root)

if __name__ == "__main__":
    main() 