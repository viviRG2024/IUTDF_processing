import pandas as pd
import numpy as np
import os
import glob
from pathlib import Path
import time

def calculate_hourly_data(sensor_df):
    """
    聚合5分钟间隔的传感器数据为小时数据
    
    Parameters:
    - sensor_df: 包含传感器数据的DataFrame
    
    Returns:
    - hourly_df: 聚合后的小时级数据
    """
    # 确保时间列的格式正确
    sensor_df['day'] = pd.to_datetime(sensor_df['day'])
    
    # 创建一个datetime列，结合日期和间隔
    sensor_df['datetime'] = sensor_df['day'] + pd.to_timedelta(sensor_df['interval'], unit='s')
    # 创建小时时间戳
    sensor_df['hour'] = sensor_df['datetime'].dt.floor('h')
    # 按小时、传感器ID和城市分组
    grouped = sensor_df.groupby(['hour', 'detid', 'city'])
    
    # 初始化结果列表
    hourly_data = []
    
    for (hour, detid, city), group in grouped:
        # 计算样本数量
        samples_count = len(group)
        
        # 1. 流量：求和和平均
        flow_sum = group['flow'].sum()
        flow_mean_5min = flow_sum / samples_count if samples_count > 0 else 0
        
        # 2. 速度：检查列是否存在和是否有非NaN值
        has_speed_column = 'speed' in group.columns
        has_valid_speed = has_speed_column and not group['speed'].isna().all()
        
        # 设置默认值
        speed_mean = np.nan
        speed_weight = np.nan
        
        # 如果有有效的速度数据，则计算平均值
        if has_valid_speed:
            speed_mean = group['speed'].mean()
            
            # 过滤掉速度为NaN的行
            valid_data = group.dropna(subset=['speed'])
            
            # 计算流量加权平均速度
            if not valid_data.empty and valid_data['flow'].sum() > 0:
                speed_weight = np.average(
                    valid_data['speed'], 
                    weights=valid_data['flow'],
                    axis=0
                )
        
        # 3. 占有率：算术平均
        occ_mean = group['occ'].mean()
        
        # 4. 统计错误率
        error_mean = group['error'].mean() if 'error' in group.columns else np.nan
        
        # 5. 格式化日期时间为指定格式 (DD/MM/YYYY HH:MM:SS)
        formatted_datetime = hour.strftime('%d/%m/%Y %H:%M:%S')
        
        # 添加到结果列表
        hourly_data.append({
            'datetime': formatted_datetime,
            'detid': detid,
            'flow_sum': flow_sum,
            'flow_mean_5min': flow_mean_5min,
            'occ_mean': occ_mean,
            'speed_mean': speed_mean,
            'speed_weight': speed_weight,
            'error_mean': error_mean,
            'city': city
        })
    
    # 转换为DataFrame
    hourly_df = pd.DataFrame(hourly_data)
    
    return hourly_df

def process_city_sensor_data(city_folder):
    """处理某个城市的传感器数据并保存小时级聚合结果"""
    
    city_name = os.path.basename(city_folder)
    sensor_file = os.path.join(city_folder, f"{city_name}.csv")
    output_file = os.path.join(city_folder, "hourly_readings.parquet")
    
    # 检查该城市的结果文件是否已存在（断点续传）
    if os.path.exists(output_file):
        print(f"Skipping {city_name} - already processed")
        return True
    
    if not os.path.exists(sensor_file):
        print(f"No sensor data found for {city_name}")
        return False
    
    try:
        start_time = time.time()
        print(f"Processing {city_name} sensor data...")
        
        # 读取传感器数据
        sensor_df = pd.read_csv(sensor_file)
        
        # 计算小时级数据
        hourly_df = calculate_hourly_data(sensor_df)
        
        # 保存结果到城市文件夹
        hourly_df.to_parquet(output_file, index=False)
        
        elapsed_time = time.time() - start_time
        print(f"Processed {city_name} in {elapsed_time:.2f} seconds - Saved to {output_file}")
        
        return True
    except Exception as e:
        print(f"Error processing {city_name}: {str(e)}")
        return False

def main():
    # 根目录
    data_root = r"data\debug\input"
    
    # 获取所有城市文件夹
    city_folders = [d for d in glob.glob(os.path.join(data_root, "*")) if os.path.isdir(d)]
    
    # 创建进度文件路径
    progress_file = os.path.join(data_root, "hourly_processing_progress.txt")
    
    # 从进度文件加载已处理的城市（如果存在）
    processed_cities = set()
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            processed_cities = set(line.strip() for line in f.readlines())
        print(f"Loaded progress: {len(processed_cities)} cities already processed")
    
    # 记录总体进度
    total_cities = len(city_folders)
    successful_count = len(processed_cities)
    
    print(f"Found {total_cities} cities, {total_cities - successful_count} remaining to process")
    
    # 处理每个城市，跳过已处理的
    for city_folder in city_folders:
        city_name = os.path.basename(city_folder)
        
        # 如果城市已经处理过，跳过
        if city_name in processed_cities:
            print(f"Skipping {city_name} - already in progress file")
            continue
        
        # 处理城市数据
        success = process_city_sensor_data(city_folder)
        
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
    
    # 统计有多少城市成功生成了hourly_readings.parquet文件
    actual_files = len([f for f in city_folders if os.path.exists(os.path.join(f, "hourly_readings.parquet"))])
    print(f"Verified {actual_files} hourly_readings.parquet files in city folders")

if __name__ == "__main__":
    main() 