import os
import shutil
import glob
from pathlib import Path
import time

def create_directory(directory):
    """创建目录，如果不存在"""
    os.makedirs(directory, exist_ok=True)

def copy_file(source, destination):
    """复制文件，确保目标目录存在"""
    dest_dir = os.path.dirname(destination)
    create_directory(dest_dir)
    
    if os.path.exists(source):
        shutil.copy2(source, destination)
        return True
    else:
        return False

def organize_city_data(city_name, source_root, weather_root, target_root, copy_instead_of_move=True):
    """
    为单个城市组织和移动数据
    
    参数:
    - city_name: 城市名称
    - source_root: 源数据根目录 (001_Integrated...)
    - weather_root: 天气数据根目录 (processed/ear5_city)
    - target_root: 目标根目录 (000_IUTFD)
    - copy_instead_of_move: True则复制文件，False则移动文件
    
    返回:
    - (成功操作的文件数, 总文件数)
    """
    print(f"\nProcessing {city_name}...")
    
    # 源路径
    source_city_dir = os.path.join(source_root, city_name)
    weather_city_dir = os.path.join(weather_root, city_name, "weather")
    
    # 目标路径
    target_city_dir = os.path.join(target_root, city_name)
    target_roads_dir = os.path.join(target_city_dir, "roads")
    target_sensors_dir = os.path.join(target_city_dir, "sensors")
    target_npz_dir = os.path.join(target_city_dir, "npz")
    target_weather_dir = os.path.join(target_city_dir, "weather")
    target_datetime_dir = os.path.join(target_weather_dir, "datetime")
    
    # 创建目标目录
    create_directory(target_roads_dir)
    create_directory(target_sensors_dir)
    create_directory(target_npz_dir)
    create_directory(target_weather_dir)
    create_directory(target_datetime_dir)
    
    # 定义要迁移的文件
    files_to_process = [
        # (源文件, 目标文件)
        # 元数据文件 - 直接放在城市根目录
        (os.path.join(source_city_dir, f"{city_name}_metadata.json"), os.path.join(target_city_dir, f"{city_name}_metadata.json")),
        
        # 道路网络文件
        (os.path.join(source_city_dir, "roads.parquet"), os.path.join(target_roads_dir, "roads.parquet")),
        (os.path.join(source_city_dir, "selected_network.parquet"), os.path.join(target_roads_dir, "selected_network.parquet")),
        
        # 传感器数据文件
        (os.path.join(source_city_dir, "detectors.parquet"), os.path.join(target_sensors_dir, "detectors_info.parquet")),
        (os.path.join(source_city_dir, "hourly_readings.parquet"), os.path.join(target_sensors_dir, "hourly_readings.parquet")),
        (os.path.join(source_city_dir, "5min_readings.parquet"), os.path.join(target_sensors_dir, "5min_readings.parquet")),
        
        # NPZ文件
        (os.path.join(source_city_dir, f"{city_name}_traffic_network.npz"), os.path.join(target_npz_dir, f"{city_name}_traffic_network.npz")),
        
        # 天气数据
        (os.path.join(weather_city_dir, "grid_info.parquet"), os.path.join(target_weather_dir, "grid_info.parquet")),
    ]
    
    # 查找并添加rainfall文件
    if os.path.exists(weather_city_dir):
        rainfall_files = glob.glob(os.path.join(weather_city_dir, "local_hourly_rainfall_*.parquet"))
        for rainfall_file in rainfall_files:
            filename = os.path.basename(rainfall_file)
            files_to_process.append((rainfall_file, os.path.join(target_datetime_dir, filename)))
    
    # 处理所有文件
    success_count = 0
    for source_file, dest_file in files_to_process:
        try:
            if not os.path.exists(source_file):
                print(f"  Warning: Source file not found: {source_file}")
                continue
                
            if os.path.exists(dest_file):
                print(f"  Skipping, already exists: {dest_file}")
                success_count += 1
                continue
                
            # 复制或移动文件
            if copy_instead_of_move:
                shutil.copy2(source_file, dest_file)
                print(f"  Copied: {source_file} -> {dest_file}")
            else:
                shutil.move(source_file, dest_file)
                print(f"  Moved: {source_file} -> {dest_file}")
                
            success_count += 1
            
        except Exception as e:
            print(f"  Error processing {source_file}: {str(e)}")
    
    print(f"Completed {city_name}: {success_count}/{len(files_to_process)} files processed")
    return success_count, len(files_to_process)

def main():
    # 配置参数
    SOURCE_ROOT = r"data\debug\input"
    WEATHER_ROOT = r"data\debug\output\city_whole"
    TARGET_ROOT = r"data\debug\IUTFD"
    
    # 是否复制而不是移动文件
    COPY_INSTEAD_OF_MOVE = True  # 设置为False以移动文件而不是复制
    
    # 创建根目标目录
    create_directory(TARGET_ROOT)
    
    # 获取城市列表
    city_dirs = [d for d in glob.glob(os.path.join(SOURCE_ROOT, "*")) if os.path.isdir(d)]
    city_names = [os.path.basename(d) for d in city_dirs]
    
    print(f"Found {len(city_names)} cities")
    print(f"Operation mode: {'Copy' if COPY_INSTEAD_OF_MOVE else 'Move'}")
    
    # 创建进度文件
    progress_file = os.path.join(TARGET_ROOT, "organization_progress.txt")
    
    # 从进度文件加载已处理的城市
    processed_cities = set()
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            processed_cities = set(line.strip() for line in f.readlines())
        print(f"Loaded progress: {len(processed_cities)} cities already processed")
    
    # 记录总进度
    total_cities = len(city_names)
    successful_count = len(processed_cities)
    total_files = 0
    total_success = 0
    
    # 处理每个城市
    start_time = time.time()
    for city_name in city_names:
        # 如果城市已处理，跳过
        if city_name in processed_cities:
            print(f"Skipping {city_name} - already processed")
            continue
        
        # 组织城市数据
        success_files, total_files_city = organize_city_data(
            city_name, 
            SOURCE_ROOT, 
            WEATHER_ROOT, 
            TARGET_ROOT,
            COPY_INSTEAD_OF_MOVE
        )
        
        total_files += total_files_city
        total_success += success_files
        
        # 如果成功处理，记录进度
        if success_files > 0:
            successful_count += 1
            processed_cities.add(city_name)
            
            # 更新进度文件
            with open(progress_file, 'w') as f:
                for city in processed_cities:
                    f.write(f"{city}\n")
        
        # 显示进度
        print(f"Progress: {successful_count}/{total_cities} cities processed ({successful_count/total_cities*100:.1f}%)")
    
    # 显示总结
    elapsed_time = time.time() - start_time
    print("\nOrganization complete!")
    print(f"Processed {successful_count}/{total_cities} cities")
    print(f"Processed {total_success}/{total_files} files")
    print(f"Total time: {elapsed_time:.2f} seconds")
    
    # 验证结果
    print("\nVerifying new directory structure...")
    verify_structure(TARGET_ROOT)

def verify_structure(target_root):
    """验证新创建的目录结构"""
    city_dirs = [d for d in glob.glob(os.path.join(target_root, "*")) if os.path.isdir(d)]
    
    for city_dir in city_dirs:
        city_name = os.path.basename(city_dir)
        print(f"\nVerifying {city_name}...")
        
        # 检查元数据文件
        metadata_file = os.path.join(city_dir, f"{city_name}_metadata.json")
        if os.path.exists(metadata_file):
            print(f"  ✓ metadata.json: file exists")
        else:
            print(f"  ✗ metadata.json: file missing")
        
        # 检查每个子目录是否存在
        subdirs = ["roads", "sensors", "npz", "weather", os.path.join("weather", "datetime")]
        for subdir in subdirs:
            full_path = os.path.join(city_dir, subdir)
            if os.path.exists(full_path):
                files = glob.glob(os.path.join(full_path, "*"))
                print(f"  ✓ {subdir}: {len(files)} files")
            else:
                print(f"  ✗ {subdir}: directory missing")

if __name__ == "__main__":
    main() 