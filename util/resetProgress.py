import os
import glob
import shutil
from pathlib import Path
import time

def reset_progress(data_root, 
                   delete_progress_files=False, 
                   delete_parquet_files=False, 
                   delete_metadata_files=False,
                   delete_detectors_files=False,
                   dry_run=False):
    """
    重置处理进度并根据需要删除相关文件
    
    参数:
    - data_root: 数据根目录
    - delete_progress_files: 是否删除进度文件
    - delete_parquet_files: 是否删除生成的parquet文件
    - delete_metadata_files: 是否删除生成的元数据文件
    - delete_detectors_files: 是否删除生成的detectors文件
    - dry_run: 如果为True，仅显示将被删除的文件而不实际删除
    """
    
    print("=== 进度重置工具 ===")
    print(f"数据根目录: {data_root}")
    print(f"模式: {'预览模式 (不会实际删除文件)' if dry_run else '执行模式 (将删除文件)'}")
    print("将执行以下操作:")
    print(f"- 删除进度文件: {delete_progress_files}")
    print(f"- 删除Parquet文件: {delete_parquet_files}")
    print(f"- 删除元数据文件: {delete_metadata_files}")
    print(f"- 删除detectors文件: {delete_detectors_files}")
    print("-" * 40)
    
    # 获取所有城市文件夹
    city_folders = [d for d in glob.glob(os.path.join(data_root, "*")) if os.path.isdir(d)]
    print(f"找到 {len(city_folders)} 个城市文件夹")
    
    files_to_delete = []
    
    # 收集进度文件
    if delete_progress_files:
        progress_files = [
            os.path.join(data_root, "hourly_processing_progress.txt"),
            os.path.join(data_root, "csv2parquet_progress.txt"),
            os.path.join(data_root, "road2grid_progress.txt"),
            os.path.join(data_root, "detectors_processing_progress.txt")
        ]
        
        for file_path in progress_files:
            if os.path.exists(file_path):
                files_to_delete.append(file_path)
                print(f"将删除进度文件: {file_path}")
    
    # 收集每个城市的Parquet文件
    if delete_parquet_files:
        for city_folder in city_folders:
            city_name = os.path.basename(city_folder)
            
            # 寻找各种可能的parquet文件
            parquet_patterns = [
                # "hourly_readings.parquet",
                # "sensor_readings.parquet", 
                # "roads.parquet",
                # "selected_network.parquet",
                # "5min_readings.parquet",
                "detectors.parquet"
            ]
            
            for pattern in parquet_patterns:
                file_path = os.path.join(city_folder, pattern)
                if os.path.exists(file_path):
                    files_to_delete.append(file_path)
                    print(f"将删除Parquet文件: {file_path}")
    
    # 收集元数据文件
    if delete_metadata_files:
        for city_folder in city_folders:
            city_name = os.path.basename(city_folder)
            metadata_file = os.path.join(city_folder, f"{city_name}_metadata.json")
            
            if os.path.exists(metadata_file):
                files_to_delete.append(metadata_file)
                print(f"将删除元数据文件: {metadata_file}")
    
    # 执行删除操作
    if not dry_run:
        if not files_to_delete:
            print("没有找到需要删除的文件。")
        else:
            print(f"\n准备删除 {len(files_to_delete)} 个文件...")
            
            # 给用户最后确认的机会
            confirm = input("确认删除以上文件? (y/n): ").strip().lower()
            
            if confirm == 'y':
                deleted_count = 0
                for file_path in files_to_delete:
                    try:
                        os.remove(file_path)
                        deleted_count += 1
                        print(f"已删除: {file_path}")
                    except Exception as e:
                        print(f"删除失败 {file_path}: {str(e)}")
                
                print(f"\n完成! 成功删除了 {deleted_count}/{len(files_to_delete)} 个文件。")
            else:
                print("操作已取消。")
    else:
        print(f"\n预览模式: 将会删除 {len(files_to_delete)} 个文件。")
    
    return len(files_to_delete)

def main():
    # 配置区域 - 根据需要修改这些值
    # ===================================================================
    DATA_ROOT = "data/001_Integrated Urban Traffic-Flood Dataset"
    
    # 设置为True以删除相应文件，设置为False保留它们
    DELETE_PROGRESS_FILES = False      # 删除所有进度跟踪文件
    DELETE_PARQUET_FILES = True      # 删除生成的parquet文件
    DELETE_METADATA_FILES = False      # 删除生成的y元数据文件
    # DELETE_DETECTORS_FILES = True     # 删除生成的detectors文件
    
    DRY_RUN = False   # 设置为True进行预览而不实际删除
    # ===================================================================
    
    reset_progress(
        data_root=DATA_ROOT,
        delete_progress_files=DELETE_PROGRESS_FILES,
        delete_parquet_files=DELETE_PARQUET_FILES,
        delete_metadata_files=DELETE_METADATA_FILES,
        # delete_detectors_files=DELETE_DETECTORS_FILES,
        dry_run=DRY_RUN
    )

if __name__ == "__main__":
    main() 