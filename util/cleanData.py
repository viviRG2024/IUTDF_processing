import os
import shutil
from pathlib import Path

def clean_era5_city_folders(base_dir="data/processed/era5_city", dry_run=True):
    """
    清理era5_city文件夹，只保留每个城市的grid_info.parquet文件
    
    参数:
    base_dir: 基础目录路径
    dry_run: 如果为True，只显示会删除什么，但不实际删除
    """
    base_path = Path(base_dir)
    
    # 确保基础目录存在
    if not base_path.exists():
        print(f"错误: 目录 {base_dir} 不存在")
        return
    
    # 获取所有城市文件夹
    city_folders = [f for f in base_path.iterdir() if f.is_dir()]
    
    if not city_folders:
        print(f"没有找到城市文件夹在 {base_dir}")
        return
    
    print(f"找到 {len(city_folders)} 个城市文件夹")
    
    # 跟踪删除统计
    deleted_files = 0
    deleted_folders = 0
    preserved_files = 0
    
    # 遍历每个城市文件夹
    for city_folder in city_folders:
        city_name = city_folder.name
        print(f"\n处理城市: {city_name}")
        
        # 要保留的文件路径
        grid_info_path = city_folder / "weather" / "grid_info.parquet"
        
        # 检查grid_info.parquet是否存在
        grid_info_exists = grid_info_path.exists()
        if grid_info_exists:
            preserved_files += 1
            print(f"  保留文件: {grid_info_path}")
        else:
            print(f"  警告: {grid_info_path} 不存在")
        
        # 遍历城市文件夹中的所有文件和目录
        for root, dirs, files in os.walk(city_folder, topdown=False):  # topdown=False 确保先处理子目录
            root_path = Path(root)
            
            # 处理文件
            for file in files:
                file_path = root_path / file
                
                # 如果不是要保留的文件，则删除
                if file_path != grid_info_path:
                    if dry_run:
                        print(f"  将删除文件: {file_path}")
                    else:
                        try:
                            file_path.unlink()
                            print(f"  已删除文件: {file_path}")
                            deleted_files += 1
                        except Exception as e:
                            print(f"  删除文件时出错 {file_path}: {str(e)}")
            
            # 处理目录
            # 不删除weather目录和城市目录本身
            if (root_path != city_folder and 
                root_path != city_folder / "weather" and
                (not grid_info_exists or "weather" not in root_path.parts)):
                
                # 检查目录是否为空
                if not any(root_path.iterdir()) or not grid_info_exists:
                    if dry_run:
                        print(f"  将删除目录: {root_path}")
                    else:
                        try:
                            # 使用rmdir只能删除空目录
                            root_path.rmdir()
                            print(f"  已删除目录: {root_path}")
                            deleted_folders += 1
                        except Exception as e:
                            print(f"  删除目录时出错 {root_path}: {str(e)}")
        
        # 确保weather目录存在
        weather_dir = city_folder / "weather"
        if not weather_dir.exists() and not dry_run:
            weather_dir.mkdir(parents=True, exist_ok=True)
            print(f"  创建目录: {weather_dir}")
    
    # 打印统计信息
    print("\n清理完成!")
    print(f"预计会删除 {deleted_files} 个文件和 {deleted_folders} 个目录" if dry_run 
          else f"已删除 {deleted_files} 个文件和 {deleted_folders} 个目录")
    print(f"保留了 {preserved_files} 个grid_info.parquet文件")

if __name__ == "__main__":
    # 先执行一次dry run，显示将要删除的内容
    print("=== 执行预览模式(不会实际删除文件) ===")
    clean_era5_city_folders(dry_run=True)
    
    # 询问用户是否继续
    response = input("\n确认删除这些文件? (yes/no): ").strip().lower()
    if response == 'yes':
        print("\n=== 执行实际删除 ===")
        clean_era5_city_folders(dry_run=False)
    else:
        print("操作已取消，没有文件被删除。")
