import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import numpy as np
from pathlib import Path
import os
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

def get_processed_files(progress_file):
    """获取所有已经处理过的时间点"""
    processed = set()
    if progress_file.exists():
        with open(progress_file, 'r') as f:
            for line in f:
                # 格式: date,time_str
                date_str, time_str = line.strip().split(',')
                processed.add((date_str, time_str))
    return processed

def plot_grib_spatial(grib_file, output_dir, progress_file, processed_times):
    """为GRIB文件创建空间分布图"""
    # 读取GRIB文件
    print(f"读取文件: {grib_file}")
    
    # 从文件名获取目标日期
    file_path = Path(grib_file)
    target_date = pd.to_datetime(file_path.stem.split('_')[-1]).date()
    date_str = target_date.strftime('%Y-%m-%d')
    print(f"目标日期: {target_date}")
    
    # 创建输出目录
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"已处理总时间点: {len(processed_times)}")
    
    try:
        # 读取降水数据 (edition 1)
        ds_rain = xr.open_dataset(grib_file, engine='cfgrib', 
                                 backend_kwargs={'filter_by_keys': {'edition': 1}})
        
        total_processed = 0
        for time in ds_rain.time:
            time_slice = ds_rain.sel(time=time)
            base_time = pd.Timestamp(time.values) + pd.Timedelta(hours=1)
            
            # 处理12小时的预报数据
            tp_values = time_slice.tp.values
            lsrr_values = time_slice.lsrr.values
            
            valid_hours = len(tp_values)
            for hour in range(valid_hours):
                current_time = base_time + pd.Timedelta(hours=hour)
                
                # 只处理目标日期的数据
                if current_time.date() == target_date:
                    time_str = current_time.strftime('%Y%m%d_%H%M')
                    
                    # 检查是否已处理
                    if (date_str, time_str) in processed_times:
                        print(f"跳过已处理的时间点: {time_str}")
                        continue
                    
                    print(f"处理时间点: {current_time}")
                    
                    # 创建图表 - 修改为一行三列的布局
                    fig = plt.figure(figsize=(24, 8))  # 调整图表大小以适应横向布局
                    
                    # 1. 总降水量 (tp)
                    ax1 = plt.subplot(1, 3, 1, projection=ccrs.PlateCarree())
                    lon, lat = np.meshgrid(time_slice.longitude, time_slice.latitude)
                    im1 = ax1.pcolormesh(lon, lat, tp_values[hour],
                                       transform=ccrs.PlateCarree(),
                                       cmap='Blues')
                    ax1.coastlines()
                    ax1.add_feature(cfeature.BORDERS, linestyle=':')
                    plt.colorbar(im1, ax=ax1, label='Total Precipitation (mm)')
                    ax1.set_title(f'Total Precipitation at {current_time}')
                    
                    # 2. 大尺度降水率 (lsrr)
                    ax2 = plt.subplot(1, 3, 2, projection=ccrs.PlateCarree())
                    im2 = ax2.pcolormesh(lon, lat, lsrr_values[hour],
                                       transform=ccrs.PlateCarree(),
                                       cmap='Greens')
                    ax2.coastlines()
                    ax2.add_feature(cfeature.BORDERS, linestyle=':')
                    plt.colorbar(im2, ax=ax2, label='Large Scale Rain Rate (mm/h)')
                    ax2.set_title(f'Large Scale Rain Rate at {current_time}')
                    
                    # 3. 降水类型 (如果可用)
                    try:
                        ds_type = xr.open_dataset(grib_file, engine='cfgrib',
                                                backend_kwargs={'filter_by_keys': {'edition': 2}})
                        type_slice = ds_type.sel(time=time)
                        ptype_values = type_slice.ptype.values[hour]
                        
                        ax3 = plt.subplot(1, 3, 3, projection=ccrs.PlateCarree())
                        im3 = ax3.pcolormesh(lon, lat, ptype_values,
                                           transform=ccrs.PlateCarree(),
                                           cmap='Set3')
                        ax3.coastlines()
                        ax3.add_feature(cfeature.BORDERS, linestyle=':')
                        plt.colorbar(im3, ax=ax3, label='Precipitation Type')
                        ax3.set_title(f'Precipitation Type at {current_time}')
                        ds_type.close()
                    except Exception as e:
                        print(f"无法读取降水类型数据: {str(e)}")
                    
                    # 添加总标题
                    plt.suptitle(f'ERA5 Rainfall Data Analysis\n{current_time}', fontsize=16)
                    
                    # 调整布局
                    plt.tight_layout()
                    
                    # 保存图片
                    plt.savefig(output_dir / f'spatial_analysis_{time_str}.png',
                               dpi=300, bbox_inches='tight')
                    plt.close()
                    
                    # 保存进度
                    total_processed += 1
                    processed_times.add((date_str, time_str))
                    
                    # 更新进度文件
                    with open(progress_file, 'a') as f:
                        f.write(f"{date_str},{time_str}\n")
                    
                    if total_processed % 10 == 0:  # 每处理10张图片输出一次进度
                        print(f"已处理 {total_processed} 张新图片")
    
    finally:
        # 确保数据集被关闭
        ds_rain.close()
        print(f"完成处理，共生成 {total_processed} 张新图片")

def main():
    # 设置数据目录
    era5_root = Path("G:/002_Data/007_ERA5")
    output_root = Path("data/processed/era5_spatial")
    progress_file = output_root / 'progress.txt'
    
    # 创建输出根目录
    output_root.mkdir(parents=True, exist_ok=True)
    
    # 获取已处理的时间点
    processed_times = get_processed_files(progress_file)
    print(f"从进度文件中读取到 {len(processed_times)} 个已处理的时间点")
    
    # 获取所有GRIB文件
    grib_files = list(era5_root.glob('era5_rainfall_*.grib'))
    
    print(f"找到 {len(grib_files)} 个GRIB文件")
    for grib_file in grib_files:
        print(f"\n处理文件: {grib_file}")
        try:
            date_str = grib_file.stem.split('_')[-1]
            output_dir = output_root / date_str
            plot_grib_spatial(grib_file, output_dir, progress_file, processed_times)
            print(f"成功创建 {date_str} 的空间分析图")
        except Exception as e:
            print(f"处理文件 {grib_file} 时出错: {str(e)}")
            # 记录错误信息
            with open(output_root / 'errors.log', 'a') as f:
                f.write(f"{grib_file}: {str(e)}\n")
            continue

if __name__ == "__main__":
    main() 