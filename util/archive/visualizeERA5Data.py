import xarray as xr
import cfgrib
import matplotlib.pyplot as plt
import os
from pathlib import Path

def show_grib_info(ds):
    """显示grib文件的基本信息"""
    print("\n=== 数据集基本信息 ===")
    print(f"dimensions: {ds.dims}")
    print("\nvariables:")
    for var_name, var in ds.variables.items():
        print(f"\n{var_name}:")
        print(f"  - dimensions: {var.dims}")
        print(f"  - shape: {var.shape}")
        if hasattr(var, 'units'):
            print(f"  - units: {var.units}")
        if hasattr(var, 'long_name'):
            print(f"  - description: {var.long_name}")

def plot_rainfall_data(ds, output_dir, date_str):
    """绘制降水数据图表"""
    # 创建输出目录
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # 为每个日期创建子目录
    date_dir = os.path.join(output_dir, date_str)
    Path(date_dir).mkdir(parents=True, exist_ok=True)
    
    # 设置基础绘图样式
    plt.style.use('default')
    
    if 'tp' in ds:
        # 处理包含step维度的数据
        try:
            # 1. 绘制所有时间点的降水量
            plt.figure(figsize=(15, 6))
            # 对经纬度取平均，保留时间和步长维度
            mean_tp = ds['tp'].mean(dim=['latitude', 'longitude'])
            
            # 对于每个时间点
            for t in range(ds.dims['time']):
                time_data = mean_tp.isel(time=t)
                plt.plot(time_data.step, time_data.values, 
                        label=f'Time {ds.time[t].values}')
            
            plt.title(f'Average total rainfall change ({date_str})')
            plt.xlabel('Forecast step (h)')
            plt.ylabel('Rainfall (m)')
            plt.legend()
            plt.grid(True)
            plt.savefig(f"{date_dir}/total_precipitation_timeseries.png")
            plt.close()

            # 2. 绘制空间分布图（使用最后一个时间点和步长的数据）
            plt.figure(figsize=(12, 8))
            last_tp = ds['tp'].isel(time=-1, step=-1)
            last_tp.plot()
            plt.title(f'Precipitation spatial distribution ({date_str})')
            plt.savefig(f"{date_dir}/precipitation_spatial.png")
            plt.close()

        except Exception as e:
            print(f"Error plotting charts: {str(e)}")

def analyze_era5_data():
    # 设置数据目录
    data_dir = "data/processed/era5_data"
    output_dir = "data/processed/era5_plots"
    
    # 查找所有grib文件
    grib_files = list(Path(data_dir).glob("*.grib"))
    
    if not grib_files:
        print("No .grib files found!")
        return
    
    for grib_file in grib_files:
        print(f"\nProcessing file: {grib_file}")
        date_str = grib_file.stem.split('_')[-1]  # 从文件名获取日期
        
        try:
            # 尝试不同的方式读取GRIB文件
            backends = [
                {'filter_by_keys': {'typeOfLevel': 'surface'}},
                {'filter_by_keys': {'edition': 1}},
                {'filter_by_keys': {'edition': 2}},
                {'filter_by_keys': {'shortName': 'tp'}}
            ]
            
            success = False
            for backend_kwargs in backends:
                try:
                    print(f"Trying with parameters: {backend_kwargs}")    
                    ds = xr.open_dataset(grib_file, engine='cfgrib', backend_kwargs=backend_kwargs)
                    show_grib_info(ds)
                    plot_rainfall_data(ds, output_dir, date_str)
                    ds.close()
                    success = True
                    print(f"Successfully processed file, using parameters: {backend_kwargs}")
                    break
                except Exception as e:
                    print(f"Failed to use parameters {backend_kwargs}: {str(e)}")
                    continue
            
            if not success:
                print(f"All attempts failed, unable to process file: {grib_file}")
            
        except Exception as e:
            print(f"Error processing file: {str(e)}")

if __name__ == "__main__":
    analyze_era5_data() 