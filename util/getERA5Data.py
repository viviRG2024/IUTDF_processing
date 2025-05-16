import cdsapi
import pandas as pd
import xarray as xr
import os
from pathlib import Path
import json

def load_download_progress(progress_file):
    """加载下载进度"""
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            return json.load(f)
    return {'downloaded_dates': []}

def save_download_progress(progress_file, downloaded_dates):
    """保存下载进度"""
    with open(progress_file, 'w') as f:
        json.dump({'downloaded_dates': downloaded_dates}, f)

def download_era5_rainfall(date, output_dir):
    """
    下载ERA5降水数据
    date: 日期 (格式: YYYY-MM-DD)
    output_dir: 输出目录
    """
    output_file = f"{output_dir}/era5_rainfall_{date}.grib"
    if os.path.exists(output_file):
        print(f"跳过已下载的日期: {date}")
        return output_file
    
    client = cdsapi.Client()
    request = {
        "product_type": "reanalysis",
        "variable": [
            "total_precipitation",
            "large_scale_rain_rate",
            "precipitation_type"
        ],
        "year": [date[:4]],
        "month": [date[5:7]],
        "day": [date[8:]],
        "time": [
            "00:00", "01:00", "02:00",
            "03:00", "04:00", "05:00",
            "06:00", "07:00", "08:00",
            "09:00", "10:00", "11:00",
            "12:00", "13:00", "14:00",
            "15:00", "16:00", "17:00",
            "18:00", "19:00", "20:00",
            "21:00", "22:00", "23:00"
        ],
        "data_format": "grib",
        "download_format": "unarchived"
    }
    
    print(f"开始下载 {date} 的数据...")
    # 下载数据
    client.retrieve(
        "reanalysis-era5-single-levels",
        request,
        output_file
    )
    print(f"完成下载 {date} 的数据")
    return output_file

def process_era5_data(): 
    # 读取我们之前生成的日期文件
    dates_df = pd.read_csv(r'data\debug\output\all_unique_dates.csv')
    
    # 创建输出目录
    output_dir = r"G:\002_Data\007_ERA5\000_weather"
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # 进度文件路径
    progress_file = os.path.join(output_dir, 'download_progress.json')
    
    # 加载已下载的进度
    progress = load_download_progress(progress_file)
    downloaded_dates = set(progress['downloaded_dates'])
    
    # 获取所有需要下载的日期
    all_dates = pd.to_datetime(dates_df['date'])
    all_dates = all_dates.sort_values()
    
    total_dates = len(all_dates)
    processed_count = 0
    
    try:
        for date in all_dates:
            date_str = date.strftime('%Y-%m-%d')
            processed_count += 1
            
            # 如果该日期已下载，跳过
            if date_str in downloaded_dates:
                print(f"跳过已下载的日期: {date_str} ({processed_count}/{total_dates})")
                continue
            
            try:
                print(f"处理日期: {date_str} ({processed_count}/{total_dates})")
                # 下载单天数据
                grib_file = download_era5_rainfall(date_str, output_dir)
                
                # 标记为已下载
                downloaded_dates.add(date_str)
                save_download_progress(progress_file, list(downloaded_dates))
                
            except Exception as e:
                print(f"下载 {date_str} 数据时发生错误: {str(e)}")
                # 保存进度并继续下一个日期
                save_download_progress(progress_file, list(downloaded_dates))
                continue
        
        print("所有ERA5数据下载完成！")
        
    except Exception as e:
        print(f"发生错误: {str(e)}")
        print("已保存下载进度，下次运行时将从中断处继续")
        # 确保在发生错误时也保存进度
        save_download_progress(progress_file, list(downloaded_dates))

if __name__ == "__main__":
    process_era5_data() 