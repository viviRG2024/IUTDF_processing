import pandas as pd
import numpy as np
from pathlib import Path
import json
import warnings
warnings.filterwarnings("ignore")

from TimeConverter import TimeConverter


class DataCompleteness:
    def __init__(self):
        self.timezone_mapping = {
            "london": "Europe/London",
            "torino": "Europe/Rome",
            "paris": "Europe/Paris",
            "augsburg": "Europe/Berlin",
            "manchester": "Europe/London",
            "toronto": "America/Toronto",
            "luzern": "Europe/Geneva",
            "marseille": "Europe/Paris",
            "cagliari": "Europe/Rome",
            "taipeh": "Asia/Taipei",
            "essen": "Europe/Berlin",
            "darmstadt": "Europe/Berlin",
            "innsbruck": "Europe/Vienna",
            "strasbourg": "Europe/Paris",
            "hamburg": "Europe/Berlin",
            "madrid": "Europe/Madrid"
        }
        self.time_converter = TimeConverter()

    def analyze_city_data(self, city_name, era5_root, processed_dir):
        """分析单个城市的数据完整性"""
        city_timezone = self.timezone_mapping.get(city_name.lower(), 'UTC')
        print(f"\nAnalyzing city {city_name} (Timezone: {city_timezone})")
        
        # 读取已处理的数据
        processed_path = Path(processed_dir) / city_name / 'weather'
        if not processed_path.exists():
            print(f"Cannot find processed data directory: {processed_path}")
            return
        
        # 获取所有已处理的日期
        processed_files = list(processed_path.glob('hourly_rainfall_*.parquet'))
        processed_dates = [pd.to_datetime(f.stem.split('_')[-1]).date() for f in processed_files]
        
        # 分析每个日期
        for date in processed_dates:
            print(f"\nChecking date: {date}")
            
            # 读取当前日期的数据
            df = pd.read_parquet(processed_path / f'hourly_rainfall_{date}.parquet')
            
            # 使用TimeConverter进行时间转换和检查
            utc_start = pd.Timestamp(f"{date} 00:00")
            utc_end = pd.Timestamp(f"{date} 23:00")
            
            # 转换为本地时间并检查夏令时
            start_result = self.time_converter.utc_to_local(utc_start, city_timezone)
            end_result = self.time_converter.utc_to_local(utc_end, city_timezone)
            
            # 显示夏令时信息
            if start_result['dst_transition']:
                print(f"Special case: {start_result['dst_transition']}")
                print(f"Local time range: {start_result['local_time']} to {end_result['local_time']}")
            
            # 获取预期的本地时间范围
            local_hours = pd.date_range(
                start=start_result['local_time'],
                end=end_result['local_time'],
                freq='h'
            )
            
            # 获取实际的小时
            actual_hours = pd.to_datetime(df['datetime'].unique())
            
            # 检查缺失的小时
            missing_hours = set(local_hours) - set(actual_hours)
            if missing_hours:
                print("\nMissing hours:")
                for missing_hour in sorted(missing_hours):
                    # 转换回UTC时间来确定需要的GRIB文件
                    utc_result = self.time_converter.local_to_utc(missing_hour, city_timezone)
                    print(f"Local time: {missing_hour}, UTC time: {utc_result['utc_time']}")
                    
                    # 检查是否需要额外的GRIB文件
                    grib_date = utc_result['utc_time'].date()
                    grib_file = Path(era5_root) / f"era5_rainfall_{grib_date}.grib"
                    if not grib_file.exists():
                        print(f"Required GRIB file: era5_rainfall_{grib_date}.grib")
            
            # 分析数据分布
            print(f"Data time range: {min(actual_hours)} to {max(actual_hours)}")
            print(f"Number of data points: {len(actual_hours)}")
            
            # 检查是否有异常值
            grid_counts = df.groupby('datetime').size()
            if grid_counts.nunique() > 1:
                print("Warning: Inconsistent number of grid points across timestamps")
                print(grid_counts.value_counts())

    def analyze_all_cities(self, era5_root="G:/002_Data/007_ERA5", processed_dir="data/processed/era5_city"):
        """分析所有城市的数据完整性"""
        processed_dir = Path(processed_dir)
        cities = [d.name for d in processed_dir.iterdir() if d.is_dir()]
        
        print(f"Found {len(cities)} cities")
        
        # 创建报告
        report = {
            'analysis_time': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
            'cities': {}
        }
        
        for city in cities:
            try:
                city_report = {
                    'missing_dates': [],
                    'incomplete_dates': [],
                    'required_grib_files': set()
                }
                if city == 'toronto':
                    print(f"Analyzing city {city}")
                    self.analyze_city_data(city, era5_root, processed_dir)
                    report['cities'][city] = city_report
                
            except Exception as e:
                print(f"Error analyzing city {city}: {str(e)}")
                report['cities'][city] = {'error': str(e)}
        
        # 保存报告
        with open(processed_dir / 'data_completeness_report.json', 'w') as f:
            json.dump(report, f, indent=2, default=str)

def main():
    analyzer = DataCompleteness()
    analyzer.analyze_all_cities()

if __name__ == "__main__":
    main() 