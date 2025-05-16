import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import os
from datetime import timedelta

def get_continuous_periods(dates):
    """识别连续的时间段"""
    dates = sorted(dates)
    periods = []
    current_period = [dates[0]]
    
    for i in range(1, len(dates)):
        current_date = pd.to_datetime(dates[i])
        previous_date = pd.to_datetime(dates[i-1])
        
        if (current_date - previous_date).days == 1:
            current_period.append(dates[i])
        else:
            periods.append(current_period)
            current_period = [dates[i]]
    
    periods.append(current_period)
    return periods

def plot_city_rainfall(city_name, data_dir="data/processed/ear5_city"):
    """为指定城市创建降水量时序图"""
    # 设置图表风格
    plt.style.use('seaborn-v0_8')  # 使用 matplotlib 内置的 seaborn 风格
    sns.set_palette("husl")
    
    # 读取该城市的所有降水数据
    weather_dir = Path(data_dir) / city_name / 'weather'
    if not weather_dir.exists():
        print(f"找不到城市 {city_name} 的数据目录")
        return
    
    # 读取网格信息
    grid_info = pd.read_parquet(weather_dir / 'grid_info.parquet')
    grid_count = len(grid_info)
    
    # 收集所有数据文件
    data_files = list(weather_dir.glob('local_hourly_rainfall_*.parquet'))
    if not data_files:
        print(f"城市 {city_name} 没有找到降水数据")
        return
    
    # 获取所有日期
    dates = [pd.to_datetime(f.stem.split('_')[-1]).strftime('%Y-%m-%d') 
            for f in data_files if f.stem.startswith('local_hourly_rainfall_')]
    
    # 识别连续的时间段
    periods = get_continuous_periods(dates)
    
    # 创建输出目录
    output_dir = Path(data_dir) / city_name / 'plots'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 为每个连续时间段创建图表
    for period in periods:
        # 收集该时间段的数据
        period_data = []
        for date in period:
            df = pd.read_parquet(weather_dir / f'local_hourly_rainfall_{date}.parquet')
            period_data.append(df)
        
        if not period_data:
            continue
            
        # 合并数据
        combined_df = pd.concat(period_data)
        combined_df['datetime'] = pd.to_datetime(combined_df['local_time'])
        
        # 创建图表
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10), height_ratios=[2, 1])
        
        # 创建时间段名称
        if len(period) == 1:
            period_name = period[0]
        else:
            period_name = f"{period[0]}_to_{period[-1]}"
            
        fig.suptitle(f'{city_name.capitalize()} Rainfall Analysis\n{period_name}', fontsize=16)
        
        # 计算每个时间点的统计值
        hourly_stats = combined_df.groupby('datetime').agg({
            'total_precipitation': ['mean', 'std', 'min', 'max'],
            'large_scale_rain_rate': ['mean', 'std', 'min', 'max']
        })
        
        # 绘制总降水量
        ax1.fill_between(hourly_stats.index, 
                         hourly_stats[('total_precipitation', 'mean')] - hourly_stats[('total_precipitation', 'std')],
                         hourly_stats[('total_precipitation', 'mean')] + hourly_stats[('total_precipitation', 'std')],
                         alpha=0.3)
        ax1.plot(hourly_stats.index, hourly_stats[('total_precipitation', 'mean')], 
                 label='Total Precipitation (mean)', linewidth=2)
        ax1.set_ylabel('Total Precipitation (mm)')
        ax1.legend()
        
        # 绘制大尺度降水率
        ax2.fill_between(hourly_stats.index,
                         hourly_stats[('large_scale_rain_rate', 'mean')] - hourly_stats[('large_scale_rain_rate', 'std')],
                         hourly_stats[('large_scale_rain_rate', 'mean')] + hourly_stats[('large_scale_rain_rate', 'std')],
                         alpha=0.3)
        ax2.plot(hourly_stats.index, hourly_stats[('large_scale_rain_rate', 'mean')], 
                 label='Large Scale Rain Rate (mean)', linewidth=2, color='green')
        ax2.set_ylabel('Large Scale Rain Rate (mm/h)')
        ax2.legend()
        
        # 设置x轴格式
        for ax in [ax1, ax2]:
            ax.grid(True)
            ax.set_xlabel('Time')
            plt.setp(ax.get_xticklabels(), rotation=45)
        
        # 添加网格信息
        plt.figtext(0.02, 0.02, f'Number of ERA5 grids: {grid_count}', fontsize=8)
        
        # 调整布局
        plt.tight_layout()
        
        # 保存图表
        plt.savefig(output_dir / f'{city_name}_rainfall_{period_name}.png', dpi=300, bbox_inches='tight')
        plt.close()

def main():
    data_dir = "data/processed/ear5_city"
    
    # 获取所有城市
    cities = [d for d in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, d))]
    
    print(f"找到 {len(cities)} 个城市")
    for city in cities:
        print(f"\n处理城市: {city}")
        try:
            plot_city_rainfall(city, data_dir)
            print(f"成功创建 {city} 的降水分析图")
        except Exception as e:
            print(f"处理城市 {city} 时出错: {str(e)}")

if __name__ == "__main__":
    main() 