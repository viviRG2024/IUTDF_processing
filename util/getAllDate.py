import os
import pandas as pd
from pathlib import Path

def get_all_unique_dates():
    # 设置数据集根目录
    root_dir = r"data\debug\input"
    # 使用字典来存储日期和对应的城市
    date_sources = {}

    # 遍历根目录下的所有城市文件夹
    for city_dir in os.listdir(root_dir):
        city_path = os.path.join(root_dir, city_dir)
        print(f"processing: {city_dir}")
        # 确保是目录而不是文件
        if os.path.isdir(city_path):
            rainfall_file = os.path.join(city_path, "rainfall_data.csv")
            
            # 检查rainfall_data.csv是否存在
            if os.path.exists(rainfall_file):
                # 读取CSV文件
                df = pd.read_csv(rainfall_file)
                # 获取该城市的所有日期
                for date in df['date'].unique():
                    if date in date_sources:
                        date_sources[date].add(city_dir)
                    else:
                        date_sources[date] = {city_dir}

    # 创建包含日期和数据来源的列表
    data_list = []
    for date in sorted(date_sources.keys()):
        data_list.append({
            'date': date,
            'datasource': ';'.join(sorted(date_sources[date]))
        })
    
    # 创建DataFrame
    dates_df = pd.DataFrame(data_list)
    
    # 确保输出目录存在
    output_dir = "data/processed"
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # 保存到CSV文件
    output_file = os.path.join(output_dir, "all_unique_dates.csv")
    dates_df.to_csv(output_file, index=False)
    print(f"已保存所有唯一日期到: {output_file}")
    print(f"总共发现 {len(data_list)} 个唯一日期")

if __name__ == "__main__":
    get_all_unique_dates()
