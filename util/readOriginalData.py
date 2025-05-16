import pandas as pd
import os
import dask.dataframe as dd

def process_links_and_detectors(in_dir, out_dir):
    """处理 links.csv 和 detectors_public.csv，并按城市代码拆分数据。

    Args:
        in_dir (str): 输入目录。
        out_dir (str): 输出目录。
    """
    links_df = pd.read_csv(os.path.join(in_dir, 'links.csv'))
    detectors_df = pd.read_csv(os.path.join(in_dir, 'detectors_public.csv'))

    city_codes = set(links_df['citycode']).union(set(detectors_df['citycode']))

    for city_code in city_codes:
        folder_name = os.path.join(out_dir, str(city_code))  # Ensure city_code is converted to string
        os.makedirs(folder_name, exist_ok=True)  # 使用 exist_ok=True 避免重复创建文件夹的错误

        links_city_df = links_df[links_df['citycode'] == city_code]
        detectors_city_df = detectors_df[detectors_df['citycode'] == city_code]

        links_city_df.to_csv(os.path.join(folder_name, 'links.csv'), index=False)
        detectors_city_df.to_csv(os.path.join(folder_name, 'detectors_public.csv'), index=False)

        print(f"Data saved to {folder_name}")

def process_utd_data(in_path, out_dir):
    """处理 utd19_u.csv，并按城市拆分数据。

    Args:
        in_path (str): 输入文件路径。
        out_dir (str): 输出目录。
    """
    df = dd.read_csv(in_path, assume_missing=True)
    cities = df['city'].unique().compute()

    for city in cities:
        city_df = df[df['city'] == city]
        out_path = os.path.join(out_dir, str(city), f'{city}_sensor.csv')  # Ensure city is converted to string
        os.makedirs(os.path.dirname(out_path), exist_ok=True) # 使用 exist_ok=True 避免重复创建文件夹的错误
        city_df.to_csv(out_path, single_file=True)
        print(f'{city}_data.csv has been saved!')

if __name__ == '__main__':
    in_dir = r'C:\D\OneDrive\Documents\002_UCL\005_Phd\003_code\002_data\002_traffic_count\eth_dataset'
    out_dir = r'data\debug\input'

    in_path_utd = os.path.join(in_dir, 'utd19_u.csv')

    process_links_and_detectors(in_dir, out_dir)
    print("process_links_and_detectors done!!!!!!!!!!")
    process_utd_data(in_path_utd, out_dir)
    print("process_utd_data done!!!!!!!!!!")