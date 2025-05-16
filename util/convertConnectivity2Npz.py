import os
import glob
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely import wkb
import time

def convert_to_optimized_npz(city_folder):
    """
    将道路网络和传感器数据转换为优化的npz格式
    
    方法：
    1. 保存完整网络拓扑结构（所有道路）
    2. 单独存储传感器数据（只包含有传感器的道路）
    3. 建立映射关系（连接完整网络和传感器数据）
    
    参数:
    - city_folder: 城市数据文件夹路径
    
    返回:
    - 是否成功转换
    """
    city_name = os.path.basename(city_folder)
    print(f"Converting data for {city_name}...")
    
    # 文件路径
    network_parquet = os.path.join(city_folder, "selected_network.parquet")
    readings_parquet = os.path.join(city_folder, "5min_readings.parquet")
    output_npz = os.path.join(city_folder, f"{city_name}_traffic_network.npz")
    
    # 检查文件是否存在
    if not os.path.exists(network_parquet):
        print(f"Error: selected_network.parquet not found for {city_name}")
        return False
    
    if not os.path.exists(readings_parquet):
        print(f"Error: 5min_readings.parquet not found for {city_name}")
        return False
    
    try:
        start_time = time.time()
        
        # 1. 读取道路网络数据
        print("Reading road network data...")
        if "wkb" in pd.read_parquet(network_parquet, columns=None).columns:
            # 如果包含WKB列，需要转换回几何对象
            df = pd.read_parquet(network_parquet)
            geometry = df['wkb'].apply(lambda x: wkb.loads(x) if x else None)
            network_df = gpd.GeoDataFrame(df.drop(columns=['wkb']), geometry=geometry)
        else:
            # 直接读取
            network_df = pd.read_parquet(network_parquet)
        
        # 确保有road_id列，如果没有则尝试使用其他可能的ID列
        id_columns = ['road_id', 'id', 'edge_id', 'index']
        road_id_col = None
        for col in id_columns:
            if col in network_df.columns:
                road_id_col = col
                break
        
        if road_id_col is None:
            print("Warning: No road ID column found, creating sequential IDs")
            network_df['road_id'] = np.arange(len(network_df))
            road_id_col = 'road_id'
        
        # 2. 按road_id排序完整网络
        print(f"Sorting complete network by {road_id_col}...")
        network_df = network_df.sort_values(by=road_id_col).reset_index(drop=True)
        
        # 3. 识别有传感器的道路
        print("Identifying roads with sensors...")
        has_sensor_mask = network_df['detid'] != '-1'
        
        sensor_roads = network_df[has_sensor_mask].copy().reset_index(drop=True)
        print(f"Complete network: {len(network_df)} roads")
        print(f"Roads with sensors: {len(sensor_roads)}")
        
        # 4. 创建映射：完整网络索引 -> 传感器道路索引
        network_to_sensor = {}
        sensor_to_network = {}
        
        for i, (idx, road) in enumerate(sensor_roads.iterrows()):
            original_idx = network_df[network_df[road_id_col] == road[road_id_col]].index[0]
            network_to_sensor[original_idx] = i
            sensor_to_network[i] = original_idx
        
        # 5. 读取传感器数据
        print("Reading sensor data...")
        readings_df = pd.read_parquet(readings_parquet)
        
        # 6. 创建传感器ID到传感器道路索引的映射
        detid_to_sensor_idx = {}
        for i, (_, road) in enumerate(sensor_roads.iterrows()):
            detid_to_sensor_idx[road['detid']] = i
        
        # 7. 提取unique的时间戳
        time_column = None
        for col in ['datetime', 'timestamp', 'time', 'date']:
            if col in readings_df.columns:
                time_column = col
                break
        
        if time_column is None:
            print("Error: No time column found in readings data")
            return False
        
        timestamps = sorted(readings_df[time_column].unique())
        print(f"Found {len(timestamps)} unique timestamps")
        
        # 8. 创建传感器数据矩阵 (时间戳 x 传感器道路)
        # 假设有这些指标: flow, speed, occupancy
        data_columns = {}
        for col in ['flow', 'flow_sum', 'flow_mean_5min', 'speed', 'speed_mean', 'speed_weight', 'occ', 'occ_mean']:
            if col in readings_df.columns:
                data_columns[col] = readings_df.columns.get_loc(col)
        
        if not data_columns:
            print("Error: No valid data columns found in readings data")
            return False
        
        # 准备数组和时间索引映射
        time_to_idx = {t: i for i, t in enumerate(timestamps)}
        n_times = len(timestamps)
        n_sensor_roads = len(sensor_roads)
        
        # 为每个指标创建数组 - 只为有传感器的道路创建
        data_arrays = {}
        for col_name in data_columns:
            data_arrays[col_name] = np.full((n_times, n_sensor_roads), np.nan, dtype=np.float32)
        
        # 填充数据矩阵
        print("Building data matrices...")
        for _, row in readings_df.iterrows():
            if row['detid'] in detid_to_sensor_idx:
                t_idx = time_to_idx.get(row[time_column])
                r_idx = detid_to_sensor_idx[row['detid']]
                
                if t_idx is not None:
                    for col_name, col_idx in data_columns.items():
                        if not pd.isna(row[col_name]):
                            data_arrays[col_name][t_idx, r_idx] = row[col_name]
        
        # 9. 准备图结构数据
        adjacency_data = None
        if 'from_node' in network_df.columns and 'to_node' in network_df.columns:
            print("Building network connectivity...")
            # 创建节点ID映射
            unique_nodes = set()
            for _, road in network_df.iterrows():
                unique_nodes.add(road['from_node'])
                unique_nodes.add(road['to_node'])
            
            node_to_idx = {node: i for i, node in enumerate(sorted(unique_nodes))}
            n_nodes = len(node_to_idx)
            
            # 创建边列表
            edges = []
            for _, road in network_df.iterrows():
                from_idx = node_to_idx[road['from_node']]
                to_idx = node_to_idx[road['to_node']]
                edges.append((from_idx, to_idx))
            
            adjacency_data = {
                'edges': np.array(edges),
                'node_ids': np.array(list(node_to_idx.keys())),
                'node_mapping': node_to_idx
            }
        
        # 10. 保存道路属性
        network_attributes = {}
        for col in network_df.columns:
            if col != 'geometry' and col != 'wkb':
                network_attributes[col] = network_df[col].values
        
        sensor_attributes = {}
        for col in sensor_roads.columns:
            if col != 'geometry' and col != 'wkb':
                sensor_attributes[col] = sensor_roads[col].values
        
        # 11. 保存为npz文件
        save_dict = {
            # 时间数据
            'timestamps': np.array(timestamps),
            
            # 网络数据（完整）
            'network_road_ids': network_df[road_id_col].values,
            'network_detector_ids': network_df['detid'].values,
            'network_attributes': network_attributes,
            
            # 传感器道路数据（子集）
            'sensor_road_ids': sensor_roads[road_id_col].values,
            'sensor_detector_ids': sensor_roads['detid'].values,
            'sensor_attributes': sensor_attributes,
            
            # 映射关系
            'network_to_sensor_map': network_to_sensor,  # 网络索引 -> 传感器索引
            'sensor_to_network_map': sensor_to_network,  # 传感器索引 -> 网络索引
            
            # 传感器计数和网络计数
            'n_network_roads': len(network_df),
            'n_sensor_roads': len(sensor_roads)
        }
        
        # 添加各指标数据（只针对传感器道路）
        for col_name, array in data_arrays.items():
            save_dict[f'sensor_{col_name}'] = array
        
        # 添加网络连接结构(如果有)
        if adjacency_data:
            for key, value in adjacency_data.items():
                save_dict[f'network_{key}'] = value
        
        np.savez_compressed(output_npz, **save_dict)
        
        elapsed_time = time.time() - start_time
        print(f"Successfully converted {city_name} data to NPZ in {elapsed_time:.2f} seconds")
        print(f"Saved to: {output_npz}")
        
        # 12. 打印一些统计信息
        print("\nData summary:")
        print(f"Complete network roads: {len(network_df)}")
        print(f"Roads with sensors: {len(sensor_roads)} ({len(sensor_roads)/len(network_df)*100:.2f}%)")
        print(f"Number of time points: {n_times}")
        
        for col_name, array in data_arrays.items():
            non_nan = np.count_nonzero(~np.isnan(array))
            coverage = non_nan / (array.shape[0] * array.shape[1]) * 100
            print(f"  {col_name} coverage: {coverage:.2f}% ({non_nan} non-NaN values)")
        
        if adjacency_data:
            print(f"Network nodes: {len(adjacency_data['node_ids'])}")
            print(f"Network edges: {len(adjacency_data['edges'])}")
        
        return True
    
    except Exception as e:
        print(f"Error converting {city_name} data: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    # 根目录
    data_root = r"data\debug\input"
    
    # 获取所有城市文件夹
    city_folders = [d for d in glob.glob(os.path.join(data_root, "*")) if os.path.isdir(d)]
    
    print(f"Found {len(city_folders)} city folders")
    print("Using optimized structure: complete network + sensor data mapping")
    
    # 创建进度文件路径
    progress_file = os.path.join(data_root, "npz_conversion_progress.txt")
    
    # 从进度文件加载已处理的城市(如果存在)
    processed_cities = set()
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            processed_cities = set(line.strip() for line in f.readlines())
        print(f"Loaded progress: {len(processed_cities)} cities already processed")
    
    # 记录总体进度
    total_cities = len(city_folders)
    successful_count = len(processed_cities)
    
    # 处理每个城市
    for city_folder in city_folders:
        city_name = os.path.basename(city_folder)
        
        # 如果城市已经处理过，跳过
        if city_name in processed_cities:
            print(f"Skipping {city_name} - already processed")
            continue
        
        # 转换数据
        success = convert_to_optimized_npz(city_folder)
        
        # 如果处理成功，记录进度
        if success:
            successful_count += 1
            processed_cities.add(city_name)
            
            # 更新进度文件
            with open(progress_file, 'w') as f:
                for city in processed_cities:
                    f.write(f"{city}\n")
        
        # 显示当前进度
        print(f"Progress: {successful_count}/{total_cities} cities processed ({successful_count/total_cities*100:.1f}%)\n")
    
    print(f"Conversion complete! {successful_count}/{total_cities} cities successfully processed.")
    
    # 提供一个简单的示例，说明如何使用转换后的数据
    print("\n使用示例:")
    print("```python")
    print("# 加载NPZ文件")
    print("data = np.load('city_traffic_network.npz')")
    print("")
    print("# 访问完整网络数据")
    print("network_roads = data['network_road_ids']")
    print("network_detids = data['network_detector_ids']")
    print("")
    print("# 访问传感器数据")
    print("sensor_flow = data['sensor_flow']  # 时间点 x 传感器道路的矩阵")
    print("sensor_detids = data['sensor_detector_ids']")
    print("")
    print("# 使用映射关系")
    print("network_to_sensor = data['network_to_sensor_map'].item()  # 将字典从npz中提取出来")
    print("sensor_to_network = data['sensor_to_network_map'].item()")
    print("")
    print("# 示例：根据网络索引找到对应的传感器数据")
    print("network_idx = 42  # 道路网络中的某个道路索引")
    print("if network_idx in network_to_sensor:")
    print("    sensor_idx = network_to_sensor[network_idx]")
    print("    flow_data = sensor_flow[:, sensor_idx]  # 该道路的所有时间点流量数据")
    print("```")

if __name__ == "__main__":
    main() 