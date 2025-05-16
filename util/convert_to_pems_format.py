import numpy as np
import pandas as pd
import geopandas as gpd
import os
import networkx as nx
from pathlib import Path
from tqdm import tqdm
from shapely.geometry import Point, LineString

def convert_to_pems_format(city_name, output_dir=None):
    """
    将城市交通数据转换为类似PEMS格式
    
    参数:
    - city_name: 城市名称
    - output_dir: 输出目录，默认为"data/debug/pems_format/{city_name}"
    
    输出:
    - {city_name}_distance.csv: 包含from_node, to_node, distance三列的CSV文件
    - {city_name}_data.npz: 包含流量、速度、占有率的NPZ文件
    """
    # 设置路径
    if output_dir is None:
        output_dir = f"data/debug/pems_format/{city_name}"
    
    input_path = f"data/debug/IUTFD/{city_name}/npz/{city_name}_traffic_network.npz"
    geojson_path = f"data/debug/input/{city_name}/selected_network_4326.geojson"
    
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"正在处理 {city_name} 城市数据...")
    
    # 加载NPZ数据
    try:
        data = np.load(input_path, allow_pickle=True)
        print(f"成功加载 {input_path}")
    except Exception as e:
        print(f"加载数据失败: {str(e)}")
        return
    
    # 获取传感器数据
    if 'sensor_flow' in data.files and 'sensor_speed' in data.files and 'sensor_occ' in data.files:
        sensor_flow = data['sensor_flow']  # 流量数据
        sensor_speed = data['sensor_speed']  # 速度数据
        sensor_occ = data['sensor_occ']  # 占有率数据
        
        # 检查数据维度
        print(f"传感器流量数据形状: {sensor_flow.shape}")
        print(f"传感器速度数据形状: {sensor_speed.shape}")
        print(f"传感器占有率数据形状: {sensor_occ.shape}")
        
        # 获取时间戳和传感器ID
        timestamps = data['timestamps'] if 'timestamps' in data.files else None
        sensor_ids = data['sensor_road_ids'] if 'sensor_road_ids' in data.files else None
        
        if timestamps is not None:
            print(f"时间戳数量: {len(timestamps)}")
            print(f"样例时间戳: {timestamps[:3]}")
        
        if sensor_ids is not None:
            print(f"传感器数量: {len(sensor_ids)}")
            print(f"样例传感器ID: {sensor_ids[:5]}")
        
        # 获取道路长度信息
        road_lengths = {}
        
        if 'sensor_attributes' in data.files and data['sensor_attributes'].size > 0:
            sensor_attr = data['sensor_attributes'].item() if hasattr(data['sensor_attributes'], 'item') else data['sensor_attributes']
            if isinstance(sensor_attr, dict) and 'road_length' in sensor_attr and 'road_id' in sensor_attr:
                for i, road_id in enumerate(sensor_attr['road_id']):
                    road_lengths[road_id] = float(sensor_attr['road_length'][i])
                print(f"从sensor_attributes中加载了{len(road_lengths)}条道路长度信息")
        
        # 获取网络属性（可能包含更多道路）
        if 'network_attributes' in data.files and data['network_attributes'].size > 0:
            network_attr = data['network_attributes'].item() if hasattr(data['network_attributes'], 'item') else data['network_attributes']
            if isinstance(network_attr, dict) and 'road_length' in network_attr and 'road_id' in network_attr:
                for i, road_id in enumerate(network_attr['road_id']):
                    if road_id not in road_lengths:  # 避免覆盖传感器道路长度
                        road_lengths[road_id] = float(network_attr['road_length'][i])
                print(f"从network_attributes中加载了更多道路长度信息，总计{len(road_lengths)}条")
        
        # 将传感器ID转换为集合，以便快速查找
        sensor_id_set = set(map(int, sensor_ids))
        
        # 使用完整的GeoJSON数据创建道路网络
        try:
            # 加载GeoJSON数据
            if os.path.exists(geojson_path):
                print(f"加载GeoJSON数据: {geojson_path}")
                roads_gdf = gpd.read_file(geojson_path)
                print(f"GeoJSON数据形状: {roads_gdf.shape}")
                print(f"GeoJSON数据列: {roads_gdf.columns.tolist()}")
                
                # 创建网络图
                G, all_road_ids, road_length_dict = create_road_network_from_geojson(roads_gdf)
                print(f"创建了包含 {len(all_road_ids)} 条道路的网络")
                
                # 创建连接关系
                print("从道路网络图中提取连接关系...")
                connections = []
                for u, v, data in G.edges(data=True):
                    # 直接使用GeoJSON中的road_length而不是计算距离
                    from_length = road_length_dict.get(u, 50.0)
                    to_length = road_length_dict.get(v, 50.0)
                    # 对于边，使用实际的道路长度而不是计算的距离
                    distance = data.get('road_length', max(from_length, to_length))
                    
                    connections.append({
                        'from_node': u,
                        'to_node': v,
                        'distance': distance
                    })
                
                print(f"从GeoJSON创建了 {len(connections)} 个道路连接")
                
                # 创建包含所有道路的数据数组
                n_timestamps = len(timestamps) if timestamps is not None else sensor_flow.shape[0]
                n_roads = len(all_road_ids)
                
                print(f"创建形状为 ({n_timestamps}, {n_roads}, 3) 的数据数组...")
                
                # 创建一个映射，将road_id映射到在all_road_ids中的索引
                road_id_to_index = {road_id: i for i, road_id in enumerate(all_road_ids)}
                
                # 创建数据数组，初始化为-1（表示没有传感器数据）
                full_data = np.full((n_timestamps, n_roads, 3), -1, dtype=np.float32)
                
                # 为有传感器的道路填充实际数据
                for i, sensor_id in enumerate(sensor_ids):
                    int_sensor_id = int(sensor_id)
                    if int_sensor_id in road_id_to_index:
                        idx = road_id_to_index[int_sensor_id]
                        full_data[:, idx, 0] = sensor_flow[:, i]  # 流量
                        full_data[:, idx, 1] = sensor_speed[:, i]  # 速度
                        full_data[:, idx, 2] = sensor_occ[:, i]    # 占有率
                
                # 检查有多少道路有实际的传感器数据
                has_data_count = sum(1 for road_id in all_road_ids if road_id in sensor_id_set)
                print(f"在 {n_roads} 条道路中，有 {has_data_count} 条道路有传感器数据")
                
                # 保存连接和距离为CSV
                connections_df = pd.DataFrame(connections)
                distance_path = os.path.join(output_dir, f"{city_name}_distance.csv")
                connections_df.to_csv(distance_path, index=False)
                print(f"距离数据已保存至: {distance_path}")
                
                # 保存为NPZ格式
                npz_path = os.path.join(output_dir, f"{city_name}_data.npz")
                np.savez_compressed(npz_path, data=full_data)
                print(f"数据已保存至: {npz_path}")
                
                # 保存元数据（包括所有道路ID）
                meta_data = {
                    "timestamps": timestamps,
                    "road_ids": np.array(all_road_ids),
                    "sensor_road_ids": sensor_ids,  # 保留原始传感器ID以供参考
                }
                meta_path = os.path.join(output_dir, f"{city_name}_meta.npz")
                np.savez_compressed(meta_path, **meta_data)
                print(f"元数据已保存至: {meta_path}")
                
                print(f"{city_name} 数据转换完成!")
                return
            else:
                print(f"GeoJSON文件不存在: {geojson_path}")
        except Exception as e:
            print(f"使用GeoJSON创建道路网络时出错: {str(e)}")
        
        # 如果GeoJSON处理失败，回退到只使用传感器数据
        print("回退到只使用传感器数据...")
        
        # 创建基本连接关系
        connections = create_connections_from_road_lengths(sensor_ids, road_lengths)
        
        # 保存连接和距离为CSV
        connections_df = pd.DataFrame(connections)
        distance_path = os.path.join(output_dir, f"{city_name}_distance.csv")
        connections_df.to_csv(distance_path, index=False)
        print(f"距离数据已保存至: {distance_path}")
        
        # 将原始数据转换为三维数组 [时间戳, 节点数, 特征]
        n_timestamps, n_sensors = sensor_flow.shape
        pems_data = np.zeros((n_timestamps, n_sensors, 3), dtype=np.float32)
        
        pems_data[:, :, 0] = sensor_flow  # 流量
        pems_data[:, :, 1] = sensor_speed  # 速度
        pems_data[:, :, 2] = sensor_occ    # 占有率
        
        # 保存为NPZ格式
        npz_path = os.path.join(output_dir, f"{city_name}_data.npz")
        np.savez_compressed(npz_path, data=pems_data)
        print(f"数据已保存至: {npz_path}")
        
        # 保存元数据
        if timestamps is not None:
            meta_data = {
                "timestamps": timestamps,
                "road_ids": sensor_ids,
            }
            meta_path = os.path.join(output_dir, f"{city_name}_meta.npz")
            np.savez_compressed(meta_path, **meta_data)
            print(f"元数据已保存至: {meta_path}")
        
        print(f"{city_name} 数据转换完成! (使用备用方法)")
    else:
        print("数据结构中缺少必要的传感器数据!")
        return

def create_road_network_from_geojson(roads_gdf):
    """
    从GeoJSON道路数据创建网络图
    
    参数:
    - roads_gdf: 包含道路几何信息的GeoDataFrame
    
    返回:
    - G: NetworkX图对象
    - all_road_ids: 所有道路ID的列表
    - road_length_dict: 道路ID到长度的映射字典
    """
    print("从GeoJSON创建道路网络...")
    
    # 创建一个空的无向图
    G = nx.Graph()
    
    # 检查必要的列
    if 'road_id' not in roads_gdf.columns:
        # 尝试找到可能的ID列
        id_columns = [col for col in roads_gdf.columns if 'id' in col.lower()]
        if id_columns:
            print(f"使用 {id_columns[0]} 作为道路ID")
            roads_gdf['road_id'] = roads_gdf[id_columns[0]]
        else:
            print("警告: 道路ID列不存在，使用索引作为ID")
            roads_gdf['road_id'] = range(len(roads_gdf))
    
    # 确保road_id是整数类型
    try:
        roads_gdf['road_id'] = roads_gdf['road_id'].astype(int)
    except:
        print("警告: 无法将道路ID转换为整数，使用原始值")
    
    # 检查是否有road_length列
    if 'road_length' not in roads_gdf.columns:
        print("警告: GeoJSON中没有road_length列，将尝试查找其他长度列")
        # 尝试找到可能的长度列
        length_columns = [col for col in roads_gdf.columns if 'length' in col.lower()]
        if length_columns:
            print(f"使用 {length_columns[0]} 作为道路长度")
            roads_gdf['road_length'] = roads_gdf[length_columns[0]]
        else:
            print("警告: 未找到长度列，将使用几何长度")
            # 计算几何长度 (注意: 在EPSG:4326中可能不准确)
            roads_gdf['road_length'] = roads_gdf.geometry.length * 111000  # 粗略转换为米
    
    # 创建道路ID到长度的映射
    road_length_dict = {}
    
    # 将道路添加为节点
    all_road_ids = []
    for idx, row in tqdm(roads_gdf.iterrows(), total=len(roads_gdf), desc="添加道路节点"):
        road_id = row['road_id']
        road_length = row['road_length']
        
        G.add_node(road_id, geometry=row.geometry, road_length=road_length)
        all_road_ids.append(road_id)
        road_length_dict[road_id] = road_length
    
    # 查找相交的道路并添加为边
    print("查找相交的道路...")
    # 创建一个R树索引以加速空间查询
    sindex = roads_gdf.sindex
    
    # 对于每条道路，找到与其相交的其他道路
    for idx, row in tqdm(roads_gdf.iterrows(), total=len(roads_gdf), desc="寻找道路连接"):
        road_id = row['road_id']
        road_length = row['road_length']
        geom = row.geometry
        
        # 使用空间索引查找可能相交的道路
        possible_matches_idx = list(sindex.intersection(geom.bounds))
        possible_matches = roads_gdf.iloc[possible_matches_idx]
        
        # 筛选出实际相交的道路
        for idx2, row2 in possible_matches.iterrows():
            if idx == idx2:
                continue  # 跳过自身
                
            road_id2 = row2['road_id']
            road_length2 = row2['road_length']
            geom2 = row2.geometry
            
            if geom.intersects(geom2):
                # 直接使用road_length作为边属性，而不是计算任何距离
                # 使用两条道路中较长的一条作为连接长度
                edge_length = max(road_length, road_length2)
                
                # 添加边
                G.add_edge(road_id, road_id2, weight=edge_length, road_length=edge_length)
    
    print(f"道路网络创建完成，共有 {G.number_of_nodes()} 个节点和 {G.number_of_edges()} 条边")
    
    # 返回图和所有道路ID的列表以及道路长度字典
    return G, all_road_ids, road_length_dict

def create_connections_from_road_lengths(sensor_ids, road_lengths):
    """
    基于道路长度创建连接关系 (备用方法)
    
    使用启发式方法：ID接近的道路可能在空间上也接近
    """
    print("使用备用方法创建道路连接...")
    connections = []
    n_sensors = len(sensor_ids)
    
    # 为每个传感器创建到其他传感器的连接
    # 仅连接ID相近的传感器 (作为简单的空间临近性启发式方法)
    max_neighbor_distance = 5  # 仅连接ID距离不超过5的传感器
    
    for i in range(n_sensors):
        source_id = int(sensor_ids[i])
        source_len = road_lengths.get(source_id, 50.0)  # 如果没有长度信息，使用默认值
        
        # 连接到ID临近的节点
        for j in range(max(0, i-max_neighbor_distance), min(n_sensors, i+max_neighbor_distance+1)):
            if i == j:
                continue  # 跳过自身连接
            
            target_id = int(sensor_ids[j])
            target_len = road_lengths.get(target_id, 50.0)
            
            # 使用两条道路中较长的一条作为连接长度
            distance = max(source_len, target_len)
            
            connections.append({
                'from_node': source_id,
                'to_node': target_id,
                'distance': distance
            })
    
    print(f"备用方法创建了 {len(connections)} 个道路连接")
    return connections

def process_all_cities():
    """处理所有可用城市的数据"""
    iutfd_dir = Path("data/debug/IUTFD")
    
    # 获取所有城市目录
    city_dirs = [d for d in iutfd_dir.iterdir() if d.is_dir()]
    city_names = [d.name for d in city_dirs]
    
    print(f"找到 {len(city_names)} 个城市")
    
    for city in city_names:
        try:
            # 检查是否存在NPZ文件
            npz_file = iutfd_dir / city / "npz" / f"{city}_traffic_network.npz"
            if not npz_file.exists():
                print(f"跳过 {city} - 未找到 {city}_traffic_network.npz")
                continue
            
            # 处理该城市的数据
            convert_to_pems_format(city)
        except Exception as e:
            print(f"处理 {city} 时出错: {str(e)}")

if __name__ == "__main__":
    # 处理单个城市
    # convert_to_pems_format("augsburg")
    
    # 或者处理所有城市
    process_all_cities() 