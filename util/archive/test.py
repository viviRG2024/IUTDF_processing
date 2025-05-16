import xarray as xr
import cfgrib
from pathlib import Path
import numpy as np

def examine_grib_file(file_path):
    """详细检查GRIB文件的内容"""
    print(f"\n检查文件: {file_path}")
    print("="*50)
    
    try:
        # 尝试读取数据
        ds = xr.open_dataset(file_path, engine='cfgrib')
        
        # 1. 基本信息
        print("\n1. 基本信息:")
        print(f"维度: {ds.dims}")
        print(f"坐标: {list(ds.coords)}")
        print(f"数据变量: {list(ds.data_vars)}")
        
        # 2. 详细变量信息
        print("\n2. 变量详细信息:")
        for var_name, var in ds.data_vars.items():
            print(f"\n变量名: {var_name}")
            print(f"  - 维度: {var.dims}")
            print(f"  - 形状: {var.shape}")
            print(f"  - 数据类型: {var.dtype}")
            if hasattr(var, 'units'):
                print(f"  - 单位: {var.units}")
            if hasattr(var, 'long_name'):
                print(f"  - 描述: {var.long_name}")
            # 显示一些基本统计信息
            try:
                print(f"  - 最小值: {var.min().values}")
                print(f"  - 最大值: {var.max().values}")
                print(f"  - 平均值: {var.mean().values}")
            except Exception as e:
                print(f"  - 无法计算统计值: {str(e)}")
        
        # 3. 时间信息
        if 'time' in ds.coords:
            print("\n3. 时间信息:")
            print(f"时间范围: {ds.time.values[0]} 到 {ds.time.values[-1]}")
            print(f"时间点数量: {len(ds.time)}")
        
        # 4. 空间信息
        if 'latitude' in ds.coords and 'longitude' in ds.coords:
            print("\n4. 空间范围:")
            print(f"纬度范围: {ds.latitude.values.min()} 到 {ds.latitude.values.max()}")
            print(f"经度范围: {ds.longitude.values.min()} 到 {ds.longitude.values.max()}")
        
        ds.close()
        
    except Exception as e:
        print(f"读取文件时发生错误: {str(e)}")
        print("\n尝试使用不同的参数...")
        
        # 尝试不同的读取参数
        backends = [
            {'filter_by_keys': {'typeOfLevel': 'surface'}},
            {'filter_by_keys': {'edition': 1}},
            {'filter_by_keys': {'edition': 2}},
            {'filter_by_keys': {'shortName': 'tp'}}
        ]
        
        for backend_kwargs in backends:
            try:
                print(f"\n尝试参数: {backend_kwargs}")
                ds = xr.open_dataset(file_path, engine='cfgrib', backend_kwargs=backend_kwargs)
                print("成功读取！数据变量:", list(ds.data_vars))
                ds.close()
            except Exception as e:
                print(f"使用参数 {backend_kwargs} 失败: {str(e)}")
                continue

def examine_grib_data(file_path, backend_kwargs):
    """检查特定参数下的GRIB数据内容"""
    print(f"\n使用参数: {backend_kwargs}")
    print("-" * 50)
    
    ds = xr.open_dataset(file_path, engine='cfgrib', backend_kwargs=backend_kwargs)
    
    for var_name, var in ds.data_vars.items():
        print(f"\n变量: {var_name}")
        print(f"维度: {var.dims}")
        print(f"形状: {var.shape}")
        if hasattr(var, 'units'):
            print(f"单位: {var.units}")
        
        # 获取前10个不同的值
        try:
            # 将数据转换为一维数组并去除NaN值
            flat_data = var.values.ravel()
            valid_data = flat_data[~np.isnan(flat_data)]
            unique_values = np.unique(valid_data)[:10]
            print("前10个不同的值:")
            for i, value in enumerate(unique_values, 1):
                print(f"  {i}. {value}")
        except Exception as e:
            print(f"无法获取数值: {str(e)}")
    
    ds.close()

def main():
    # 查找所有grib文件
    data_dir = "data/processed/era5_data"
    grib_files = list(Path(data_dir).glob("*.grib"))
    
    if not grib_files:
        print("未找到.grib文件！")
        return
    
    # 检查第一个文件
    file_path = grib_files[0]
    print(f"检查文件: {file_path}")
    
    # 使用不同的参数读取数据
    backends = [
        {'filter_by_keys': {'edition': 1}},  # tp 和 lsrr
        {'filter_by_keys': {'edition': 2}},  # ptype
        {'filter_by_keys': {'shortName': 'tp'}}  # 只看tp
    ]
    
    for backend_kwargs in backends:
        try:
            examine_grib_data(file_path, backend_kwargs)
        except Exception as e:
            print(f"使用参数 {backend_kwargs} 失败: {str(e)}")

if __name__ == "__main__":
    # main()
    npz_file = r"data\001_Integrated Urban Traffic-Flood Dataset\torino\torino_data_hours.npz"
    data = np.load(npz_file)
    print(data.keys())
    print(data['data'].shape)
    print(data['data'][1])

