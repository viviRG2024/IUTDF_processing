import xarray as xr
import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import box, Polygon
from pathlib import Path
import warnings
import json
warnings.filterwarnings('ignore')

import os
import sys

class WeatherGridProcessor:
    def __init__(self, city_name, era5_root="G:/002_Data/007_ERA5", processed_dir="data/processed"):
        """
        初始化处理器
        city_name: 城市名称
        era5_root: ERA5数据根目录
        """
        self.city_name = city_name
        self.era5_root = Path(era5_root)
        self.data_dir = Path(f"data/001_Integrated Urban Traffic-Flood Dataset/{city_name}")
        self.processed_dir = Path(f"{processed_dir}/{city_name}")
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
        # 断点续传相关
        self.progress_file = self.processed_dir / 'processing_progress.json'
        self.processed_dates = self._load_progress()
    
    def _load_progress(self):
        """加载处理进度"""
        if self.progress_file.exists():
            with open(self.progress_file, 'r') as f:
                return set(json.load(f))
        return set()
    
    def _save_progress(self, date):
        """保存处理进度"""
        self.processed_dates.add(str(date))
        with open(self.progress_file, 'w') as f:
            json.dump(list(self.processed_dates), f)
    
    def get_target_dates(self):
        """获取需要处理的日期列表"""
        rainfall_file = self.data_dir / 'rainfall_data.csv'
        if not rainfall_file.exists():
            raise FileNotFoundError(f"找不到降雨数据文件: {rainfall_file}")
        
        df = pd.read_csv(rainfall_file)
        return df[df['city'] == self.city_name]['date'].unique()
    
    def process_all_dates(self):
        """处理所有目标日期的数据"""
        target_dates = self.get_target_dates()
        
        for date in target_dates:
            # 确保日期格式正确
            try:
                formatted_date = pd.to_datetime(date).strftime('%Y-%m-%d')
                # 检查是否已处理
                if formatted_date in self.processed_dates:
                    print(f"日期 {formatted_date} 已处理，跳过")
                    continue
                
                # 构建GRIB文件路径
                grib_file = self.era5_root / f"era5_rainfall_{formatted_date}.grib"
                
                if not grib_file.exists():
                    print(f"警告：找不到日期 {formatted_date} 的GRIB文件")
                    continue
                
                try:
                    print(f"处理日期: {formatted_date}")
                    grid_gdf, rainfall_df = self.process_grib_data(str(grib_file))
                    print(f"日期 {formatted_date} 处理完成")
                    
                    # 保存进度
                    self._save_progress(formatted_date)
                    
                except Exception as e:
                    print(f"处理日期 {formatted_date} 时出错: {str(e)}")
                    continue
                
            except Exception as e:
                print(f"日期格式错误 {date}: {str(e)}")
                continue
    
    def get_city_bounds(self):
        """获取城市边界"""
        network_file = self.data_dir / "selected_network_4326.geojson"
        road_network = gpd.read_file(network_file)
        bounds = road_network.total_bounds  # (minx, miny, maxx, maxy)
        return bounds
    
    def create_era5_grid(self, ds):
        """基于ERA5数据创建网格"""
        # 获取ERA5的经纬度网格
        lons = ds.longitude.values
        lats = ds.latitude.values
        
        # 创建网格多边形
        grid_cells = []
        grid_ids = []
        grid_id = 0
        
        # ERA5网格的分辨率
        lon_res = np.abs(lons[1] - lons[0])
        lat_res = np.abs(lats[1] - lats[0])
        
        print(f"ERA5网格分辨率: 经度 {lon_res:.4f}°, 纬度 {lat_res:.4f}°")
        print(f"近似距离: {lon_res * 111} km x {lat_res * 111} km")
        
        for lon in lons:
            for lat in lats:
                # 创建网格单元（考虑到ERA5的网格中心点）
                minx = lon - lon_res/2
                maxx = lon + lon_res/2
                miny = lat - lat_res/2
                maxy = lat + lat_res/2
                
                polygon = box(minx, miny, maxx, maxy)
                grid_cells.append(polygon)
                grid_ids.append(grid_id)
                grid_id += 1
        
        # 创建GeoDataFrame
        grid_gdf = gpd.GeoDataFrame({
            'grid_id': grid_ids,
            'geometry': grid_cells,
            'longitude': [p.centroid.x for p in grid_cells],
            'latitude': [p.centroid.y for p in grid_cells],
            'area': [p.area for p in grid_cells]
        }, crs="EPSG:4326")
        return grid_gdf
    
    def process_grib_data(self, grib_file):
        """处理GRIB文件"""
        # 从文件名获取目标日期
        # print(grib_file)
        file_path = Path(grib_file)
        try:
            # 更严格的日期解析
            target_date = pd.to_datetime(file_path.stem.split('_')[-1]).date()
        except Exception as e:
            print(f"日期解析错误: {str(e)}")
            raise
        # print(target_date)
        # 读取降水数据 (edition 1)
        ds_rain = xr.open_dataset(grib_file, engine='cfgrib', 
                                 backend_kwargs={'filter_by_keys': {'edition': 1}})
        # print(ds_rain)
        # 获取城市边界
        city_bounds = self.get_city_bounds()
        minx, miny, maxx, maxy = city_bounds
        
        # 转换负经度到0-360系统
        if minx < 0:
            minx += 360
        if maxx < 0:
            maxx += 360
        
        converted_bounds = (minx, miny, maxx, maxy)
        print(f"原始城市边界坐标: {city_bounds}")
        print(f"转换后的城市边界坐标: {converted_bounds}")
        
        # 创建基于ERA5的网格
        grid_gdf = self.create_era5_grid(ds_rain)
        # print(f"ERA5网格范围: {grid_gdf.total_bounds}")
        
        # 只保留与城市边界相交的网格
        city_box = box(*converted_bounds)
        # print(f"转换后的城市边界框: {city_box.bounds}")
        
        # 保存调试信息
        city_box_gdf = gpd.GeoDataFrame(geometry=[city_box], crs="EPSG:4326")
        city_box_gdf.to_file(self.processed_dir / 'city_box.geojson', driver='GeoJSON')
        grid_gdf.to_file(self.processed_dir / 'era5_grid.geojson', driver='GeoJSON')
        
        # 检查相交情况
        grid_gdf['intersects_city'] = grid_gdf.geometry.intersects(city_box)
        intersecting_grids = grid_gdf[grid_gdf['intersects_city']]
        # print(f"相交网格数量: {len(intersecting_grids)}")
        
        if len(intersecting_grids) == 0:
            print("警告：没有网格与城市边界相交！")
            print("ERA5网格范围:", grid_gdf.total_bounds)
            print("转换后的城市边界范围:", city_box.bounds)
            raise ValueError("没有找到相交的网格")
        
        grid_gdf = intersecting_grids.copy()
        grid_gdf = grid_gdf.drop(columns=['intersects_city'])
        # print(grid_gdf)
        # 重新编号grid_id
        # grid_gdf['grid_id'] = range(len(grid_gdf))
        
        # 创建时间序列数据
        time_data = []
        
        for time in ds_rain.time:
            time_slice = ds_rain.sel(time=time)
            base_time = pd.Timestamp(time.values)+ pd.Timedelta(hours=1)
            
            for idx, grid_cell in grid_gdf.iterrows():
                # print(idx)
                cell_data = time_slice.sel(
                    longitude=grid_cell.longitude,
                    latitude=grid_cell.latitude,
                    method='nearest'
                )
                
                # 获取降水类型数据
                try:
                    ds_type = xr.open_dataset(grib_file, engine='cfgrib',
                                            backend_kwargs={'filter_by_keys': {'edition': 2}})
                    type_data = ds_type.sel(
                        time=time,
                        longitude=grid_cell.longitude,
                        latitude=grid_cell.latitude,
                        method='nearest'
                    )
                    precip_type = float(type_data.ptype.values.item())
                    ds_type.close()
                except Exception as e:
                    precip_type = -1
                
                # 处理12小时的预报数据
                tp_values = cell_data.tp.values
                lsrr_values = cell_data.lsrr.values
                
                # 只处理有效的数据（非nan的值）
                valid_hours = len(tp_values)
                # print(valid_hours)
                for hour in range(valid_hours):
                    current_time = base_time + pd.Timedelta(hours=hour)
                    # print(current_time)  # Removed debug print
                    # 只保留目标日期的数据
                    if current_time.date() == target_date:
                        time_data.append({
                            'datetime': pd.Timestamp(current_time).tz_localize('UTC'),  # Explicitly set UTC timezone
                            'grid_id': grid_cell.grid_id,
                            'longitude': grid_cell.longitude,
                            'latitude': grid_cell.latitude,
                            'total_precipitation': float(tp_values[hour]) if not np.isnan(tp_values[hour]) else 0.0,
                            'large_scale_rain_rate': float(lsrr_values[hour]) if not np.isnan(lsrr_values[hour]) else 0.0,
                            'precipitation_type': precip_type,
                            'forecast_base_time': pd.Timestamp(base_time).tz_localize('UTC')  # Also set UTC for base_time
                        })
        
        # 创建数据框
        rainfall_df = pd.DataFrame(time_data)
        
        # 检查是否有缺失的小时数据
        all_hours = pd.date_range(
            start=f"{target_date} 00:00",
            end=f"{target_date} 23:00",
            freq='H'
        )
        # print(rainfall_df)
        missing_hours = set(all_hours) - set(rainfall_df['datetime'].unique())
        if missing_hours:
            print(f"警告：以下时间点的数据缺失：{sorted(missing_hours)}")
        
        # 保存处理后的数据
        output_dir = self.processed_dir / 'weather'
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # 保存网格信息
        grid_gdf.to_parquet(output_dir / 'grid_info.parquet')
        
        # 保存降水数据，使用文件名中的日期
        rainfall_df.to_parquet(output_dir / f'hourly_rainfall_{target_date}.parquet')
        
        return grid_gdf, rainfall_df

def main():
    # 设置数据目录
    city_dir = Path('data/001_Integrated Urban Traffic-Flood Dataset')
    era5_root = Path("G:/002_Data/007_ERA5/000_weather")
    output_dir = Path('data/processed/era5_city')
    
    # 创建全局进度文件
    global_progress_file = city_dir / 'global_processing_progress.json'
    
    # 加载全局进度
    processed_cities = set()
    if global_progress_file.exists():
        with open(global_progress_file, 'r') as f:
            processed_cities = set(json.load(f))
    
    # 获取所有城市
    cities = [city for city in os.listdir(city_dir) if (city_dir / city).is_dir()]
    
    # 显示总体进度
    total_cities = len(cities)
    processed_count = len(processed_cities)
    print(f"总城市数: {total_cities}")
    print(f"已处理城市数: {processed_count}")
    print(f"剩余城市数: {total_cities - processed_count}")
    
    try:
        for city in cities:
            if city in processed_cities:
                print(f"\n城市 {city} 已处理完成，跳过")
                continue
            
            print(f"\n开始处理城市: {city} ({len(processed_cities) + 1}/{total_cities})")
            try:
                processor = WeatherGridProcessor(city, era5_root, output_dir)
                processor.process_all_dates()
                
                # 更新并保存全局进度
                processed_cities.add(city)
                with open(global_progress_file, 'w') as f:
                    json.dump(list(processed_cities), f)
                
                print(f"城市 {city} 处理完成")
                
            except Exception as e:
                print(f"处理城市 {city} 时出错: {str(e)}")
                continue
            
    except KeyboardInterrupt:
        print("\n检测到用户中断，保存进度并退出...")
        with open(global_progress_file, 'w') as f:
            json.dump(list(processed_cities), f)
        sys.exit(0)
    
    print("\n所有城市处理完成！")

if __name__ == "__main__":
    main()