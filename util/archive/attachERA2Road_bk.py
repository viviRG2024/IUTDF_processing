import xarray as xr
import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import box, Polygon
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

class WeatherGridProcessor:
    def __init__(self, city_name):
        """
        初始化处理器
        city_name: 城市名称
        """
        self.city_name = city_name
        self.data_dir = Path(f"data/001_Integrated Urban Traffic-Flood Dataset/{city_name}")
        self.processed_dir = Path(f"data/processed/{city_name}")
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
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
        file_path = Path(grib_file)
        target_date = pd.to_datetime(file_path.stem.split('_')[-1]).date()
        
        # 读取降水数据 (edition 1)
        ds_rain = xr.open_dataset(grib_file, engine='cfgrib', 
                                 backend_kwargs={'filter_by_keys': {'edition': 1}})
        
        # 获取城市边界
        city_bounds = self.get_city_bounds()
        
        # 创建基于ERA5的网格
        grid_gdf = self.create_era5_grid(ds_rain)
        
        # 只保留与城市边界相交的网格
        city_box = box(*city_bounds)
        grid_gdf['intersects_city'] = grid_gdf.geometry.intersects(city_box)
        grid_gdf = grid_gdf[grid_gdf['intersects_city']].copy()
        grid_gdf.to_parquet(self.processed_dir / 'debug_grid_info.parquet')
        grid_gdf = grid_gdf.drop(columns=['intersects_city'])
        
        # 重新编号grid_id
        # grid_gdf['grid_id'] = range(len(grid_gdf))
        
        # 创建时间序列数据
        time_data = []
        
        for time in ds_rain.time:
            time_slice = ds_rain.sel(time=time)
            base_time = pd.Timestamp(time.values)+ pd.Timedelta(hours=1)
            
            for idx, grid_cell in grid_gdf.iterrows():
                print(idx)
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
                print(tp_values)
                for hour in range(valid_hours):
                    current_time = base_time + pd.Timedelta(hours=hour)
                    print(hour)
                    print(current_time)
                    print(tp_values[hour])
                    # 只保留目标日期的数据
                    if current_time.date() == target_date:
                        time_data.append({
                            'datetime': current_time,
                            'grid_id': grid_cell.grid_id,
                            'longitude': grid_cell.longitude,
                            'latitude': grid_cell.latitude,
                            'total_precipitation': float(tp_values[hour]) if not np.isnan(tp_values[hour]) else 0.0,
                            'large_scale_rain_rate': float(lsrr_values[hour]) if not np.isnan(lsrr_values[hour]) else 0.0,
                            'precipitation_type': precip_type,
                            'forecast_base_time': base_time
                        })
        
        # 创建数据框
        rainfall_df = pd.DataFrame(time_data)
        
        # 检查是否有缺失的小时数据
        all_hours = pd.date_range(
            start=f"{target_date} 00:00",
            end=f"{target_date} 23:00",
            freq='H'
        )
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
    # 示例使用
    city = "torino"
    processor = WeatherGridProcessor(city)
    
    # 处理GRIB文件
    grib_file = r"data\processed\era5_data\era5_rainfall_2015-11-04.grib"
    grid_gdf, rainfall_df = processor.process_grib_data(grib_file)
    
    # 显示一些基本信息
    print(f"\n创建的网格数量: {len(grid_gdf)}")
    print("\n网格数据示例:")
    print(grid_gdf.head())
    print("\n降水数据示例:")
    print(rainfall_df.head())

if __name__ == "__main__":
    main()