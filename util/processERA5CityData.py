import os
import sys
import pandas as pd
import geopandas as gpd
import xarray as xr
import numpy as np
from pathlib import Path
import json
import pytz
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Import custom modules
sys.path.append('.')  # Ensure the current directory is in the path
from util.TimeConverter import TimeConverter
from util.getERA5Data import download_era5_rainfall, load_download_progress, save_download_progress

class ERA5CityProcessor:
    def __init__(self, city_name, era5_root="G:/002_Data/007_ERA5/000_weather", 
                 processed_dir="data/processed/era5_city", city_data_dir=r"data\debug\input"):
        """
        初始化ERA5城市数据处理器
        
        Parameters:
        -----------
        city_name : str
            城市名称
        era5_root : str
            ERA5数据根目录
        processed_dir : str
            处理后数据保存目录
        city_data_dir : str
            城市原始数据目录
        """
        self.city_name = city_name
        self.era5_root = Path(era5_root)
        self.city_data_dir = Path(f"{city_data_dir}/{city_name}")
        self.processed_dir = Path(f"{processed_dir}/{city_name}")
        self.weather_dir = self.processed_dir / 'weather'
        
        # 创建必要的目录
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        self.weather_dir.mkdir(parents=True, exist_ok=True)
        
        # 时间转换器
        self.time_converter = TimeConverter()
        
        # 进度跟踪
        self.progress_file = self.processed_dir / 'era5_processing_progress.json'
        self.processed_dates = self._load_progress()
        
        # 获取城市时区
        self.timezone = self._get_city_timezone()
        print(f"城市 {city_name} 的时区: {self.timezone}")
        
    def _get_city_timezone(self):
        """获取城市时区"""
        # 这里可以根据城市名称或坐标查询时区
        # 简化版本：使用预定义的映射或默认时区
        # timezone_mapping = {
        #     "london": "Europe/London",
        #     "torino": "Europe/Rome",
        #     "paris": "Europe/Paris",
        #     "augsburg": "Europe/Berlin",
        #     "manchester": "Europe/London",
        #     "toronto": "America/Toronto",
        #     "luzern": "Europe/Zurich",
        #     "marseille": "Europe/Paris",
        #     "cagliari": "Europe/Rome",
        #     "taipeh": "Asia/Taipei",
        #     "essen": "Europe/Berlin",
        #     "darmstadt": "Europe/Berlin",
        #     "innsbruck": "Europe/Vienna",
        #     "strasbourg": "Europe/Paris",
        #     "hamburg": "Europe/Berlin",
        #     "madrid": "Europe/Madrid"
        # }
        timezone_mapping = {
            # Existing mappings from your original data
            "london": "Europe/London",
            "torino": "Europe/Rome",
            "paris": "Europe/Paris",
            "augsburg": "Europe/Berlin",
            "manchester": "Europe/London",
            "toronto": "America/Toronto",
            "luzern": "Europe/Zurich",
            "marseille": "Europe/Paris",
            "cagliari": "Europe/Rome",
            "taipeh": "Asia/Taipei",
            "essen": "Europe/Berlin",
            "darmstadt": "Europe/Berlin",
            "innsbruck": "Europe/Vienna",
            "strasbourg": "Europe/Paris",
            "hamburg": "Europe/Berlin",
            "madrid": "Europe/Madrid",

            # New mappings based on your list
            "constance": "Europe/Berlin",      # 康斯坦茨 (Konstanz), 德国
            "frankfurt": "Europe/Berlin",      # 法兰克福 (Frankfurt), 德国
            "losangeles": "America/Los_Angeles", # 洛杉矶 (Los Angeles), 美国
            "bremen": "Europe/Berlin",         # 不莱梅 (Bremen), 德国
            "stuttgart": "Europe/Berlin",      # 斯图加特 (Stuttgart), 德国
            "vilnius": "Europe/Vilnius",       # 维尔纽斯 (Vilnius), 立陶宛
            "groningen": "Europe/Amsterdam",   # 格罗宁根 (Groningen), 荷兰
            "zurich": "Europe/Zurich",         # 苏黎世 (Zurich), 瑞士
            "bordeaux": "Europe/Paris",        # 波尔多 (Bordeaux), 法国
            "wolfsburg": "Europe/Berlin",      # 沃尔夫斯堡 (Wolfsburg), 德国
            "basel": "Europe/Zurich",          # 巴塞尔 (Basel), 瑞士
            "toulouse": "Europe/Paris",        # 图卢兹 (Toulouse), 法国
            "speyer": "Europe/Berlin",         # 施派尔 (Speyer), 德国
            "bolton": "Europe/London",         # 博尔顿 (Bolton), 英国
            "birmingham": "Europe/London",     # 伯明翰 (Birmingham), 英国
            "rotterdam": "Europe/Amsterdam",   # 鹿特丹 (Rotterdam), 荷兰
            "kassel": "Europe/Berlin",         # 卡塞尔 (Kassel), 德国
            "munich": "Europe/Berlin",         # 慕尼黑 (Munich), 德国
            "bern": "Europe/Zurich",           # 伯尔尼 (Bern), 瑞士
            "melbourne": "Australia/Melbourne",# 墨尔本 (Melbourne), 澳大利亚
            "tokyo": "Asia/Tokyo",             # 东京 (Tokyo), 日本
            "utrecht": "Europe/Amsterdam",     # 乌得勒支 (Utrecht), 荷兰
            "santander": "Europe/Madrid",      # 桑坦德 (Santander), 西班牙
            "graz": "Europe/Vienna"            # 格拉茨 (Graz), 奥地利
        }
        
        return timezone_mapping.get(self.city_name, 'UTC')  # 默认使用UTC
    
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
        rainfall_file = self.city_data_dir / 'rainfall_data.csv'
        if not rainfall_file.exists():
            print(f"找不到降雨数据文件: {rainfall_file}")
            # 尝试查找其他可能的文件名
            alternative_files = list(self.city_data_dir.glob('*rainfall*.csv'))
            if alternative_files:
                rainfall_file = alternative_files[0]
                print(f"使用替代文件: {rainfall_file}")
            else:
                raise FileNotFoundError(f"找不到任何降雨数据文件")
        
        df = pd.read_csv(rainfall_file)
        
        # 检查是否有city列
        if 'city' in df.columns:
            dates = df[df['city'] == self.city_name]['date'].unique()
        else:
            # 如果没有city列，假设所有数据都属于当前城市
            dates = df['date'].unique()
        
        return dates
    
    def get_or_create_grid_info(self):
        """获取或创建网格信息"""
        grid_file = self.weather_dir / 'grid_info.parquet'
        
        if grid_file.exists():
            print(f"加载已有的网格信息: {grid_file}")
            return pd.read_parquet(grid_file)
        
        print("未找到网格信息，将从城市边界计算...")
        # 获取城市边界
        # network_file = self.city_data_dir / "selected_network_4326.geojson"
        network_file = self.city_data_dir / "selected_roads.gpkg"
        if not network_file.exists():
            # 尝试查找其他可能的文件名
            alternative_files = list(self.city_data_dir.glob('*network*4326*.geojson'))
            if alternative_files:
                network_file = alternative_files[0]
                print(f"使用替代文件: {network_file}")
            else:
                raise FileNotFoundError(f"找不到城市网络文件")
        
        road_network = gpd.read_file(network_file)
        bounds = road_network.total_bounds  # (minx, miny, maxx, maxy)
        
        # 找一个示例ERA5文件来创建网格 - 过滤掉.idx文件
        sample_files = [f for f in list(self.era5_root.glob('era5_rainfall_*.grib')) 
                        if not str(f).endswith('.idx')]
        if not sample_files:
            raise FileNotFoundError("找不到任何ERA5 GRIB文件来创建网格")
        
        sample_file = sample_files[0]
        print(f"使用示例文件创建网格: {sample_file}")
        
        # 读取示例文件
        ds_rain = xr.open_dataset(sample_file, engine='cfgrib', 
                                 backend_kwargs={'filter_by_keys': {'edition': 1}})
        
        # 创建网格
        grid_gdf = self._create_era5_grid(ds_rain, bounds)
        
        # 保存网格信息
        grid_gdf.to_parquet(grid_file)
        print(f"网格信息已保存到: {grid_file}")
        
        return grid_gdf
    
    def _create_era5_grid(self, ds, city_bounds):
        """基于ERA5数据创建网格"""
        # 获取ERA5的经纬度网格
        lons = ds.longitude.values
        lats = ds.latitude.values
        
        # 创建网格多边形
        grid_cells = []
        grid_ids = []
        grid_id = 0
        
        # ERA5网格的分辨率
        lon_res = np.abs(lons[1] - lons[0]) if len(lons) > 1 else 0.25
        lat_res = np.abs(lats[1] - lats[0]) if len(lats) > 1 else 0.25
        
        print(f"ERA5网格分辨率: 经度 {lon_res:.4f}°, 纬度 {lat_res:.4f}°")
        print(f"近似距离: {lon_res * 111} km x {lat_res * 111} km")
        
        # 处理城市边界
        minx, miny, maxx, maxy = city_bounds
        
        # 检查ERA5数据的经度范围
        era5_uses_0_to_360 = np.min(lons) >= 0 and np.max(lons) > 180
        print(f"ERA5数据使用 0-360° 经度系统: {era5_uses_0_to_360}")
        print(f"ERA5经度范围: {np.min(lons)} to {np.max(lons)}")
        print(f"原始城市边界经度范围: {minx} to {maxx}")
        
        # 创建包含城市的边界框
        from shapely.geometry import box
        
        # 处理跨越0°经线的城市
        crosses_prime_meridian = minx < 0 and maxx > 0
        
        if era5_uses_0_to_360:
            if crosses_prime_meridian:
                print(f"城市跨越0°经线，创建两个边界框")
                west_box = box(minx + 360, miny, 360, maxy)  # 西部边界框 (负经度部分转换为360°附近)
                east_box = box(0, miny, maxx, maxy)          # 东部边界框 (正经度部分)
                city_boxes = [west_box, east_box]
                
                # 调试信息
                print(f"西部边界框: [{minx + 360}, {miny}, 360, {maxy}]")
                print(f"东部边界框: [0, {miny}, {maxx}, {maxy}]")
            else:
                # 非跨越0°经线的城市，全部转换到0-360°系统
                adj_minx = minx + 360 if minx < 0 else minx
                adj_maxx = maxx + 360 if maxx < 0 else maxx
                city_boxes = [box(adj_minx, miny, adj_maxx, maxy)]
                print(f"转换后城市边界经度范围: {adj_minx} to {adj_maxx}")
        else:
            # ERA5使用-180到180系统
            city_boxes = [box(minx, miny, maxx, maxy)]
            print(f"城市边界经度范围: {minx} to {maxx}")
        
        # 检查每个经纬度
        print(f"开始检查 {len(lons)}x{len(lats)} = {len(lons)*len(lats)} 个网格点")
        
        # 存储原始网格和对应的标准化网格
        original_grid_cells = []
        standard_grid_cells = []
        
        for lon in lons:
            # 为每个经度值创建两个可能的表示，处理0/360和-180/180之间的转换
            lon_360 = lon
            lon_180 = lon if lon <= 180 else lon - 360
            
            for lat in lats:
                # 创建网格单元 (使用原始坐标系)
                cell_minx_orig = lon - lon_res/2
                cell_maxx_orig = lon + lon_res/2
                cell_miny = lat - lat_res/2
                cell_maxy = lat + lat_res/2
                
                original_cell_box = box(cell_minx_orig, cell_miny, cell_maxx_orig, cell_maxy)
                
                # 创建-180/180系统的表示
                cell_minx_std = lon_180 - lon_res/2
                cell_maxx_std = lon_180 + lon_res/2
                standard_cell_box = box(cell_minx_std, cell_miny, cell_maxx_std, cell_maxy)
                
                # 检查是否与任一城市边界框相交
                intersects = False
                for city_box in city_boxes:
                    if original_cell_box.intersects(city_box):
                        intersects = True
                        break
                
                if intersects:
                    original_grid_cells.append(original_cell_box)
                    standard_grid_cells.append(standard_cell_box)
                    grid_ids.append(grid_id)
                
                grid_id += 1
        
        # 如果没有找到相交的网格，尝试使用扩展边界
        if len(original_grid_cells) == 0:
            print("警告: 没有找到与城市相交的网格单元，尝试扩大边界...")
            
            # 扩大边界并重试
            expanded_city_boxes = []
            for city_box in city_boxes:
                bounds = city_box.bounds
                expanded_box = box(
                    bounds[0] - lon_res, 
                    bounds[1] - lat_res, 
                    bounds[2] + lon_res, 
                    bounds[3] + lat_res
                )
                expanded_city_boxes.append(expanded_box)
            
            for lon in lons:
                lon_180 = lon if lon <= 180 else lon - 360
                
                for lat in lats:
                    # 创建原始坐标系的网格单元
                    cell_minx_orig = lon - lon_res/2
                    cell_maxx_orig = lon + lon_res/2
                    cell_miny = lat - lat_res/2
                    cell_maxy = lat + lat_res/2
                    
                    original_cell_box = box(cell_minx_orig, cell_miny, cell_maxx_orig, cell_maxy)
                    
                    # 创建-180/180系统的表示
                    cell_minx_std = lon_180 - lon_res/2
                    cell_maxx_std = lon_180 + lon_res/2
                    standard_cell_box = box(cell_minx_std, cell_miny, cell_maxx_std, cell_maxy)
                    
                    # 检查是否与任一扩展边界框相交
                    intersects = False
                    for expanded_box in expanded_city_boxes:
                        if original_cell_box.intersects(expanded_box):
                            intersects = True
                            break
                    
                    if intersects:
                        original_grid_cells.append(original_cell_box)
                        standard_grid_cells.append(standard_cell_box)
                        grid_ids.append(grid_id)
                    
                    grid_id += 1
        
        # 创建GeoDataFrame - 使用标准化的(-180到180)坐标系网格几何数据
        grid_gdf = gpd.GeoDataFrame({
            'grid_id': grid_ids,
            'geometry': standard_grid_cells,  # 使用标准化的几何数据
            'longitude': [p.centroid.x for p in standard_grid_cells],  # 标准化的经度
            'latitude': [p.centroid.y for p in standard_grid_cells],
            'longitude_era5': [p.centroid.x for p in original_grid_cells]  # 保留原始ERA5经度作为参考
        }, crs="EPSG:4326")
        
        print(f"找到与城市相交的网格单元数量: {len(standard_grid_cells)}")
        
        # 检查转换是否成功
        print(f"标准化后的经度范围: {grid_gdf.geometry.bounds.minx.min():.4f} 到 {grid_gdf.geometry.bounds.maxx.max():.4f}")
        
        # 可视化验证
        try:
            import matplotlib.pyplot as plt
            fig, ax = plt.subplots(figsize=(10, 8))
            grid_gdf.plot(ax=ax, color='pink', edgecolor='black')
            
            # 尝试加载道路数据进行验证
            try:
                network_file = self.city_data_dir / "roads.gpkg"
                if not network_file.exists():
                    alternative_files = list(self.city_data_dir.glob('*network*4326*.geojson'))
                    if alternative_files:
                        network_file = alternative_files[0]
                
                if network_file.exists():
                    road_network = gpd.read_file(network_file)
                    road_network.plot(ax=ax, color='red', linewidth=0.5)
                    plt.title(f"{self.city_name} ERA5网格与道路网络叠加")
                else:
                    plt.title(f"{self.city_name} ERA5网格")
            except Exception as e:
                print(f"加载道路数据失败: {str(e)}")
                plt.title(f"{self.city_name} ERA5网格")
            
            plt.savefig(self.weather_dir / 'grid_verification.png', dpi=300)
            plt.close()
            print(f"网格验证图已保存到: {self.weather_dir / 'grid_verification.png'}")
        except Exception as e:
            print(f"创建验证图失败: {str(e)}")
        
        return grid_gdf
    
    def ensure_era5_data_available(self, utc_dates_needed):
        """确保指定UTC日期的ERA5数据可用"""
        missing_dates = []
        
        for date in utc_dates_needed:
            date_str = date.strftime('%Y-%m-%d')
            # 查找确切匹配的.grib文件，排除.idx文件
            grib_files = [f for f in self.era5_root.glob(f"era5_rainfall_{date_str}.grib") 
                         if not str(f).endswith('.idx')]
            
            if not grib_files:
                missing_dates.append(date_str)
        
        # 如果有缺失的日期，下载它们
        if missing_dates:
            print(f"需要下载以下日期的ERA5数据: {missing_dates}")
            self._download_missing_era5_data(missing_dates)
        
        return True
    
    def _download_missing_era5_data(self, missing_dates):
        """下载缺失的ERA5数据"""
        # 进度文件路径
        progress_file = self.era5_root / 'download_progress.json'
        
        # 加载已下载的进度
        progress = load_download_progress(str(progress_file))
        downloaded_dates = set(progress['downloaded_dates'])
        
        for date_str in missing_dates:
            # 如果该日期已下载，跳过
            if date_str in downloaded_dates:
                print(f"跳过已下载的日期: {date_str}")
                continue
            
            try:
                print(f"下载日期: {date_str} 的ERA5数据")
                # 下载单天数据
                download_era5_rainfall(date_str, str(self.era5_root))
                
                # 标记为已下载
                downloaded_dates.add(date_str)
                save_download_progress(str(progress_file), list(downloaded_dates))
                
            except Exception as e:
                print(f"下载 {date_str} 数据时发生错误: {str(e)}")
                continue
    
    def get_era5_data_for_local_date(self, local_date):
        """处理指定本地日期的数据"""
        # 转换为datetime对象
        if isinstance(local_date, str):
            local_date = pd.to_datetime(local_date).date()
        
        local_date_str = local_date.strftime('%Y-%m-%d')
        
        # 检查是否已处理
        if local_date_str in self.processed_dates:
            print(f"日期 {local_date_str} 已处理，跳过")
            return
        
        print(f"处理日期: {local_date_str}")
        
        # 获取网格信息
        grid_info = self.get_or_create_grid_info()
        
        # 生成当天的所有小时 (本地时间)
        local_hours = pd.date_range(
            start=f"{local_date_str} 00:00",
            end=f"{local_date_str} 23:00",
            freq='H'
        )
        
        # 将本地时间转换为UTC时间，并收集所有需要的UTC日期
        utc_times = []
        utc_dates_needed = set()
        
        for local_hour in local_hours:
            # 将本地时间转换为UTC
            conversion_result = self.time_converter.local_to_utc(local_hour, self.timezone)
            utc_time = conversion_result['utc_time']
            utc_times.append(utc_time)
            utc_dates_needed.add(utc_time.date())
        # print("local_hours")
        # print(local_hours)
        # print("utc_times")
        # print(utc_times)
        # 确保所有需要的UTC日期数据都可用
        self.ensure_era5_data_available(utc_dates_needed)
        
        # 为每个本地时间点获取对应的ERA5数据
        all_hourly_data = []
        
        for i, local_hour in enumerate(local_hours):
            utc_time = utc_times[i]
            
            # 获取对应的ERA5数据
            hourly_data = self._get_era5_data_for_time(utc_time, grid_info)
            
            if hourly_data is not None:
                # 添加本地时间信息
                # local_time_no_tz = local_hour.tz_localize(None)
                hourly_data['local_time'] = local_hour.strftime('%Y-%m-%d %H:%M:%S')
                # hourly_data['local_time'] = local_hour.tz_localize(None)
                all_hourly_data.append(hourly_data)
            else:
                print(f"警告: 无法获取本地时间 {local_hour} (UTC: {utc_time}) 的ERA5数据")
        
        # 合并所有小时的数据
        if all_hourly_data:
            combined_data = pd.concat(all_hourly_data, ignore_index=True)
            
            # 保存处理后的数据
            output_file = self.weather_dir / f'local_hourly_rainfall_{local_date_str}.parquet'
            combined_data.to_parquet(output_file)
            print(f"已保存 {local_date_str} 的本地时间降水数据到 {output_file}")
            
            # 更新进度
            self._save_progress(local_date_str)
        else:
            print(f"警告: {local_date_str} 没有有效的ERA5数据")
    
    def _get_era5_data_for_time(self, utc_time, grid_info):
        """获取指定UTC时间的ERA5数据"""
        # 获取日期字符串
        utc_date_str = utc_time.strftime('%Y-%m-%d')
        
        # 构建GRIB文件路径，确保不使用.idx文件
        grib_pattern = self.era5_root / f"era5_rainfall_{utc_date_str}.grib"
        grib_files = [f for f in self.era5_root.glob(grib_pattern.name) 
                     if not str(f).endswith('.idx')]
        
        if not grib_files:
            print(f"错误: 找不到 {utc_date_str} 的GRIB文件")
            return None
        
        grib_file = grib_files[0]  # 使用找到的第一个有效GRIB文件
        
        try:
            # 读取降水数据 (edition 1)
            ds_rain = xr.open_dataset(grib_file, engine='cfgrib', 
                                     backend_kwargs={'filter_by_keys': {'edition': 1}})
            
            # 获取所有时间点
            era5_times = ds_rain.time.values
            # print("era5_times")
            # print(era5_times)
            
            # 找到最接近的时间区间起始点
            # 将utc_time转换为np.datetime64格式以进行比较
            utc_np_time = np.datetime64(utc_time)
            
            # 找到小于或等于目标时间的最大时间点(即最接近的前一个时间段起始点)
            valid_times = era5_times[era5_times <= utc_np_time]
            if len(valid_times) == 0:
                print(f"错误: 在GRIB文件中找不到适合 {utc_time} 的时间区间")
                return None
            
            closest_time = valid_times[-1]  # 取最后一个(最接近的)时间点
            
            # 计算小时偏移量
            time_diff = utc_time - pd.Timestamp(closest_time)
            hour_offset = int(time_diff.total_seconds() / 3600)
            
            print(f"目标时间: {utc_time}, 最近区间起始点: {closest_time}, 小时偏移: {hour_offset}")
            
            # 确保小时偏移在有效范围内(通常是0-11小时)
            if hour_offset < 0 or hour_offset >= 12:
                print(f"警告: 计算的小时偏移({hour_offset})超出了预期范围(0-11)")
                if hour_offset >= 12:
                    print(f"尝试查找下一个时间区间")
                    # 可能需要检查下一天的数据
                    next_day_utc = utc_time + pd.Timedelta(days=1)
                    next_day_str = next_day_utc.strftime('%Y-%m-%d')
                    next_grib_pattern = self.era5_root / f"era5_rainfall_{next_day_str}.grib"
                    next_grib_files = [f for f in self.era5_root.glob(next_grib_pattern.name) 
                                     if not str(f).endswith('.idx')]
                    
                    if not next_grib_files:
                        print(f"错误: 找不到 {next_day_str} 的GRIB文件")
                        return None
                    
                    try:
                        ds_next = xr.open_dataset(next_grib_files[0], engine='cfgrib', 
                                               backend_kwargs={'filter_by_keys': {'edition': 1}})
                        era5_times_next = ds_next.time.values
                        for t in era5_times_next:
                            if t > utc_np_time:
                                closest_time = t
                                time_diff = utc_time - pd.Timestamp(closest_time)
                                hour_offset = int(time_diff.total_seconds() / 3600)
                                if 0 <= hour_offset < 12:
                                    print(f"在下一天找到合适的时间区间: {closest_time}, 新的小时偏移: {hour_offset}")
                                    ds_rain = ds_next  # 使用新的数据集
                                    break
                    except Exception as e:
                        print(f"读取下一天数据时出错: {str(e)}")
            
            # 提取该时间点的数据切片
            time_slice = ds_rain.sel(time=closest_time)
            
            # 收集所有网格的数据
            grid_data = []
            
            for _, grid_cell in grid_info.iterrows():
                # 获取该网格的数据
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
                        time=closest_time,
                        longitude=grid_cell.longitude,
                        latitude=grid_cell.latitude,
                        method='nearest'
                    )
                    # print("type_data")
                    # print(type_data.ptype.values)
                    
                    if hasattr(type_data, 'ptype'):
                        if type_data.ptype.values.size > hour_offset:
                            precip_type = float(type_data.ptype.values[hour_offset])
                            if np.isnan(precip_type):
                                precip_type = 0
                        elif type_data.ptype.values.size == 1:
                            precip_type = float(type_data.ptype.values.item())
                        else:
                            print(f"警告: type_data.ptype.values大小({type_data.ptype.values.size})小于小时偏移({hour_offset})")
                            precip_type = 0.0
                    else:
                        precip_type = -1
                        
                    ds_type.close()
                except Exception as e:
                    precip_type = -1
                
                # 安全地提取tp和lsrr值 - 考虑到数组形式的数据
                try:
                    # 处理tp值 - 使用计算出的小时偏移
                    if hasattr(cell_data, 'tp'):
                        tp_values = cell_data.tp.values
                        if tp_values.size > hour_offset:
                            tp_value = float(tp_values[hour_offset])
                        elif tp_values.size == 1:
                            tp_value = float(tp_values.item())
                        else:
                            print(f"警告: tp_values大小({tp_values.size})小于小时偏移({hour_offset})")
                            tp_value = 0.0
                    else:
                        tp_value = 0.0
                    
                    # 处理lsrr值 - 使用计算出的小时偏移
                    if hasattr(cell_data, 'lsrr'):
                        lsrr_values = cell_data.lsrr.values
                        if lsrr_values.size > hour_offset:
                            lsrr_value = float(lsrr_values[hour_offset])
                        elif lsrr_values.size == 1:
                            lsrr_value = float(lsrr_values.item())
                        else:
                            print(f"警告: lsrr_values大小({lsrr_values.size})小于小时偏移({hour_offset})")
                            lsrr_value = 0.0
                    else:
                        lsrr_value = 0.0
                
                except Exception as e:
                    print(f"提取降水数据时出错: {str(e)}")
                    tp_value = 0.0
                    lsrr_value = 0.0
                
                # 添加到结果中
                grid_data.append({
                    'utc_time': utc_time.tz_localize('UTC'),  # 使用原始的目标UTC时间
                    'grid_id': grid_cell.grid_id,
                    'longitude': grid_cell.longitude,
                    'latitude': grid_cell.latitude,
                    'total_precipitation': tp_value if not np.isnan(tp_value) else 0.0,
                    'large_scale_rain_rate': lsrr_value if not np.isnan(lsrr_value) else 0.0,
                    'precipitation_type': precip_type
                })
            
            # 创建数据框
            return pd.DataFrame(grid_data)
            
        except Exception as e:
            print(f"处理 {utc_time} 的ERA5数据时出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
    
    def process_all_dates(self):
        """处理所有目标日期的数据"""
        target_dates = self.get_target_dates()
        
        for date in target_dates:
            try:
                # 确保日期格式正确
                formatted_date = pd.to_datetime(date).strftime('%Y-%m-%d')
                self.get_era5_data_for_local_date(formatted_date)
            except Exception as e:
                print(f"处理日期 {date} 时出错: {str(e)}")
                import traceback
                traceback.print_exc()
                continue
        
        print(f"城市 {self.city_name} 的所有日期处理完成！")

def main():
    # 设置数据目录
    city_dir = Path(r'data\debug\input')
    era5_root = Path(r"G:\002_Data\007_ERA5\000_weather")
    output_dir = Path(r'data\debug\output\city_whole')
    
    # 创建全局进度文件
    global_progress_file = Path(r'data\debug\output\global_era5_processing_progress.json')
    
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
                processor = ERA5CityProcessor(city, era5_root, output_dir, city_dir)
                processor.process_all_dates()
                
                # 更新并保存全局进度
                processed_cities.add(city)
                with open(global_progress_file, 'w') as f:
                    json.dump(list(processed_cities), f)
                
                print(f"城市 {city} 处理完成")
                
            except Exception as e:
                print(f"处理城市 {city} 时出错: {str(e)}")
                import traceback
                traceback.print_exc()
                continue
            
    except KeyboardInterrupt:
        print("\n检测到用户中断，保存进度并退出...")
        with open(global_progress_file, 'w') as f:
            json.dump(list(processed_cities), f)
        sys.exit(0)
    
    print("\n所有城市处理完成！")

if __name__ == "__main__":
    main() 