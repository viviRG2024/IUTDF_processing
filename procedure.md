# get road network of the city
- [spilt original data into city](./util/readOriginalData.py)
- [get OSMData](./util/getOSMData.py)
- [get centroidline]
- [attach sensor to centroidline](./util/attachSensorOnRoads.py)


# get weather date
- [collect all rainfall date](./util/collectAddress&Day.py)
- [get all unique dates](./util/getAllDate.py)
- [get weather data](./util/getERA5Data.py)

# data convert
- [covert sensor data to parquet data](./util/convertSensorCSV2Parquet.py)
- [convert detector info to parquet data](./util/convertDetectors2Parquet.py)
- [covert geojson data to npz data](./util/convertConnectivity2Npz.py)
- [convert npz data to pems series dataformat](./util/convert_to_pems_format.py)

# date integration
- [attach weather data to road networks](./util/processERA5CityData.py)
- [calculate hourly sensor data](./util/calculateHourlySensorData.py)
- [map road data to grid map](./util/attachRoad2Grid.py)
- [meta data generation](./util/metaData.py)
- [organize dat](./util/organizeData.py)

# visulize the weather data
- [visulize the weather data based on time series data](./util/visualizeGribSpatial.py)
- [visulize the weather data based on spatial data](./util/visualizeRainFallData.py)

# extra
- [time covert from utc to local time](./util/timeConvert.py)
- [check the weather data](./util/checkMissingData.py)
- [clean the weather data](./util/cleanData.py)
- [reset progress](./util/resetProgress.py)

# analysis
- [select rainy day and dry day](./analysis/rainfall_analysis.py)
- [plot traffic flow on rainy day and dray day](./analysis/traffic_weather_analysis.py)
- [plot rain traffic flow comparison data](./analysis/rain_traffic_comparison.py)
- [find 3 hours before and after raining time](./analysis/rainfall_timing_offset.py)
- [the connection between rainfall and traffic flow](./analysis/rainfall_intensity_impact.py)
- [final comparison](./analysis/rainfall_intensity_timing_impact.py)