# IUTFD: Integrated Urban Traffic Flow & Weather Data Framework

A comprehensive framework for processing and analyzing the relationship between urban traffic flow and weather data, with a particular focus on rainfall. This project provides a complete workflow from data collection and processing to analysis and visualization, helping researchers understand how rainfall impacts urban traffic patterns.

## Project Overview

Urban traffic flow is influenced by various factors, with weather conditions (especially rainfall) being an important but often underestimated variable. This framework integrates:

- City road network data (from OpenStreetMap)
- Real-time traffic sensor data
- High-precision rainfall and weather data (from ERA5)

Through the integration and analysis of these datasets, the complex relationships between rainfall intensity, timing, and traffic flow can be revealed, providing scientific basis for urban traffic management and planning.

## Dataset Availability

The dataset associated with this project can be accessed via the following link:

[👉 Download the dataset](https://drive.google.com/drive/folders/1gaaqQSwLy7aojxeW1pWW9S7Ovzz8sFAB?usp=sharing)
This dataset includes urban traffic flow data combined with weather observations, with a particular emphasis on rainfall conditions. If you encounter any issues, feel free to raise an issue in this repository.

## Key Features

### Data Acquisition & Processing
- Retrieve and process city road network data
- Collect traffic sensor data
- Obtain and process ERA5 weather and rainfall data
- Data format conversion (CSV, Parquet, GeoJSON, NPZ)

### Data Integration
- Attach sensors to road networks
- Integrate weather data with road networks
- Calculate hourly sensor data
- Generate standardized metadata

### Visualization & Analysis
- Time series and spatial rainfall data visualization
- Comparative analysis of traffic flow on rainy versus dry days
- Analysis of rainfall intensity impact on traffic
- Analysis of traffic changes before and after rainfall (3-hour offset)

## Project Structure

```
├── util/                      # Utility and data processing scripts
│   ├── readOriginalData.py    # Read and split original data
│   ├── getOSMData.py          # Get OpenStreetMap data
│   ├── attachSensorOnRoads.py # Attach sensors to roads
│   ├── collectAddress&Day.py  # Collect address and date information
│   ├── getAllDate.py          # Get all unique dates
│   ├── getERA5Data.py         # Get ERA5 weather data
│   ├── getRainData.py         # Get rainfall data
│   ├── convertSensorCSV2Parquet.py # Convert sensor data to Parquet format
│   ├── convertDetectors2Parquet.py # Convert detector info to Parquet format
│   ├── convertConnectivity2Npz.py  # Convert connectivity data to NPZ format
│   ├── processERA5CityData.py # Process city ERA5 data
│   ├── calculateHourlySensorData.py # Calculate hourly sensor data
│   ├── attachRoad2Grid.py     # Map road data to grid
│   ├── metaData.py            # Generate metadata
│   ├── organizeData.py        # Organize and structure data
│   ├── visualizeGribSpatial.py # Visualize spatial grid data
│   ├── visualizeRainFallData.py # Visualize rainfall data
│   └── TimeConverter.py       # UTC and local time conversion
│
├── analysis/                  # Analysis scripts
│   ├── rainfall_analysis.py   # Select rainy and dry days
│   ├── traffic_weather_analysis.py # Plot traffic flow on rainy and dry days
│   ├── rain_traffic_comparison.py # Rainfall and traffic comparison
│   ├── rainfall_timing_offset.py # 3-hour before/after rainfall analysis
│   ├── rainfall_intensity_impact.py # Rainfall intensity impact on traffic
│   └── rainfall_intensity_timing_impact.py # Final comprehensive analysis
│
└── data/                      # Data directory (not included in repo)
    └── debug/
        └── IUTFD/             # Integrated urban traffic flow and weather data
            ├── [city]/        # Data for each city
            │   ├── datetime/  # Date-time related data
            │   ├── npz/       # NPZ format data
            │   ├── roads/     # Road network data
            │   ├── sensors/   # Sensor data
            │   └── weather/   # Weather data
```

## Workflow

1. **Obtain Road Network Data**
   - Split original data
   - Get OSM data
   - Get centerline
   - Attach sensors to centerline

2. **Obtain Weather Data**
   - Collect rainfall dates
   - Get all unique dates
   - Obtain ERA5 weather data

3. **Data Conversion**
   - Convert sensor data to Parquet format
   - Convert detector information to Parquet format
   - Convert GeoJSON to NPZ format
   - Convert to PEMS series data format

4. **Data Integration**
   - Attach weather data to road networks
   - Calculate hourly sensor data
   - Map road data to grid
   - Generate metadata and organize data

5. **Data Visualization & Analysis**
   - Visualize time series and spatial weather data
   - Analyze traffic differences on rainy versus dry days
   - Study rainfall intensity impact on traffic
   - Analyze traffic changes before and after rainfall (3-hour offset)

## Dependencies

- Python 3.8+
- pandas
- numpy
- matplotlib
- seaborn
- scipy
- geopandas
- pyarrow (for Parquet support)
- osmnx (for OpenStreetMap data)
- xarray (for ERA5 data)

## Applications

This framework can be used for:
- Urban traffic management and planning
- Development of intelligent transportation systems
- Studies on rainfall impact on traffic
- Integrated analysis of traffic and weather data
- Establishment of traffic prediction models

## Contributing

Issue reports and code contributions are welcome. Please ensure you follow the project's code style and contribution guidelines.

## License

MIT Lisence
