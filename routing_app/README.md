# Route Optimization Dashboard

A Dash web application that displays optimized delivery routes retrieved from a Databricks SQL warehouse using the Databricks Python SDK. The app provides interactive route visualization with detailed statistics for route optimization analysis.

## Features

- **Route Selection**: Dropdown to select from available routes with truck type information
- **Interactive Map**: Uses Folium to display route visualization with:
  - Color-coded stops showing visit order
  - Connecting lines between stops
  - Popup information for each stop
- **Route Statistics**: Shows comprehensive route details including:
  - Route ID and truck type
  - Total number of stops
  - Package IDs for all deliveries
- **Databricks Integration**: Seamlessly queries route data from your Databricks SQL warehouse

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Databricks Authentication

This application uses the Databricks CLI for authentication. Set up your credentials:

```bash
# Install Databricks CLI if not already installed
pip install databricks-cli

# Login to your Databricks workspace
databricks auth login
```

The app will use your default profile for authentication.

### 3. Configure Environment Variables

Set the following environment variables, or modify the values directly in `config.py`:

```bash
export DATABRICKS_WAREHOUSE_ID="your-warehouse-id"
export ROUTES_TABLE="your_catalog.your_schema.your_routes_table"
```

### 4. Expected Table Schema

Your routes table should have the following columns:

- `cluster_id`: Identifier for each route (STRING)
- `truck_type`: Type of truck used for the route (STRING)
- `route_index`: Order of stops within a route (INT)
- `package_id`: Identifier for each package/stop (STRING)
- `latitude`: Decimal latitude coordinates (DOUBLE)
- `longitude`: Decimal longitude coordinates (DOUBLE)

## Usage

1. Run the application:
```bash
python app.py
```

2. Open your browser and navigate to `http://localhost:8050`

3. Select a route from the dropdown to view its visualization and statistics

## How It Works

1. **Connection**: The app connects to your Databricks SQL warehouse using the Databricks Python SDK with CLI authentication
2. **Route Loading**: It queries for distinct routes (cluster_id and truck_type) to populate the dropdown
3. **Route Visualization**: When a route is selected, it retrieves all stops for that route ordered by route_index
4. **Map Generation**: The `plot_route_folium` function creates an interactive Folium map with:
   - Markers for each stop colored by visit order
   - Lines connecting consecutive stops
   - Popup information showing package details
5. **Statistics**: Real-time statistics are displayed showing route details and package information

## Files

- `app.py`: Main Dash application with callbacks for route selection and visualization
- `config.py`: Configuration settings for Databricks connection and table schema
- `plotting.py`: Contains the route visualization function using Folium
- `requirements.txt`: Python dependencies

## Dependencies

- `folium==0.20.0`: Interactive map visualization
- `dash==2.14.1`: Web application framework
- `pandas==2.1.4`: Data manipulation and analysis
- `databricks-sdk==0.20.0`: Databricks Python SDK for warehouse connections
- `branca==0.6.0`: Folium dependency for map styling

## Troubleshooting

- **Authentication Issues**: Make sure you've run `databricks auth login` and have valid credentials
- **Connection Problems**: Verify your warehouse ID is correct and the warehouse is running
- **Data Issues**: Ensure your routes table exists and has the expected schema
- **Map Not Loading**: Check that your latitude/longitude data is valid and within expected ranges 