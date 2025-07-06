import dash
from dash import dcc, html, Input, Output, State, callback
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
import json
import os
from plotting import plot_route_plotly, get_reorder_instructions
import time
import config
import math
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp

# Initialize Dash app
app = dash.Dash(__name__)

# Initialize Databricks client
try:
    w = WorkspaceClient()
    print("✅ Successfully connected to Databricks")
except Exception as e:
    print(f"❌ Failed to connect to Databricks: {e}")
    print("Make sure you've run 'databricks auth login' and have valid credentials")
    exit(1)

# App layout
app.layout = html.Div([
    # Store components for state management
    dcc.Store(id='current-route-data'),
    dcc.Store(id='reorder-state', data={'mode': 'normal'}),
    dcc.Store(id='original-route-data'),
    
    html.Div([
        # Left column - Controls
        html.Div([
            html.H1("Route Optimization Dashboard", style={'textAlign': 'center', 'marginBottom': '30px', 'fontSize': '18px'}),
            
            html.Div([
                html.Label("Select Route:", style={'fontWeight': 'bold', 'marginBottom': '10px'}),
                dcc.Dropdown(
                    id='route-dropdown',
                    placeholder="Select a route...",
                    style={'marginBottom': '20px'}
                ),
            ]),
            
            html.Div(id='route-stats', style={'marginTop': '20px'}),
            
            # Reordering controls
            html.Div([
                html.Hr(style={'margin': '20px 0'}),
                html.H3("Route Reordering", style={'fontSize': '16px', 'marginBottom': '15px'}),
                
                html.Div([
                    html.Button("Start Reordering", id='start-reorder-btn', 
                               style={'marginRight': '10px', 'marginBottom': '10px'}),
                    html.Button("Confirm Changes", id='confirm-reorder-btn', 
                               style={'marginRight': '10px', 'marginBottom': '10px', 'display': 'none'}),
                    html.Button("Re-optimize Rest", id='reoptimize-btn', 
                               style={'marginRight': '10px', 'marginBottom': '10px', 'display': 'none'}),
                    html.Button("Cancel", id='cancel-reorder-btn', 
                               style={'marginBottom': '10px', 'display': 'none'}),
                ]),
                
                html.Div(id='reorder-instructions', style={'marginTop': '15px', 'fontSize': '14px', 'color': '#666'}),
                
                html.Div(id='reorder-sequence', style={'marginTop': '15px'})
            ], id='reorder-controls'),
            
            html.Div([
                html.Label("Search Packages:", style={'fontWeight': 'bold', 'marginBottom': '10px'}),
                dcc.Dropdown(
                    id='packages-dropdown',
                    placeholder="Search packages...",
                    searchable=True,
                    multi=True,
                    style={'marginBottom': '20px'}
                ),
            ], id='packages-section', style={'display': 'none'})
        ], style={
            'width': '16.67%',  # 2/12 of the width
            'padding': '20px',
            'boxSizing': 'border-box',
            'height': '100vh',
            'overflowY': 'auto'
        }),
        
        # Right column - Map
        html.Div([
            dcc.Graph(
                id='route-map', 
                style={'height': '100%', 'width': '100%'},
                config={'scrollZoom': True}  # Enable scroll wheel zoom
            )
        ], style={
            'width': '83.33%',  # 10/12 of the width
            'height': '100vh',
            'overflow': 'hidden',
            'display': 'flex',
            'flexDirection': 'column'
        })
    ], style={
        'display': 'flex',
        'flexDirection': 'row',
        'height': '100vh',
        'margin': '0',
        'padding': '0',
        'overflow': 'hidden'
    })
], style={'margin': '0', 'padding': '0', 'height': '100vh', 'overflow': 'hidden', 'backgroundColor': '#f9f9f7'})

def execute_sql_query(query):
    """Execute SQL query against Databricks SQL warehouse"""
    try:
        response = w.statement_execution.execute_statement(
            warehouse_id=config.DATABRICKS_WAREHOUSE_ID,
            statement=query,
            wait_timeout='30s'
        )
        
        # Convert result to DataFrame
        if response.result and response.result.data_array:
            columns = [col.name for col in response.manifest.schema.columns]
            data = []
            for row in response.result.data_array:
                row_data = []
                for val in row:
                    # Handle both object format and primitive format
                    if hasattr(val, 'str_value') or hasattr(val, 'double_value') or hasattr(val, 'long_value'):
                        # Object format - extract value from attributes
                        if hasattr(val, 'str_value') and val.str_value is not None:
                            row_data.append(val.str_value)
                        elif hasattr(val, 'double_value') and val.double_value is not None:
                            row_data.append(val.double_value)
                        elif hasattr(val, 'long_value') and val.long_value is not None:
                            row_data.append(val.long_value)
                        else:
                            row_data.append(None)
                    else:
                        # Primitive format - use value directly
                        row_data.append(val)
                data.append(row_data)
            
            df = pd.DataFrame(data, columns=columns)
            return df
        else:
            return pd.DataFrame()
    except Exception as e:
        print(f"Error executing SQL query: {e}")
        print(f"Query: {query}")
        return pd.DataFrame()

def calculate_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two lat/lon points using Haversine formula"""
    R = 6371  # Earth's radius in kilometers
    
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    
    return R * c

def solve_tsp_with_ortools(coordinates, start_index=0):
    """Solve TSP using OR-Tools with lat/lon coordinates"""
    if len(coordinates) <= 2:
        return list(range(len(coordinates)))
    
    # Create distance matrix
    num_locations = len(coordinates)
    distance_matrix = []
    
    for i in range(num_locations):
        row = []
        for j in range(num_locations):
            if i == j:
                row.append(0)
            else:
                dist = calculate_distance(
                    coordinates[i][0], coordinates[i][1],
                    coordinates[j][0], coordinates[j][1]
                )
                # Scale distance to integer (multiply by 1000 for precision)
                row.append(int(dist * 1000))
        distance_matrix.append(row)
    
    # Create routing model
    manager = pywrapcp.RoutingIndexManager(num_locations, 1, start_index)
    routing = pywrapcp.RoutingModel(manager)
    
    # Create distance callback
    def distance_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return distance_matrix[from_node][to_node]
    
    transit_callback_index = routing.RegisterTransitCallback(distance_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)
    
    # Set search parameters
    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    )
    search_parameters.time_limit.seconds = 5  # 5 second time limit
    
    # Solve
    solution = routing.SolveWithParameters(search_parameters)
    
    if solution:
        # Extract route
        route = []
        index = routing.Start(0)
        while not routing.IsEnd(index):
            route.append(manager.IndexToNode(index))
            index = solution.Value(routing.NextVar(index))
        return route
    else:
        # If no solution found, return original order
        return list(range(num_locations))

@callback(
    [Output('route-dropdown', 'options'),
     Output('route-dropdown', 'value')],
    Input('route-dropdown', 'id')
)
def update_route_dropdown_callback(dropdown_id):
    """Populate dropdown with distinct routes from the table and set default value"""
    query = f"""
    SELECT DISTINCT cluster_id, truck_type 
    FROM {config.ROUTES_TABLE} 
    ORDER BY cluster_id
    """
    
    try:
        df = execute_sql_query(query)
        if not df.empty:
            options = [{'label': f"Route {cluster_id} ({truck_type})", 'value': str(cluster_id)} 
                      for cluster_id, truck_type in zip(df['cluster_id'].tolist(), df['truck_type'].tolist())]
            # Set the first route as default
            default_value = str(df['cluster_id'].iloc[0])
            return options, default_value
        else:
            return [], None
    except Exception as e:
        print(f"Error loading routes: {e}")
        return [], None

@callback(
    [Output('current-route-data', 'data'),
     Output('original-route-data', 'data'),
     Output('reorder-state', 'data'),
     Output('packages-dropdown', 'options'),
     Output('packages-section', 'style')],
    [Input('route-dropdown', 'value'),
     Input('reorder-state', 'data')],
    prevent_initial_call=True
)
def load_route_data_callback(selected_route, reorder_state):
    """Load route data when route selection changes or after confirming reorder"""
    ctx = dash.callback_context
    
    if not selected_route:
        return None, None, {'mode': 'normal'}, [], {'display': 'none'}
    
    # Check if this is a reload request after confirming changes
    if ctx.triggered and ctx.triggered[0]['prop_id'] == 'reorder-state.data':
        # Only reload if the reload_data flag is set
        if not reorder_state.get('reload_data'):
            # Return current state without reloading - preserve the reorder state
            return dash.no_update, dash.no_update, reorder_state, dash.no_update, dash.no_update
    
    query = f"""
    SELECT 
        cluster_id,
        truck_type,
        route_index,
        package_id,
        latitude,
        longitude
    FROM {config.ROUTES_TABLE} 
    WHERE cluster_id = '{selected_route}'
    ORDER BY route_index
    """
    
    try:
        df = execute_sql_query(query)
        
        if df.empty:
            return None, None, {'mode': 'normal'}, [], {'display': 'none'}
        
        # Convert numeric columns
        df['route_index'] = pd.to_numeric(df['route_index'])
        df['latitude'] = pd.to_numeric(df['latitude'])
        df['longitude'] = pd.to_numeric(df['longitude'])
        
        # Convert to dict for storage
        route_data = df.to_dict('records')
        
        # Create package options for dropdown
        package_options = [{'label': str(pkg_id), 'value': str(pkg_id)} 
                          for pkg_id in sorted(df['package_id'].astype(str).tolist())]
        
        return route_data, route_data, {'mode': 'normal'}, package_options, {'display': 'block'}
        
    except Exception as e:
        print(f"Error loading route data: {e}")
        return None, None, {'mode': 'normal'}, [], {'display': 'none'}

@callback(
    [Output('route-map', 'figure'),
     Output('route-stats', 'children'),
     Output('reorder-instructions', 'children'),
     Output('reorder-sequence', 'children'),
     Output('reorder-state', 'data', allow_duplicate=True),
     Output('reoptimize-btn', 'style', allow_duplicate=True)],
    [Input('current-route-data', 'data'),
     Input('reorder-state', 'data'),
     Input('route-map', 'clickData')],
    [State('route-dropdown', 'value')],
    prevent_initial_call=True
)
def update_route_map_callback(route_data, reorder_state, click_data, selected_route):
    """Update the map and handle click interactions"""
    if not route_data:
        return {}, "", "", "", reorder_state, {'marginRight': '10px', 'marginBottom': '10px', 'display': 'none'}
    
    # Convert back to DataFrame
    df = pd.DataFrame(route_data)
    
    # Handle click interactions during reordering
    updated_state = reorder_state.copy()
    if reorder_state.get('mode') == 'reordering' and click_data:
        # Extract clicked package info
        clicked_package = click_data['points'][0]['customdata'][0]
        
        # Get current sequence
        current_sequence = reorder_state.get('new_sequence', [])
        
        # Add clicked package to sequence if not already added
        if clicked_package not in current_sequence:
            current_sequence.append(clicked_package)
            updated_state['new_sequence'] = current_sequence
    
    # Create the plotly figure
    fig = plot_route_plotly(df, updated_state)
    
    # Create stats
    truck_type = df['truck_type'].iloc[0] if not df.empty else 'Unknown'
    stats = html.Div([
        html.P(f"Route: {selected_route}"),
        html.P(f"Truck Type: {truck_type}"),
        html.P(f"Total Stops: {len(df)}")
    ])
    
    # Create instructions
    instructions = get_reorder_instructions(updated_state)
    
    # Create sequence display
    sequence_display = ""
    if updated_state.get('mode') == 'reordering':
        sequence_items = []
        if updated_state.get('new_sequence'):
            # Show clicked packages
            sequence_items.append(html.H4("Reordered Stops:", style={'fontSize': '14px', 'marginBottom': '5px', 'color': '#2E86AB'}))
            for i, pkg_id in enumerate(updated_state['new_sequence']):
                sequence_items.append(html.Span(f"{i+1}. {pkg_id} ✓", 
                                              style={'display': 'block', 'marginBottom': '5px', 'color': '#2E86AB', 'fontWeight': 'bold'}))
            
            # Show that remaining stops will keep original order or have been optimized
            remaining_count = len(df) - len(updated_state['new_sequence'])
            if remaining_count > 0:
                if updated_state.get('optimized'):
                    sequence_items.append(html.Span(f"+ {remaining_count} more stops (optimized)", 
                                                  style={'display': 'block', 'marginTop': '10px', 'color': '#28a745', 'fontStyle': 'italic', 'fontWeight': 'bold'}))
                else:
                    sequence_items.append(html.Span(f"+ {remaining_count} more stops in original order", 
                                                  style={'display': 'block', 'marginTop': '10px', 'color': '#666', 'fontStyle': 'italic'}))
        else:
            sequence_items.append(html.Span("Click stops to reorder them", style={'color': '#666', 'fontStyle': 'italic'}))
        
        sequence_display = html.Div(sequence_items)
    
    # Determine re-optimize button visibility
    reoptimize_style = {'marginRight': '10px', 'marginBottom': '10px', 'display': 'none'}
    if (updated_state.get('mode') == 'reordering' and 
        updated_state.get('new_sequence') and 
        len(updated_state['new_sequence']) > 0 and
        len(df) - len(updated_state['new_sequence']) > 1 and
        not updated_state.get('optimized')):
        reoptimize_style = {'marginRight': '10px', 'marginBottom': '10px', 'display': 'inline-block'}
    
    return fig, stats, instructions, sequence_display, updated_state, reoptimize_style

@callback(
    [Output('reorder-state', 'data', allow_duplicate=True),
     Output('start-reorder-btn', 'style'),
     Output('confirm-reorder-btn', 'style'),
     Output('reoptimize-btn', 'style'),
     Output('cancel-reorder-btn', 'style')],
    [Input('start-reorder-btn', 'n_clicks'),
     Input('confirm-reorder-btn', 'n_clicks'),
     Input('reoptimize-btn', 'n_clicks'),
     Input('cancel-reorder-btn', 'n_clicks')],
    [State('reorder-state', 'data'),
     State('current-route-data', 'data'),
     State('original-route-data', 'data'),
     State('route-dropdown', 'value')],
    prevent_initial_call=True
)
def handle_reorder_buttons_callback(start_clicks, confirm_clicks, reoptimize_clicks, cancel_clicks, 
                                  reorder_state, current_route_data, original_route_data, selected_route):
    """Handle reordering button clicks"""
    ctx = dash.callback_context
    if not ctx.triggered:
        return reorder_state, {'marginRight': '10px', 'marginBottom': '10px'}, {'marginRight': '10px', 'marginBottom': '10px', 'display': 'none'}, {'marginRight': '10px', 'marginBottom': '10px', 'display': 'none'}, {'marginBottom': '10px', 'display': 'none'}
    
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    if button_id == 'start-reorder-btn':
        # Start reordering mode
        new_state = {
            'mode': 'reordering',
            'new_sequence': [],
            'total_stops': len(current_route_data) if current_route_data else 0
        }
        return (new_state, 
                {'marginRight': '10px', 'marginBottom': '10px', 'display': 'none'},
                {'marginRight': '10px', 'marginBottom': '10px', 'display': 'inline-block'},
                {'marginRight': '10px', 'marginBottom': '10px', 'display': 'none'},
                {'marginBottom': '10px', 'display': 'inline-block'})
    
    elif button_id == 'reoptimize-btn':
        # Re-optimize remaining stops using OR-Tools
        if reorder_state.get('new_sequence') and current_route_data:
            selected_packages = set(reorder_state['new_sequence'])
            
            # Get remaining unselected packages with their coordinates
            remaining_packages = []
            remaining_coords = []
            
            for item in current_route_data:
                if item['package_id'] not in selected_packages:
                    remaining_packages.append(item['package_id'])
                    remaining_coords.append((item['latitude'], item['longitude']))
            
            if len(remaining_packages) > 1:
                # Solve TSP for remaining packages
                optimal_indices = solve_tsp_with_ortools(remaining_coords)
                optimized_packages = [remaining_packages[i] for i in optimal_indices]
                
                # Update state with optimized sequence
                new_sequence = reorder_state['new_sequence'] + optimized_packages
                updated_state = reorder_state.copy()
                updated_state['new_sequence'] = new_sequence
                updated_state['optimized'] = True
                
                return (updated_state,
                        {'marginRight': '10px', 'marginBottom': '10px', 'display': 'none'},
                        {'marginRight': '10px', 'marginBottom': '10px', 'display': 'inline-block'},
                        {'marginRight': '10px', 'marginBottom': '10px', 'display': 'none'},
                        {'marginBottom': '10px', 'display': 'inline-block'})
        
        # If no optimization needed, return current state
        return (reorder_state,
                {'marginRight': '10px', 'marginBottom': '10px', 'display': 'none'},
                {'marginRight': '10px', 'marginBottom': '10px', 'display': 'inline-block'},
                {'marginRight': '10px', 'marginBottom': '10px', 'display': 'none'},
                {'marginBottom': '10px', 'display': 'inline-block'})
    
    elif button_id == 'confirm-reorder-btn':
        # Confirm changes and update database
        if reorder_state.get('new_sequence'):
            # Create full sequence with partial reordering
            new_sequence = reorder_state['new_sequence']
            
            # Get all packages and create the full reordered sequence
            if current_route_data:
                all_packages = [item['package_id'] for item in current_route_data]
                clicked_packages = set(new_sequence)
                unclicked_packages = [pkg for pkg in all_packages if pkg not in clicked_packages]
                
                # Sort unclicked packages by their original route_index
                unclicked_with_order = [(pkg, next(item['route_index'] for item in current_route_data if item['package_id'] == pkg)) 
                                      for pkg in unclicked_packages]
                unclicked_with_order.sort(key=lambda x: x[1])
                unclicked_packages = [pkg for pkg, _ in unclicked_with_order]
                
                # Create full sequence: clicked first, then unclicked
                full_sequence = new_sequence + unclicked_packages
                
                # Update database with new order
                update_route_order_in_db(selected_route, full_sequence)
        
        # Return to normal mode
        new_state = {'mode': 'normal', 'reload_data': True}
        return (new_state,
                {'marginRight': '10px', 'marginBottom': '10px'},
                {'marginRight': '10px', 'marginBottom': '10px', 'display': 'none'},
                {'marginRight': '10px', 'marginBottom': '10px', 'display': 'none'},
                {'marginBottom': '10px', 'display': 'none'})
    
    elif button_id == 'cancel-reorder-btn':
        # Cancel reordering and return to normal mode
        new_state = {'mode': 'normal'}
        return (new_state,
                {'marginRight': '10px', 'marginBottom': '10px'},
                {'marginRight': '10px', 'marginBottom': '10px', 'display': 'none'},
                {'marginRight': '10px', 'marginBottom': '10px', 'display': 'none'},
                {'marginBottom': '10px', 'display': 'none'})
    
    return reorder_state, {'marginRight': '10px', 'marginBottom': '10px'}, {'marginRight': '10px', 'marginBottom': '10px', 'display': 'none'}, {'marginRight': '10px', 'marginBottom': '10px', 'display': 'none'}, {'marginBottom': '10px', 'display': 'none'}

def update_route_order_in_db(cluster_id, new_sequence):
    """Update route order in database"""
    try:
        # Create update statements for each package
        case_statements = []
        for new_index, package_id in enumerate(new_sequence, 1):
            case_statements.append(f"WHEN '{package_id}' THEN {new_index}")
        
        case_clause = " ".join(case_statements)
        
        update_query = f"""
        UPDATE {config.ROUTES_TABLE} 
        SET route_index = CASE package_id
            {case_clause}
        END
        WHERE cluster_id = '{cluster_id}'
        """
        
        execute_sql_query(update_query)
        print(f"✅ Successfully updated route order for cluster {cluster_id}")
        
    except Exception as e:
        print(f"❌ Error updating route order: {e}")

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8050) 