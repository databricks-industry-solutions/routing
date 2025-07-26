import folium
import branca.colormap as cm
import plotly.graph_objects as go
import plotly.express as px


def plot_route_folium(route_pdf):
    # Sort by route_index to get the visiting order
    df = route_pdf.sort_values('route_index').reset_index(drop=True)

    # Set up a color gradient (e.g., blue -> red)
    colormap = cm.LinearColormap(['blue', 'lime', 'yellow', 'red'], 
                                 vmin=df['route_index'].min(), 
                                 vmax=df['route_index'].max())

    # Create a folium map centered on the mean location
    m = folium.Map(
        location=[df['latitude'].mean(), df['longitude'].mean()],
        zoom_start=11,
        tiles='cartodbpositron'
    )

    # Add points with color by route_index
    for _, row in df.iterrows():
        folium.CircleMarker(
            location=[row['latitude'], row['longitude']],
            radius=5,
            color=colormap(row['route_index']),
            fill=True,
            fill_color=colormap(row['route_index']),
            fill_opacity=0.9,
            popup=f"Order: {row['route_index']}<br>Package: {row['package_id']}"
        ).add_to(m)

    # Draw the route as a line colored by order
    coordinates = df[['latitude', 'longitude']].values.tolist()
    folium.PolyLine(
        locations=coordinates,
        color='black',
        weight=2,
        opacity=0.5
    ).add_to(m)

    # Add the color legend
    colormap.caption = 'Route Order'
    colormap.add_to(m)
    return m


def plot_route_plotly(route_pdf, reorder_state=None):
    """
    Create an interactive plotly mapbox route visualization
    
    Args:
        route_pdf: DataFrame with route data
        reorder_state: Dict with reordering state information
    """
    # Sort by route_index to get the visiting order
    df = route_pdf.sort_values('route_index').reset_index(drop=True).copy()
    
    # If we're in reorder mode and have a new sequence, update the display order
    if reorder_state and reorder_state.get('mode') == 'reordering' and reorder_state.get('new_sequence'):
        new_sequence = reorder_state['new_sequence']
        
        # Create partial reordering: clicked points first, then unclicked points in original order
        clicked_packages = set(new_sequence)
        unclicked_packages = [pkg for pkg in df['package_id'].tolist() if pkg not in clicked_packages]
        
        # Sort unclicked packages by their original route_index
        unclicked_df = df[df['package_id'].isin(unclicked_packages)].sort_values('route_index')
        unclicked_packages = unclicked_df['package_id'].tolist()
        
        # Create the full sequence: clicked first, then unclicked
        full_sequence = new_sequence + unclicked_packages
        
        # Update display order
        for i, package_id in enumerate(full_sequence):
            df.loc[df['package_id'] == package_id, 'display_order'] = i + 1
    else:
        df['display_order'] = df['route_index']
    
    # Create color scale based on route order
    df['color'] = df['display_order']
    
    # Determine marker styling based on reorder state
    if reorder_state and reorder_state.get('mode') == 'reordering':
        # In reordering mode - highlight clickable markers
        marker_size = 15
        marker_opacity = 0.8
    else:
        # Normal mode
        marker_size = 12
        marker_opacity = 0.9
    
    # Create the scatter plot for route points
    fig = go.Figure()
    
    # Add route line first (so it appears under markers)
    if len(df) > 1:
        # Sort by display_order for line drawing
        line_df = df.sort_values('display_order')
        fig.add_trace(go.Scattermapbox(
            lat=line_df['latitude'],
            lon=line_df['longitude'],
            mode='lines',
            line=dict(width=3, color='rgba(0,0,0,0.6)'),
            name='Route',
            hoverinfo='skip',
            showlegend=False
        ))
    
    # Add markers for each stop
    fig.add_trace(go.Scattermapbox(
        lat=df['latitude'],
        lon=df['longitude'],
        mode='markers+text',
        marker=dict(
            size=marker_size,
            color=df['color'],
            colorscale='Viridis',
            opacity=marker_opacity,
            showscale=True,
            colorbar=dict(title="Stop Order")
        ),
        text=df['display_order'].astype(str),
        textposition="middle center",
        textfont=dict(size=10, color='white'),
        customdata=df[['package_id', 'route_index', 'display_order']].values,
        hovertemplate='<b>Package %{customdata[0]}</b><br>' +
                      'Current Order: %{customdata[2]}<br>' +
                      'Original Order: %{customdata[1]}<br>' +
                      'Lat: %{lat}<br>' +
                      'Lon: %{lon}<extra></extra>',
        name='Stops'
    ))
    
    # Configure the map layout
    fig.update_layout(
        mapbox=dict(
            style="open-street-map",
            center=dict(lat=df['latitude'].mean(), lon=df['longitude'].mean()),
            zoom=11
        ),
        showlegend=False,
        height=None,  # Let the Graph component control the height
        margin=dict(l=0, r=0, t=0, b=0),
        uirevision=True,  # Preserve user interactions like zoom/pan
        autosize=True  # Allow the figure to resize automatically
    )
    
    return fig


def get_reorder_instructions(reorder_state):
    """Generate instruction text for reordering mode"""
    if not reorder_state or reorder_state.get('mode') != 'reordering':
        return ""
    
    current_step = len(reorder_state.get('new_sequence', []))
    total_stops = reorder_state.get('total_stops', 0)
    remaining_stops = total_stops - current_step
    
    if current_step == 0:
        return f"Click stops in your desired NEW order. Unclicked stops will maintain their original order after the clicked ones."
    elif remaining_stops > 1 and not reorder_state.get('optimized'):
        return f"Selected {current_step} stops. Click more stops to reorder them, use 'Re-optimize Rest' to automatically optimize the remaining {remaining_stops} stops, or 'Confirm Changes' to save."
    elif reorder_state.get('optimized'):
        return f"Selected {current_step} stops and optimized {remaining_stops} remaining stops. Click 'Confirm Changes' to save the optimized route."
    else:
        return f"Selected {current_step} stops. Click more stops to reorder them, or 'Confirm Changes' to save (unclicked stops keep original order)."