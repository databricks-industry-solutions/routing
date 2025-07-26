def plot_route_folium(solution_df):
    import folium
    import branca.colormap as cm

    # Sort by route_index to get the visiting order
    df = solution_df.sort_values('route_index').reset_index(drop=True)

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