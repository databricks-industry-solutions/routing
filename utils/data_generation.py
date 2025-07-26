areas = [
    {"name":"Indianapolis","pop":887642,"lat":39.7684,"lon":-86.1581,"radius":0.32},
    {"name":"Carmel",       "pop":100777,"lat":39.9784,"lon":-86.1180,"radius":0.28},
    {"name":"Fishers",      "pop":95194, "lat":39.9570,"lon":-86.0134,"radius":0.28},
    {"name":"Greenwood",    "pop":65000, "lat":39.6137,"lon":-86.1067,"radius":0.24},
    {"name":"Lawrence",     "pop":49000, "lat":39.8389,"lon":-86.0253,"radius":0.20},
    {"name":"Noblesville",  "pop":69000, "lat":40.0456,"lon":-86.0086,"radius":0.20},
    {"name":"Westfield",    "pop":48000, "lat":40.0280,"lon":-86.1240,"radius":0.20},
    {"name":"Zionsville",   "pop":30000, "lat":39.9661,"lon":-86.2660,"radius":0.16},
    {"name":"Plainfield",   "pop":35000, "lat":39.7067,"lon":-86.3893,"radius":0.20},
    {"name":"Brownsburg",   "pop":27000, "lat":39.8434,"lon":-86.3380,"radius":0.16},
    {"name":"Avon",         "pop":19000, "lat":39.7606,"lon":-86.3410,"radius":0.16},
    {"name":"Speedway",     "pop":13000, "lat":39.7894,"lon":-86.2384,"radius":0.12},
    {"name":"Beech Grove",  "pop":13000, "lat":39.7598,"lon":-86.1073,"radius":0.12},
    {"name":"Southport",    "pop":2500,  "lat":39.6350,"lon":-86.1305,"radius":0.08},
    {"name":"Cumberland",   "pop":5700,  "lat":39.9145,"lon":-85.9621,"radius":0.12},
]

areas_pd = pd.DataFrame(areas)
tot_pop   = areas_pd["pop"].sum()
areas_pd["count"] = (areas_pd["pop"] * NUM_SHIPMENTS // tot_pop).astype(int)
areas_df = spark.createDataFrame(areas_pd)

def sample_nodes(lat, lon, radius_deg):
    radius_m = radius_deg * 111_000
    try:
        G = ox.graph_from_point((lat, lon), dist=radius_m, network_type="drive")
        return [(d["y"], d["x"]) for _, d in G.nodes(data=True)]
    except Exception:
        return []

shipment_schema = StructType([
    StructField("package_id", StringType()),
    StructField("city",       StringType()),
    StructField("latitude",   DoubleType()),
    StructField("longitude",  DoubleType()),
    StructField("weight", DoubleType()),
])

def build_shipments(pdf: pd.DataFrame) -> pd.DataFrame:
    area = pdf.iloc[0]            
    n    = int(area["count"])

    city = area["name"] 

    pool = sample_nodes(area.lat, area.lon, area.radius)
    if not pool:                    # fallback if OSM call fails
        pool = [(area.lat + (random.random()-0.5)*1e-3,
                 area.lon + (random.random()-0.5)*1e-3)
                for _ in range(n)]

    idx      = random.choices(range(len(pool)), k=n)
    lat_lon  = [pool[i] for i in idx]
    weights  = np.random.triangular(1, 5, 100, size=n)

    return pd.DataFrame({
        "package_id": [f"{city[:3].upper()}_{i:05d}" for i in range(n)],
        "city":       city,
        "latitude":   [xy[0] for xy in lat_lon],
        "longitude":  [xy[1] for xy in lat_lon],
        "weight": weights,
    })

# shipments_df = (
#     areas_df
#     .groupBy("name")                 # one group per city
#     .applyInPandas(build_shipments, schema=shipment_schema)
# )