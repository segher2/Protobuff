import duckdb
import geopandas as gpd

N_BUILDINGS = 100_000
OUTPUT_FILE = f"examples/data/overture_buildings_{N_BUILDINGS}.geojson"

con = duckdb.connect()

con.execute("INSTALL spatial;")
con.execute("LOAD spatial;")

con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")
con.execute("SET s3_region='us-west-2';")

OVERTURE_S3_PATH = (
    "s3://overturemaps-us-west-2/"
    "release/*/theme=buildings/type=building/*.parquet"
)

df = con.execute(f"""
SELECT
    id,
    ST_AsWKB(geometry) AS geometry
FROM read_parquet('{OVERTURE_S3_PATH}')
LIMIT {N_BUILDINGS};
""").fetch_df()

gdf = gpd.GeoDataFrame(
    df,
    geometry=gpd.GeoSeries.from_wkb(df.geometry.apply(bytes)),
    crs="EPSG:4326"
)

gdf.to_file(OUTPUT_FILE, driver="GeoJSON")
print(f"Saved {len(gdf)} buildings to {OUTPUT_FILE}")
