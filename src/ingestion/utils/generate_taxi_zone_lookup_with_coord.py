"""
    Ingest the taxi zone lookup table with coordinates from the NYC TLC.
    The lookup table is stored in the bronze.taxi_zone_lookup table.
    The shapefile is stored in the general-data/taxi_zones/taxi_zones.shp file.
    The shapefile is a shapefile of the taxi zones in the NYC TLC and we can extract the coordinates from the shapefile.
    The csv https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv only includes a mapping of the location id to the zone name.
    We need to join the lookup table with the shapefile to get the coordinates.
    This includes manual process of downloading the shapefile and storing it in the general-data/taxi_zones/taxi_zones.shp file.

    The csv file is then copy pated into the transformation/taxi/seeds/taxi_zone_lookup.csv file.
    Not production ready code.
"""

import pandas as pd
import geopandas as gpd

# 1. Taxi zone lookup table
lookup_url = (
    "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
)
lookup = pd.read_csv(lookup_url)

# 2. Taxi zone shapefile
# Download the official Taxi Zone shapefile ZIP from NYC TLC
shapefile_zip = "taxi_zones.zip"

gdf = gpd.read_file('/Users/fmohammadmahditardasti/Documents/exercise/schwarz/general-data/taxi_zones/taxi_zones.shp')

# Typical columns:
# LocationID, zone, borough, geometry

# 3. Convert to WGS84 (lat/lon)
gdf = gdf.to_crs(epsg=4326)

# 4. Compute centroids
gdf["longitude"] = gdf.geometry.centroid.x
gdf["latitude"] = gdf.geometry.centroid.y

coords = gdf[["LocationID", "latitude", "longitude"]]

# 5. Join with lookup table
result = lookup.merge(coords, on="LocationID", how="left")

# 6. Save
result.to_csv("taxi_zone_lookup_with_coordinates.csv", index=False)

print(result.head())