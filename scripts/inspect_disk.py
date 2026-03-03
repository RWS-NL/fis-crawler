import geopandas as gpd


def main():
    url = "https://geo.rijkswaterstaat.nl/services/ogc/gdr/disk_beheerobjecten/ows?service=WFS&version=2.0.0&request=GetFeature&typeName=disk_beheerobjecten:schutsluis&outputFormat=application/json"
    gdf = gpd.read_file(url)
    print("Columns:")
    print(gdf.columns)
    print("\nSample Data:")
    print(gdf.head(2))


if __name__ == "__main__":
    main()
