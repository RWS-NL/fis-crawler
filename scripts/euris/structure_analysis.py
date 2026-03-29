import pathlib
import geopandas as gpd
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def analyze_euris_structures(export_dir: pathlib.Path, output_dir: pathlib.Path):
    """Analyze EURIS structure files and generate validation datasets."""
    output_dir.mkdir(parents=True, exist_ok=True)

    structure_types = [
        "LockComplex",
        "LockChamber",
        "LockChamberArea",
        "BridgeArea",
        "BridgeOpening",
        "Terminal",
        "Berth",
        "BerthArea",
    ]

    summary_data = []
    all_structures_gdfs = []

    for stype in structure_types:
        files = list(export_dir.glob(f"{stype}_*.geojson"))
        logger.info(f"Found {len(files)} files for {stype}")

        for f in files:
            country = f.name.split("_")[1]
            try:
                gdf = gpd.read_file(f)
                if gdf.empty:
                    continue

                geom_types = gdf.geometry.type.value_counts().to_dict()

                summary_data.append(
                    {
                        "file": f.name,
                        "type": stype,
                        "country": country,
                        "count": len(gdf),
                        "geom_types": str(geom_types),
                    }
                )

                # Add metadata for unified validation dataset
                gdf["src_file"] = f.name
                gdf["src_type"] = stype
                gdf["src_country"] = country

                # Ensure geometry is valid and consistent
                gdf = gdf[gdf.geometry.notna()]
                all_structures_gdfs.append(
                    gdf[["geometry", "src_file", "src_type", "src_country"]]
                )

            except Exception as e:
                logger.error(f"Error reading {f.name}: {e}")

    # Save summary
    summary_df = pd.DataFrame(summary_data)
    summary_df.to_csv(output_dir / "structure_summary.csv", index=False)
    logger.info(f"Saved summary to {output_dir / 'structure_summary.csv'}")

    # Generate validation datasets for QGIS
    if all_structures_gdfs:
        # We might have mixed geometry types, so we group them by type to save to separate files
        all_structures_gdf = pd.concat(all_structures_gdfs, ignore_index=True)
        all_structures_gdf = gpd.GeoDataFrame(all_structures_gdf, crs="EPSG:4326")

        for gtype, group in all_structures_gdf.groupby(
            all_structures_gdf.geometry.type
        ):
            safe_gtype = gtype.lower()
            out_path = output_dir / f"validation_structures_{safe_gtype}.geoparquet"
            group.to_parquet(out_path)
            logger.info(f"Saved {gtype} validation dataset to {out_path}")

    # Specialized analysis for Issue #18: Point vs Polygon Locks
    locks_summary = summary_df[
        summary_df["type"].isin(["LockComplex", "LockChamber", "LockChamberArea"])
    ]
    logger.info("\nLock Geometry Summary per Country:")
    logger.info(locks_summary.to_string())


if __name__ == "__main__":
    export_dir = pathlib.Path("output/euris-export")
    output_dir = pathlib.Path("output/euris-analysis")
    analyze_euris_structures(export_dir, output_dir)
