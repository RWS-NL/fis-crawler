import os
import sys
from qgis.core import (
    QgsProject,
    QgsApplication,
    QgsPrintLayout,
    QgsLayoutItemMap,
    QgsLayoutItemPage,
    QgsLayoutPoint,
    QgsLayoutSize,
    QgsUnitTypes,
    QgsLayoutExporter,
)


def main():
    project_path = os.path.abspath("qgis/diagnostics.qgz")
    print(f"Reading project from: {project_path}")
    project = QgsProject.instance()
    if not project.read(project_path):
        print(f"Error: Could not read project {project_path}")
        sys.exit(1)

    # Check for boundaries layer
    boundaries_layer = None
    for layer in project.mapLayers().values():
        if "boundaries" in layer.name().lower():
            boundaries_layer = layer
            break

    if not boundaries_layer:
        print("Error: Could not find a layer containing 'boundaries' in the project.")
        sys.exit(1)

    print(f"Found boundaries layer: {boundaries_layer.name()}")

    layout_manager = project.layoutManager()
    layouts = layout_manager.printLayouts()

    layout = None
    # Look for layout with atlas enabled
    for lay in layouts:
        if lay.atlas().enabled():
            layout = lay
            break

    if not layout:
        if layouts:
            layout = layouts[0]
            print(f"Using existing layout: {layout.name()}")
        else:
            print(
                "No layouts found. Creating a temporary print layout programmatically..."
            )
            layout = QgsPrintLayout(project)
            layout.initializeDefaults()
            layout.setName("Programmatic Atlas Layout")
            layout_manager.addLayout(layout)

            # Set page orientation to Landscape A4 (297x210 mm)
            page = layout.pageCollection().pages()[0]
            page.setPageSize("A4", QgsLayoutItemPage.Landscape)

            # Add a map item that covers most of the page
            # Leave 10mm margins: width=277, height=190
            map_item = QgsLayoutItemMap(layout)
            map_item.attemptMove(QgsLayoutPoint(10, 10, QgsUnitTypes.LayoutMillimeters))
            map_item.attemptResize(
                QgsLayoutSize(277, 190, QgsUnitTypes.LayoutMillimeters)
            )
            map_item.setFrameEnabled(True)
            layout.addLayoutItem(map_item)

    # Find the map item and apply Atlas driving + clipping settings
    map_item = None
    for item in layout.items():
        if isinstance(item, QgsLayoutItemMap):
            map_item = item
            break

    if map_item:
        print(
            "Configuring layout map item: Atlas driven, 0% margin, and clipping enabled."
        )
        map_item.setAtlasDriven(True)
        map_item.setAtlasScalingMode(QgsLayoutItemMap.Auto)
        map_item.setAtlasMargin(0.0)  # 0 margin for sharp cut at boundaries

        clipping = map_item.atlasClippingSettings()
        clipping.setEnabled(False)

    atlas = layout.atlas()
    atlas.setCoverageLayer(boundaries_layer)
    atlas.setEnabled(True)

    output_dir = os.path.abspath("output/lock-diagnostics/qgis_atlas")
    os.makedirs(output_dir, exist_ok=True)
    print(f"Exporting atlas layouts to: {output_dir}")

    if not atlas.beginRender():
        print("Error: Could not begin Atlas render.")
        sys.exit(1)

    for i in range(atlas.count()):
        atlas.seekTo(i)
        feature = layout.reportContext().feature()

        name = None
        for field in feature.fields():
            if field.name().lower() in ["complex_name", "name"]:
                name = feature.attribute(field.name())
                break

        if not name:
            name = f"complex_{i}"

        filename = f"{str(name).lower().replace(' ', '_')}.png"
        filepath = os.path.join(output_dir, filename)

        exporter = QgsLayoutExporter(layout)
        settings = QgsLayoutExporter.ImageExportSettings()
        settings.dpi = 150

        print(f"Rendering atlas feature {i + 1}/{atlas.count()}: {name} -> {filename}")
        result = exporter.exportToImage(filepath, settings)
        if result == QgsLayoutExporter.Success:
            print(f"  Successfully exported {filepath}")
        else:
            print(f"  Failed to export {name} (Error code: {result})")

    atlas.endRender()
    print("QGIS Atlas export completed successfully!")
    try:
        QgsApplication.exitQgis()
    except RuntimeError:
        pass
    sys.exit(0)


if __name__ == "__main__":
    main()
