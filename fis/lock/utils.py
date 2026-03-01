from shapely.ops import transform
from shapely.geometry import Point
import pyproj


def project_geometry(geometry, crs_from="EPSG:4326", crs_to="EPSG:28992"):
    """
    Project a shapely geometry from one CRS to another.
    Default is WGS84 (4326) to Amersfoort / RD New (28992).
    """
    if not geometry:
        return None

    project = pyproj.Transformer.from_crs(crs_from, crs_to, always_xy=True).transform
    return transform(project, geometry)


def find_chamber_doors(chamber_geom, split_point, merge_point):
    """
    Find the entrance and exit points (doors) of a chamber.

    Uses the Minimum Rotated Rectangle (MRR) of the chamber to find its centerline
    axis that aligns with the flow direction. The intersection of this centerline
    with the chamber boundary defines the door locations.
    """
    if not chamber_geom or not split_point or not merge_point:
        return None, None

    # Project inputs to Meters for robust MRR calculation
    c_geom_rd = project_geometry(chamber_geom, "EPSG:4326", "EPSG:28992")
    split_rd = project_geometry(split_point, "EPSG:4326", "EPSG:28992")
    merge_rd = project_geometry(merge_point, "EPSG:4326", "EPSG:28992")

    if not c_geom_rd or not split_rd or not merge_rd:
        return None, None

    # Flow vector (Split -> Merge)
    vx = merge_rd.x - split_rd.x
    vy = merge_rd.y - split_rd.y

    # Get Centerline Axis from MRR
    from shapely.geometry import LineString

    # MRR is a Polygon (rectangle)
    mrr = c_geom_rd.minimum_rotated_rectangle

    if mrr.geom_type != "Polygon":
        # Fallback for lines/points
        return None, None

    # Get edge midpoints
    coords = list(mrr.exterior.coords)  # 5 points (first=last)
    # Segments: 0-1, 1-2, 2-3, 3-0
    midpoints = []
    for i in range(4):
        p1 = coords[i]
        p2 = coords[i + 1]
        midpoints.append(((p1[0] + p2[0]) / 2, (p1[1] + p2[1]) / 2))

    # Two possible axes:
    # Axis A: Midpoint 0 -> Midpoint 2
    # Axis B: Midpoint 1 -> Midpoint 3
    axis_a = (midpoints[0], midpoints[2])
    axis_b = (midpoints[1], midpoints[3])

    def get_vec(p1, p2):
        return p2[0] - p1[0], p2[1] - p1[1]

    va = get_vec(*axis_a)
    vb = get_vec(*axis_b)

    # Normalize and dot product with flow vector to find parallel axis
    def dot_normalized(v_axis, v_flow):
        len_a = (v_axis[0] ** 2 + v_axis[1] ** 2) ** 0.5
        len_f = (v_flow[0] ** 2 + v_flow[1] ** 2) ** 0.5
        if len_a == 0 or len_f == 0:
            return 0
        return abs((v_axis[0] * v_flow[0] + v_axis[1] * v_flow[1]) / (len_a * len_f))

    score_a = dot_normalized(va, (vx, vy))
    score_b = dot_normalized(vb, (vx, vy))

    best_axis = axis_a if score_a > score_b else axis_b

    # Create LineString centerline and intersect with chamber boundary
    centerline = LineString(best_axis)

    # Intersection with boundary
    boundary = c_geom_rd.boundary
    intersection = boundary.intersection(centerline)

    # Result might be MultiPoint or Point
    candidates = []
    if intersection.geom_type == "Point":
        candidates = [intersection]
    elif intersection.geom_type == "MultiPoint":
        candidates = list(intersection.geoms)
    elif intersection.geom_type == "LineString":
        candidates = [Point(intersection.coords[0]), Point(intersection.coords[-1])]

    if not candidates:
        # Fallback: project the MRR midpoints onto the boundary
        p1 = Point(best_axis[0])
        p2 = Point(best_axis[1])
        from shapely.ops import nearest_points

        candidates = [nearest_points(boundary, p1)[0], nearest_points(boundary, p2)[0]]

    # Sort candidates by distance along flow
    # Projected position on flow vector: t = (P - Split) . V
    scored = []
    for pt in candidates:
        dx = pt.x - split_rd.x
        dy = pt.y - split_rd.y
        t = dx * vx + dy * vy
        scored.append((pt, t))

    scored.sort(key=lambda x: x[1])

    start_rd = scored[0][0]
    end_rd = scored[-1][0]

    # Project back to WGS84
    door_start = project_geometry(start_rd, "EPSG:28992", "EPSG:4326")
    door_end = project_geometry(end_rd, "EPSG:28992", "EPSG:4326")

    return door_start, door_end
