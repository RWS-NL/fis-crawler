import logging
from dataclasses import dataclass
from typing import List, Optional

from shapely.geometry import LineString, Point
from shapely.ops import substring

logger = logging.getLogger(__name__)


@dataclass
class SplicedSegment:
    """
    Represents a geometric segment of a fairway sliced by obstacles.
    A segment can be bounded by 0, 1, or 2 obstacles depending on its position:
    - 0 obstacles: Splicer ran with an empty obstacle list.
    - 1 obstacle: The segment is at the very beginning or very end of the fairway.
    - 2 obstacles: The segment lies between two consecutive obstacles (e.g., Lock A then Lock B).

    `source` is the obstacle it just left, `target` is the obstacle it is approaching.
    """

    geometry: LineString
    # Distances are in the relative coordinate units of the CRS (e.g. degrees if 4326, meters if 28992)
    start_distance: float
    end_distance: float
    source_obstacle_id: Optional[str] = None
    target_obstacle_id: Optional[str] = None


@dataclass
class ObstacleCut:
    """Represents an obstacle projected onto a LineString."""

    id: str
    geometry: Point
    # The projected coordinate distance along the LineString (units of the CRS, not strictly geodetic meters)
    projected_distance: float
    # The coordinate buffer to slice out around the obstacle (units of the CRS)
    buffer_distance: float = 0.0


class FairwaySplicer:
    """
    Pure geometric utility to slice a LineString fairway into segments
    given a list of obstacles.
    """

    def __init__(self, line: LineString):
        """
        Args:
            line: The canonical fairway LineString to slice. Must be an EPSG:28992
                  or a projected CRS where distances are meaningful.
        """
        if not isinstance(line, LineString):
            raise ValueError("FairwaySplicer requires a valid LineString.")
        self.line = line
        # Note: line.length is in the units of the LineString's CRS.
        # If the input is EPSG:4326, this is in degrees. If EPSG:28992, it is in meters.
        # Splicer operates in relative distance of the provided CRS.
        self.total_length = line.length

    def splice(self, obstacles: List[ObstacleCut]) -> List[SplicedSegment]:
        """
        Splice the fairway line given an unordered list of obstacles.

        Args:
            obstacles: List of ObstacleCuts containing their projected distance along the line.

        Returns:
            A consecutive sequence of `SplicedSegment`s from start to end of the fairway.
        """
        segments = []

        if not obstacles:
            segments.append(
                SplicedSegment(
                    geometry=self.line,
                    start_distance=0.0,
                    end_distance=self.total_length,
                )
            )
            return segments

        # 1. Sort obstacles purely by projected sequential distance along the line
        sorted_obstacles = sorted(obstacles, key=lambda o: o.projected_distance)

        # 2. Slice iteratively
        current_distance = 0.0
        current_source_id = None

        EPSILON = 0.001

        for obs in sorted_obstacles:
            split_distance = max(0.0, obs.projected_distance - obs.buffer_distance)
            merge_distance = min(
                self.total_length, obs.projected_distance + obs.buffer_distance
            )

            # Ensure we don't go backwards or overlap negatively due to large buffers on close obstacles
            # We strictly require split_distance > current_distance to create a valid connecting line segment
            split_distance = max(current_distance + EPSILON, split_distance)

            # Prevent going out of bounds
            if split_distance >= self.total_length:
                split_distance = max(
                    current_distance + EPSILON, self.total_length - (EPSILON * 2)
                )

            merge_distance = max(split_distance + EPSILON, merge_distance)
            if merge_distance >= self.total_length:
                merge_distance = max(
                    split_distance + EPSILON, self.total_length - EPSILON
                )

            start_distance = current_distance
            source_id = current_source_id

            current_distance = merge_distance
            current_source_id = obs.id

            seg_line = substring(self.line, start_distance, split_distance)

            segments.append(
                SplicedSegment(
                    geometry=seg_line,
                    start_distance=start_distance,
                    end_distance=split_distance,
                    source_obstacle_id=source_id,
                    target_obstacle_id=obs.id,
                )
            )

        # 3. Final trailing segment after the last obstacle
        if current_distance < self.total_length:
            seg_line = substring(self.line, current_distance, self.total_length)
            segments.append(
                SplicedSegment(
                    geometry=seg_line,
                    start_distance=current_distance,
                    end_distance=self.total_length,
                    source_obstacle_id=current_source_id,
                    target_obstacle_id=None,
                )
            )

        return segments
