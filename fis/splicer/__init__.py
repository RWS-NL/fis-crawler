from dataclasses import dataclass
from typing import List, Optional
import logging
from shapely.geometry import LineString, Point
from shapely.ops import substring

logger = logging.getLogger(__name__)


@dataclass
class SplicedSegment:
    """
    A single segment of a sliced fairway.
    """

    geometry: LineString
    source_structure_id: Optional[str] = None
    target_structure_id: Optional[str] = None


@dataclass
class StructureCut:
    """Represents a structure projected onto a LineString."""

    id: str
    geometry: Point
    # Distance from the start of the line (in units of the CRS)
    projected_distance: float
    # The coordinate buffer to slice out around the structure (units of the CRS)
    buffer_distance: float


class FairwaySplicer:
    """
    Slices a LineString into multiple segments based on a set of structures.
    """

    def __init__(self, line: LineString):
        self.line = line
        self.total_length = line.length

    def splice(self, structures: List[StructureCut]) -> List[SplicedSegment]:
        """
        Splice the fairway line given an unordered list of structures.
        """
        if not structures:
            return [
                SplicedSegment(
                    geometry=self.line,
                    source_structure_id=None,
                    target_structure_id=None,
                )
            ]

        # 1. Sort structures purely by projected sequential distance along the line
        sorted_structures = sorted(structures, key=lambda o: o.projected_distance)

        segments = []
        current_distance = 0.0
        current_source_id = None

        # Minimal gap to avoid zero-length segments or precision errors
        EPSILON = 0.001

        for struct in sorted_structures:
            split_distance = max(
                0.0, struct.projected_distance - struct.buffer_distance
            )
            merge_distance = min(
                self.total_length, struct.projected_distance + struct.buffer_distance
            )

            # Ensure we don't go backwards or overlap negatively
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
            current_source_id = struct.id

            if split_distance > start_distance + EPSILON:
                geom = substring(self.line, start_distance, split_distance)
                if not geom.is_empty:
                    segments.append(
                        SplicedSegment(
                            geometry=geom,
                            source_structure_id=source_id,
                            target_structure_id=struct.id,
                        )
                    )
            else:
                logger.debug(
                    f"Skipping segment between {source_id} and {struct.id} due to overlap or precision limit."
                )

        # 3. Final trailing segment after the last structure
        if current_distance < self.total_length:
            geom = substring(self.line, current_distance, self.total_length)
            if not geom.is_empty:
                segments.append(
                    SplicedSegment(
                        geometry=geom,
                        source_structure_id=current_source_id,
                        target_structure_id=None,
                    )
                )

        return segments
