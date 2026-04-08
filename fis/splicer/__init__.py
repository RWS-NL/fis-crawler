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
    buffer_before: float
    buffer_after: float


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
        logger.debug(
            "Splicing line of length %.1f with %d structures: %s",
            self.total_length,
            len(sorted_structures),
            [s.id for s in sorted_structures],
        )

        segments = []
        current_distance = 0.0
        current_source_id = None

        # Minimal gap to avoid zero-length segments or precision errors
        EPSILON = 0.001

        for struct in sorted_structures:
            logger.debug(
                "  Processing structure %s at %.1f (buffer: %.1f/%.1f)",
                struct.id,
                struct.projected_distance,
                struct.buffer_before,
                struct.buffer_after,
            )
            split_distance = max(0.0, struct.projected_distance - struct.buffer_before)
            merge_distance = min(
                self.total_length, struct.projected_distance + struct.buffer_after
            )

            # Ensure we don't go backwards or overlap negatively
            split_distance = max(current_distance, split_distance)
            merge_distance = max(split_distance, merge_distance)

            start_distance = current_distance
            source_id = current_source_id

            current_distance = merge_distance
            current_source_id = struct.id

            if split_distance > start_distance + EPSILON:
                geom = substring(self.line, start_distance, split_distance)
                if not geom.is_empty:
                    logger.debug(
                        "    Added segment: %.1f -> %.1f (%s -> %s)",
                        start_distance,
                        split_distance,
                        source_id,
                        struct.id,
                    )
                    segments.append(
                        SplicedSegment(
                            geometry=geom,
                            source_structure_id=source_id,
                            target_structure_id=struct.id,
                        )
                    )
            else:
                # Segment is too small for geometry, but we still record the topological connection
                logger.debug(
                    "    Added zero-geom segment (%s -> %s)", source_id, struct.id
                )
                segments.append(
                    SplicedSegment(
                        geometry=None,
                        source_structure_id=source_id,
                        target_structure_id=struct.id,
                    )
                )

        # 3. Final trailing segment after the last structure
        if current_distance < self.total_length:
            geom = substring(self.line, current_distance, self.total_length)
            if not geom.is_empty:
                logger.debug(
                    "    Added trailing segment: %.1f -> %.1f (%s -> None)",
                    current_distance,
                    self.total_length,
                    current_source_id,
                )
                segments.append(
                    SplicedSegment(
                        geometry=geom,
                        source_structure_id=current_source_id,
                        target_structure_id=None,
                    )
                )

        return segments
