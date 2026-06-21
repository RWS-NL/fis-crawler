import unittest
import pathlib
from fis.ivs.assign import (
    normalize_class_code,
    get_ship_dimensions,
    check_edge_constraints_soft,
    get_edge_weight_soft,
)


class TestRoutingEngine(unittest.TestCase):
    def test_normalize_class_code(self):
        self.assertEqual(normalize_class_code("B01"), "BO1")
        self.assertEqual(normalize_class_code("B02"), "BO2")
        self.assertEqual(normalize_class_code("b01"), "BO1")
        self.assertEqual(normalize_class_code("C3l"), "C3L")
        self.assertEqual(normalize_class_code("BII-6l"), "BII-6L")
        self.assertEqual(normalize_class_code("M8"), "M8")
        self.assertEqual(normalize_class_code(""), "")
        self.assertEqual(normalize_class_code(None), "")

    def test_get_ship_dimensions(self):
        dtv_db = {}
        ref_path = pathlib.Path("reference/DTV_shiptypes_database.json")
        if ref_path.exists():
            from fis.ivs.assign import load_shiptypes

            dtv_db = load_shiptypes(pathlib.Path("reference"))

        if not dtv_db:
            dtv_db = {
                "M8": {
                    "Beam [m]": 11.4,
                    "Length [m]": 110.0,
                    "Draught loaded [m]": 3.5,
                    "Draught empty [m]": 1.4,
                    "Height average [m]": 6.21,
                }
            }

        ship_empty = get_ship_dimensions("M8", 0, 1000, dtv_db)
        self.assertEqual(ship_empty["beam"], 11.4)
        self.assertEqual(ship_empty["length"], 110.0)
        self.assertEqual(ship_empty["height"], 6.21)
        self.assertEqual(ship_empty["draft"], 1.4)

        ship_loaded = get_ship_dimensions("M8", 1000, 1000, dtv_db)
        self.assertEqual(ship_loaded["draft"], 3.5)

        ship_half = get_ship_dimensions("M8", 500, 1000, dtv_db)
        self.assertAlmostEqual(ship_half["draft"], 1.4 + 0.5 * (3.5 - 1.4))

    def test_check_edge_constraints_soft(self):
        ship_dims = {"beam": 10.0, "length": 80.0, "height": 5.0, "draft": 2.0}

        # 1. Width constraint
        # Pass
        d_edge = {}
        struct_ok = {"chambers": [{"dim_gate_width": 12.0}], "openings": []}
        pen, viols = check_edge_constraints_soft(d_edge, struct_ok, ship_dims)
        self.assertEqual(pen["total"], 0.0)
        self.assertEqual(len(viols), 0)

        # Fail
        struct_narrow = {"chambers": [{"dim_gate_width": 8.0}], "openings": []}
        pen, viols = check_edge_constraints_soft(d_edge, struct_narrow, ship_dims)
        self.assertEqual(pen["total"], 1000.0)
        self.assertEqual(pen["lock"], 1000.0)
        self.assertEqual(viols[0]["type"], "beam")

        # 2. Length constraint
        # Fail
        struct_short_lock = {"chambers": [{"dim_usable_length": 60.0}], "openings": []}
        pen, viols = check_edge_constraints_soft(d_edge, struct_short_lock, ship_dims)
        self.assertEqual(pen["total"], 1000.0)
        self.assertEqual(pen["lock"], 1000.0)
        self.assertEqual(viols[0]["type"], "length")

        # Pass
        struct_long_lock = {"chambers": [{"dim_usable_length": 100.0}], "openings": []}
        pen, viols = check_edge_constraints_soft(d_edge, struct_long_lock, ship_dims)
        self.assertEqual(pen["total"], 0.0)

        # 3. Air draft check (Disabled/Ignored)
        struct_low = {
            "chambers": [],
            "openings": [{"type": "VST", "height_closed": 4.0}],
        }
        pen, viols = check_edge_constraints_soft(d_edge, struct_low, ship_dims)
        self.assertEqual(pen["total"], 0.0)
        self.assertEqual(len(viols), 0)

        # Movable, fits
        struct_mov = {
            "chambers": [],
            "openings": [{"type": "OPH", "height_closed": 3.0}],
        }
        pen, viols = check_edge_constraints_soft(d_edge, struct_mov, ship_dims)
        self.assertEqual(pen["total"], 0.0)

        # 4. Depth constraint with safety margin (Disabled/Ignored)
        d_depth_ok = {"mindepth_lower": -2.5}
        pen, viols = check_edge_constraints_soft(
            d_depth_ok, {"chambers": [], "openings": []}, ship_dims
        )
        self.assertEqual(pen["total"], 0.0)

        d_depth_shallow = {"mindepth_lower": -2.1}
        pen, viols = check_edge_constraints_soft(
            d_depth_shallow, {"chambers": [], "openings": []}, ship_dims
        )
        self.assertEqual(pen["total"], 0.0)
        self.assertEqual(len(viols), 0)

    def test_get_edge_weight_soft(self):
        ship_dims = {"beam": 10.0, "length": 80.0, "height": 5.0, "draft": 2.0}
        d_edge = {"length_m": 1000.0, "maxspeed_up": 10.0}
        structs = {"chambers": [], "openings": []}

        # 1 km / 10 km/h = 0.1 hours
        w = get_edge_weight_soft(d_edge, structs, ship_dims)
        self.assertAlmostEqual(w, 0.1)

        # With lock delay: passage duration 30m = 0.5 hours
        structs_lock = {"chambers": [{"passage_duration_m": 30.0}], "openings": []}
        w_lock = get_edge_weight_soft(d_edge, structs_lock, ship_dims)
        self.assertAlmostEqual(w_lock, 0.6)

        # With penalty
        structs_blocked = {"chambers": [{"dim_gate_width": 8.0}], "openings": []}
        w_blocked = get_edge_weight_soft(d_edge, structs_blocked, ship_dims)
        self.assertAlmostEqual(w_blocked, 1000.6)


if __name__ == "__main__":
    unittest.main()
