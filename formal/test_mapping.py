"""Offline consistency evidence, not an implementation-refinement proof."""
import hashlib
import json
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[1]
FORMAL = ROOT / "formal"


def load(name):
    return json.loads((FORMAL / name).read_text())


class Mapping(unittest.TestCase):
    def test_current_inventory_and_all_pins_agree(self):
        rows = load("inventory.json")["paths"]
        inventory = {row["path"]: row for row in rows}
        self.assertEqual(len(rows), len(inventory))
        pins = load("implementation-hashes.json")
        self.assertLessEqual(pins.keys(), inventory.keys())
        for path, row in inventory.items():
            with self.subTest(path=path):
                data = (ROOT / path).read_bytes()
                self.assertEqual(hashlib.sha256(data).hexdigest(), row["sha256"])
                if path in pins:
                    self.assertEqual(pins[path], row["sha256"])
                lines = data.decode().splitlines() if row["declarations"] else []
                for declaration in row["declarations"]:
                    self.assertEqual(
                        lines[declaration["line"] - 1].strip(), declaration["text"]
                    )

    def test_every_action_anchor_binds_current_source(self):
        pins = load("implementation-hashes.json")
        families = load("source-map.json")["families"]
        self.assertEqual(set(families), {p.stem for p in FORMAL.glob("*.tla")})
        for family in families.values():
            for anchors in family["actions"].values():
                self.assertTrue(anchors)
                for anchor in anchors:
                    self.assertEqual(pins[anchor["path"]], anchor["sha256"])
                    lines = (ROOT / anchor["path"]).read_text().splitlines()
                    for declaration in anchor["declarations"]:
                        self.assertEqual(
                            lines[declaration["line"] - 1].strip(), declaration["text"]
                        )

    def test_reconciliation_records_match_reviewed_inputs(self):
        pins = load("implementation-hashes.json")
        audit = load("reconciliation.json")
        integration = {
            row["path"]: row for row in load("integration-reconciliation.json")["reviews"]
        }
        rows = audit["reviews"]
        self.assertEqual(len(rows), len({row["path"] for row in rows}))
        for row in rows:
            reviewed = row["current"]
            if row["path"] in integration:
                delta = integration[row["path"]]
                self.assertEqual(reviewed, delta["feature_sha256"])
                reviewed = delta["integrated_sha256"]
            self.assertEqual(reviewed, pins[row["path"]])
            self.assertNotEqual(row["pin"], row["current"])
            self.assertTrue(row["inspection"])
            self.assertTrue(row["resolution"])
            self.assertRegex(row["reviewed_delta_sha256"], r"^[0-9a-f]{64}$")
        for row in audit["new_inputs"]:
            self.assertEqual(row["sha256"], pins[row["path"]])

    def test_integration_reviews_cover_pin_union_without_losing_ui_history(self):
        pins = load("implementation-hashes.json")
        audit = load("integration-reconciliation.json")
        rows = audit["reviews"]
        reviewed = {row["path"] for row in rows}
        unchanged = set(audit["unchanged_feature_inputs"])
        self.assertEqual(len(rows), len(reviewed))
        self.assertFalse(reviewed & unchanged)
        self.assertEqual(reviewed | unchanged, set(pins))
        for row in rows:
            self.assertEqual(row["integrated_sha256"], pins[row["path"]])
            self.assertEqual(row["main_sha256"], row["main_pin_sha256"])
            self.assertTrue(row["inspection"])
            self.assertTrue(row["affected_families"])
            self.assertRegex(row["reviewed_delta_sha256"], r"^[0-9a-f]{64}$")
        self.assertFalse(audit["inherited_main_pin_mismatches"])

    def test_case_inventory_and_required_actions_exist(self):
        cases = load("cases.json")
        self.assertEqual(len(cases), len({case["id"] for case in cases}))
        self.assertEqual(
            {case["config"] for case in cases},
            {p.name for p in FORMAL.glob("*.cfg")},
        )
        for case in cases:
            text = (FORMAL / (case["module"] + ".tla")).read_text()
            for action in case.get("required_actions", []):
                self.assertRegex(text, r"(?m)^" + re.escape(action) + r"(?:\(|\s*==)")
            if case.get("violation"):
                directive = "PROPERTY" if case.get("liveness") else "INVARIANT"
                config = (FORMAL / case["config"]).read_text()
                self.assertIn(directive + " " + case["violation"], config)
            else:
                self.assertTrue(case["required_actions"])


if __name__ == "__main__":
    unittest.main()
