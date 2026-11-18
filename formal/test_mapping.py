"""Offline consistency evidence, not an implementation-refinement proof."""
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import unittest

ROOT = Path(__file__).resolve().parents[1]
FORMAL = ROOT / "formal"


def load(name):
    return json.loads((FORMAL / name).read_text())


def production_pin(path, previous):
    """Preserve historical integration evidence and check the next exact delta."""
    changes = {row["path"]: row for row in load("production-reconciliation.json")["changes"]}
    if path not in changes:
        return windows_pin(path, previous)
    row = changes[path]
    if row["before"] != previous:
        raise AssertionError(f"broken production mapping ancestry: {path}")
    if not row["inspection"] or not row["affected_families"]:
        raise AssertionError(f"missing production mapping inspection: {path}")
    return windows_pin(path, row["after"])


def windows_pin(path, previous):
    changes = {row["path"]: row for row in load("windows-native-reconciliation.json")["changes"]}
    if path not in changes:
        return review_pin(path, previous)
    row = changes[path]
    if row["before"] != previous:
        raise AssertionError(f"broken Windows mapping ancestry: {path}")
    return review_pin(path, row["after"])


def review_pin(path, previous):
    audit = load("review-fixes-reconciliation.json")
    changes = {row["path"]: row for row in audit["changes"]}
    if path not in changes:
        return remediation_pin(path, previous)
    row = changes[path]
    if row["before"] != previous:
        raise AssertionError(f"broken review-fixes mapping ancestry: {path}")
    return remediation_pin(path, row["after"])


def remediation_pin(path, previous):
    changes = {row["path"]: row for row in load("review-remediation-reconciliation.json")["changes"]}
    if path not in changes:
        return previous
    row = changes[path]
    if row["before"] != previous:
        raise AssertionError(f"broken remediation mapping ancestry: {path}")
    return row["after"]


class Mapping(unittest.TestCase):
    def test_sql_inventory_covers_actual_database_objects_and_known_safety_schema(self):
        inventory = {row["path"]: row for row in load("inventory.json")["paths"]}
        db = sqlite3.connect(":memory:")
        declared = set()
        for path in ["sql/schema.sql", "sql/memory.sql"]:
            db.executescript((ROOT / path).read_text())
            for declaration in inventory[path]["declarations"]:
                match = re.match(
                    r"CREATE\s+(?:UNIQUE\s+|VIRTUAL\s+)?(TABLE|INDEX|TRIGGER)\s+(?:IF NOT EXISTS\s+)?(\w+)",
                    declaration["text"], re.I,
                )
                self.assertIsNotNone(match, declaration)
                declared.add((match[1].lower(), match[2]))
        actual = {
            (kind, name) for kind, name in db.execute(
                "SELECT type,name FROM sqlite_master WHERE sql IS NOT NULL"
            ) if not name.startswith(("sqlite_", "memory_search_"))
        }
        db.close()
        self.assertEqual(actual, declared)
        self.assertLessEqual({
            ("table", "telegram_ingress"), ("table", "runtime_inbox"),
            ("table", "telegram_ui_controls"), ("table", "runtime_reconciliation_audit"),
            ("index", "telegram_ui_controls_expiry"), ("index", "telegram_ui_inputs_expiry"),
            ("trigger", "reconciliation_audit_immutable_delete"),
            ("trigger", "memory_record_immutable_update"),
        }, declared)

    def test_platform_inventory_includes_whole_file_gates_and_cfg_macros(self):
        inventory = {row["path"]: row for row in load("inventory.json")["paths"]}
        for path, row in inventory.items():
            if not path.endswith(".rs"):
                continue
            required = {
                (i, line.strip()) for i, line in enumerate((ROOT / path).read_text().splitlines(), 1)
                if re.search(r"#!?\[cfg(?:_attr)?\s*\(|\bcfg!\s*\(", line)
            }
            recorded = {(d["line"], d["text"]) for d in row["conditional_branches"]}
            self.assertLessEqual(required, recorded, path)
        topics = inventory["crates/coconutclaw-cli/tests/topics_rich.rs"]["conditional_branches"]
        self.assertIn({"line": 1, "text": "#![cfg(unix)]"}, topics)
        config = inventory["crates/coconutclaw-config/src/lib.rs"]["conditional_branches"]
        self.assertIn("if cfg!(windows) {", {d["text"] for d in config})
        self.assertIn("#[cfg(windows)]", {d["text"] for d in config})
        source = (ROOT / "crates/coconutclaw-config/src/lib.rs").read_text().splitlines()
        self.assertLessEqual(
            {line.strip() for line in source if re.match(r"\s*(?:if|match)\s", line)},
            {d["text"] for d in config},
        )

    def test_remediation_keeps_complete_pin_ancestry_and_executable_obligations(self):
        audit = load("review-remediation-reconciliation.json")
        pins = load("implementation-hashes.json")
        self.assertEqual(audit["base_revision"], "6d8a2c91be8780a1579454378369817d7b1ec164")
        self.assertEqual(set(audit["inherited_pins"]), set(pins))
        rows = {row["path"]: row for row in audit["changes"]}
        self.assertEqual(len(rows), len(audit["changes"]))
        self.assertEqual(set(rows), {path for path, pin in audit["inherited_pins"].items() if pin != pins[path]})
        for path, before in audit["inherited_pins"].items():
            self.assertEqual(remediation_pin(path, before), pins[path])
        for row in rows.values():
            self.assertNotEqual(row["before"], row["after"])
            self.assertRegex(row["delta_sha256"], r"^[0-9a-f]{64}$")
            self.assertTrue(row["inspection"])
            self.assertTrue(row["affected_families"])
            self.assertTrue(row["executable_obligation"])

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
                lines = data.decode().splitlines() if row["declarations"] or row["conditional_branches"] else []
                for declaration in row["declarations"] + row["conditional_branches"]:
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
            self.assertEqual(production_pin(row["path"], reviewed), pins[row["path"]])
            self.assertNotEqual(row["pin"], row["current"])
            self.assertTrue(row["inspection"])
            self.assertTrue(row["resolution"])
            self.assertRegex(row["reviewed_delta_sha256"], r"^[0-9a-f]{64}$")
        for row in audit["new_inputs"]:
            self.assertEqual(production_pin(row["path"], row["sha256"]), pins[row["path"]])

    def test_integration_reviews_cover_pin_union_without_losing_ui_history(self):
        pins = load("implementation-hashes.json")
        audit = load("integration-reconciliation.json")
        rows = audit["reviews"]
        reviewed = {row["path"] for row in rows}
        unchanged = set(audit["unchanged_feature_inputs"])
        self.assertEqual(len(rows), len(reviewed))
        self.assertFalse(reviewed & unchanged)
        added = load("production-reconciliation.json")["new_inputs"]
        windows_added = load("windows-native-reconciliation.json")["new_inputs"]
        review_added = load("review-fixes-reconciliation.json")["new_inputs"]
        self.assertFalse(set(added) & (reviewed | unchanged))
        self.assertFalse(set(windows_added) & (reviewed | unchanged | set(added)))
        self.assertEqual(reviewed | unchanged | set(added) | set(windows_added) | set(review_added), set(pins))
        for row in rows:
            self.assertEqual(production_pin(row["path"], row["integrated_sha256"]), pins[row["path"]])
            self.assertEqual(row["main_sha256"], row["main_pin_sha256"])
            self.assertTrue(row["inspection"])
            self.assertTrue(row["affected_families"])
            self.assertRegex(row["reviewed_delta_sha256"], r"^[0-9a-f]{64}$")
        self.assertFalse(audit["inherited_main_pin_mismatches"])

    def test_production_deltas_and_new_inputs_have_exact_provenance(self):
        pins = load("implementation-hashes.json")
        audit = load("production-reconciliation.json")
        changes = {r["path"]: r for r in audit["changes"]}
        windows_added = load("windows-native-reconciliation.json")["new_inputs"]
        review_added = load("review-fixes-reconciliation.json")["new_inputs"]
        self.assertEqual(set(audit["inherited_pins"]) | set(audit["new_inputs"]) | set(windows_added) | set(review_added), set(pins))
        for path, inherited in audit["inherited_pins"].items():
            if path in changes:
                self.assertEqual(changes[path]["before"], inherited)
            else:
                self.assertEqual(pins[path], windows_pin(path, inherited))
        self.assertEqual(len(audit["changes"]), len({r["path"] for r in audit["changes"]}))
        for row in audit["changes"]:
            self.assertEqual(windows_pin(row["path"], row["after"]), pins[row["path"]])
            self.assertNotEqual(row["before"], row["after"])
            self.assertRegex(row["delta_sha256"], r"^[0-9a-f]{64}$")
            self.assertTrue(row["inspection"])
            self.assertTrue(row["affected_families"])
        for path, row in audit["new_inputs"].items():
            self.assertEqual(windows_pin(path, row["sha256"]), pins[path])
            self.assertTrue(row["inspection"])
            self.assertTrue(row["affected_families"])

    def test_windows_native_deltas_preserve_prior_pins_and_mark_native_boundary(self):
        pins = load("implementation-hashes.json")
        audit = load("windows-native-reconciliation.json")
        self.assertEqual(audit["base_revision"], "114a1cf9e768b681d0d47cdf871e1906386243e2")
        review_added = load("review-fixes-reconciliation.json")["new_inputs"]
        self.assertEqual(set(audit["inherited_pins"]) | set(audit["new_inputs"]) | set(review_added), set(pins))
        changes = {row["path"]: row for row in audit["changes"]}
        self.assertEqual(len(changes), len(audit["changes"]))
        for path, previous in audit["inherited_pins"].items():
            self.assertEqual(windows_pin(path, previous), pins[path])
        for path, row in changes.items():
            self.assertNotEqual(row["before"], row["after"])
            self.assertRegex(row["delta_sha256"], r"^[0-9a-f]{64}$")
            self.assertTrue(row["inspection"])
            self.assertTrue(row["affected_families"])
        for path, row in audit["new_inputs"].items():
            self.assertEqual(review_pin(path, row["sha256"]), pins[path])
            self.assertTrue(row["inspection"])
            self.assertTrue(row["affected_families"])
        self.assertIn("not proved", audit["native_boundary"])

    def test_review_fixes_preserve_ancestry_and_explicit_executable_gaps(self):
        pins = load("implementation-hashes.json")
        audit = load("review-fixes-reconciliation.json")
        self.assertEqual(audit["base_revision"], "2e8eb2defafd62b89496660d5553a2dbf0c413ee")
        self.assertEqual(set(audit["inherited_pins"]) | set(audit["new_inputs"]), set(pins))
        self.assertEqual(len(audit["changes"]), len({r["path"] for r in audit["changes"]}))
        for path, previous in audit["inherited_pins"].items():
            self.assertEqual(review_pin(path, previous), pins[path])
        for row in audit["changes"]:
            self.assertNotEqual(row["before"], row["after"])
            self.assertRegex(row["delta_sha256"], r"^[0-9a-f]{64}$")
            self.assertTrue(row["inspection"])
            self.assertTrue(row["affected_families"])
            self.assertTrue(row["executable_obligation"])
        for path, row in audit["new_inputs"].items():
            self.assertNotIn(path, audit["inherited_pins"])
            self.assertEqual(remediation_pin(path, row["sha256"]), pins[path])
            self.assertTrue(row["inspection"])
            self.assertTrue(row["executable_obligation"])

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
