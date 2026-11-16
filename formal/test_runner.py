"""Gate classification tests, supplementary to real JVM negative controls."""
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location(
    "formal_runner", Path(__file__).resolve().parents[1] / "scripts/check-formal.py"
)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class GateClassification(unittest.TestCase):
    def test_requires_exhaustion_and_success(self):
        good = ("Model checking completed. No error has been found.\n"
                "20 states generated, 10 distinct states found, 0 states left on queue.\nFinished in")
        self.assertTrue(runner.classify({}, 0, good)[0])
        for code, text, timeout in [
            (1, good, False), (0, good, True),
            (0, good.replace("0 states left", "1 states left"), False),
            (0, "Finished in", False),
        ]:
            self.assertFalse(runner.classify({}, code, text, timeout)[0])

    def test_negative_requires_named_nonzero_counterexample(self):
        case = {"violation": "NoRetry"}
        trace = "Invariant NoRetry is violated.\nThe behavior up to this point is"
        self.assertTrue(runner.classify(case, 12, trace)[0])
        self.assertFalse(runner.classify(case, 0, trace)[0])
        self.assertFalse(runner.classify(case, 12, trace.replace("NoRetry", "Other"))[0])
        self.assertFalse(runner.classify(case, 1, "OutOfMemoryError")[0])
        self.assertFalse(runner.classify(case, 12, trace, True)[0])

    def test_liveness_requires_actual_trace(self):
        trace = "Temporal properties were violated.\nThe behavior up to this point is"
        self.assertTrue(runner.classify({"violation": "AllSettled"}, 13, trace)[0])
        self.assertFalse(runner.classify({"violation": "AllSettled"}, 0, trace)[0])

    def test_negative_rejects_resource_failure_after_trace(self):
        trace = "Invariant NoRetry is violated.\nThe behavior up to this point is"
        self.assertFalse(runner.classify({"violation": "NoRetry"}, 12, trace + "\nOutOfMemoryError")[0])
        self.assertFalse(runner.classify({"violation": "NoRetry"}, 137, trace)[0])


if __name__ == "__main__":
    unittest.main()
