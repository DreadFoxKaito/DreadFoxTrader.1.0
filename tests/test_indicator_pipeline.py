import unittest

from app.indicator_pipeline import apply_final_candle_policy, heikin_ashi_series


class IndicatorPipelineTests(unittest.TestCase):
    def test_current_candle_policy_includes_latest(self):
        result = apply_final_candle_policy(
            opens=[1, 2],
            highs=[2, 3],
            lows=[0.5, 1.5],
            closes=[1.5, 2.5],
            use_current_candle=True,
        )
        self.assertEqual(result.closes, [1.5, 2.5])
        self.assertTrue(result.latest_included)
        self.assertFalse(result.latest_excluded)

    def test_current_candle_policy_excludes_latest_when_closed_only(self):
        result = apply_final_candle_policy(
            opens=[1, 2],
            highs=[2, 3],
            lows=[0.5, 1.5],
            closes=[1.5, 2.5],
            use_current_candle=False,
        )
        self.assertEqual(result.closes, [1.5])
        self.assertFalse(result.latest_included)
        self.assertTrue(result.latest_excluded)

    def test_heikin_ashi_formula_is_recursive_and_deterministic(self):
        ho, hh, hl, hc = heikin_ashi_series(
            opens=[10, 12],
            highs=[13, 14],
            lows=[9, 11],
            closes=[12, 13],
        )
        self.assertEqual(hc[0], 11.0)
        self.assertEqual(ho[0], 11.0)
        self.assertEqual(hh[0], 13.0)
        self.assertEqual(hl[0], 9.0)
        self.assertEqual(hc[1], 12.5)
        self.assertEqual(ho[1], 11.0)
        self.assertEqual(hh[1], 14.0)
        self.assertEqual(hl[1], 11.0)


if __name__ == "__main__":
    unittest.main()
