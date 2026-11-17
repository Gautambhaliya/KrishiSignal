from scripts.price_analytics import calculate_historical_alerts
import unittest
import pandas as pd


def calculate_historical_alerts(df, threshold=20):
    df = df.copy()

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["modal_price"] = pd.to_numeric(
        df["modal_price"], errors="coerce"
    )

    df = df.dropna(
        subset=[
            "date", "district", "market",
            "commodity", "modal_price"
        ]
    )
    df = df[df["modal_price"] > 0]

    keys = ["district", "market", "commodity", "variety"]

    daily = (
        df.groupby(
            ["date"] + keys,
            dropna=False,
            as_index=False,
        )
        .agg(modal_price=("modal_price", "mean"))
        .sort_values("date")
    )

    daily["previous_price"] = (
        daily.groupby(keys, dropna=False)["modal_price"].shift(1)
    )

    daily["change_percent"] = (
        (daily["modal_price"] - daily["previous_price"])
        / daily["previous_price"] * 100
    )

    return daily[
        daily["previous_price"].notna()
        & (daily["previous_price"] > 0)
        & (daily["change_percent"].abs() >= threshold)
    ].copy()


class TestHistoricalAlerts(unittest.TestCase):

    def setUp(self):
        self.df = pd.DataFrame({
            "date": [
                "2026-07-01",
                "2026-07-02",
                "2026-07-03",
                "2026-07-04",
            ],
            "district": ["Ahmedabad"] * 4,
            "market": ["Ahmedabad APMC"] * 4,
            "commodity": ["Tomato"] * 4,
            "variety": ["Local"] * 4,
            "modal_price": [100, 130, 120, 90],
        })

    def test_increase_alert(self):
        alerts = calculate_historical_alerts(self.df)
        increase = alerts[alerts["change_percent"] > 0]
        self.assertEqual(len(increase), 1)
        self.assertAlmostEqual(
            increase.iloc[0]["change_percent"], 30.0
        )

    def test_decrease_alert(self):
        alerts = calculate_historical_alerts(self.df)
        decrease = alerts[alerts["change_percent"] < 0]
        self.assertEqual(len(decrease), 1)
        self.assertAlmostEqual(
            decrease.iloc[0]["change_percent"], -25.0
        )

    def test_below_threshold_not_alerted(self):
        alerts = calculate_historical_alerts(self.df)
        self.assertFalse(
            (alerts["date"] == pd.Timestamp("2026-07-03")).any()
        )

    def test_single_record_has_no_alert(self):
        alerts = calculate_historical_alerts(self.df.iloc[:1])
        self.assertEqual(len(alerts), 0)


if __name__ == "__main__":
    unittest.main()
