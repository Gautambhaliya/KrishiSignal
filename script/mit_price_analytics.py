import pandas as pd


def calculate_historical_alerts(df, threshold=20):
    """
    Calculate historical price alerts for each market/crop/variety.
    Compare each observation with the previous available date.
    """
    required = {
        "date", "district", "market", "commodity",
        "variety", "modal_price",
    }

    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Missing required columns: {sorted(missing)}"
        )

    data = df.copy()

    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    data["modal_price"] = pd.to_numeric(
        data["modal_price"], errors="coerce"
    )

    data = data.dropna(
        subset=[
            "date", "district", "market",
            "commodity", "modal_price",
        ]
    )
    data = data[data["modal_price"] > 0]

    keys = ["district", "market", "commodity", "variety"]

    daily = (
        data.groupby(
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

    daily["previous_date"] = (
        daily.groupby(keys, dropna=False)["date"].shift(1)
    )

    daily["price_change"] = (
        daily["modal_price"] - daily["previous_price"]
    ).round(2)

    daily["change_percent"] = (
        (
            daily["modal_price"] - daily["previous_price"]
        )
        / daily["previous_price"]
        * 100
    ).round(2)

    alerts = daily[
        daily["previous_price"].notna()
        & (daily["previous_price"] > 0)
        & (daily["change_percent"].abs() >= threshold)
    ].copy()

    alerts["alert_type"] = alerts["change_percent"].apply(
        lambda value: (
            "PRICE_INCREASE" if value > 0 else "PRICE_DECREASE"
        )
    )

    return alerts.reset_index(drop=True)
