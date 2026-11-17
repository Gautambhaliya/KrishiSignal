from scripts.price_analytics import calculate_historical_alerts
import os
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import plotly.express as px
from scripts.data_quality_rules import split_test_records
from pyspark.sql import SparkSession

st.set_page_config(
    page_title="KrishiSignal",
    page_icon="🌾",
    layout="wide",
)

st.title("🌾 KrishiSignal")
st.caption("Agricultural Price Intelligence Dashboard")

# Automatically rerun the dashboard every 60 seconds.
refresh_count = st_autorefresh(
    interval=60_000,
    limit=None,
    key="krishisignal_auto_refresh",
)
st.caption(f"Automatic refresh: every 60 seconds · Refresh #{refresh_count}")
st.info(
    "Displaying API-fetched mandi prices from HDFS. "
    "Price alerts require review and are not market predictions."
)

# HDFS paths
STREAM_PATH = (
    "hdfs://localhost:9000/"
    "krishisignal/processed/streaming_mandi_prices_api_v2"
)
ALERT_PATH = (
    "hdfs://localhost:9000/"
    "krishisignal/analytics/price_alerts"
)
VALIDATED_PATH = (
    "hdfs://localhost:9000/"
    "krishisignal/processed/validated_mandi_prices"
)


@st.cache_resource
def get_spark():
    return (
        SparkSession.builder
        .appName("KrishiSignal-Dashboard")
        .master("local[1]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )

def read_parquet(path):
    try:
        return get_spark().read.parquet(path).toPandas()
    except Exception as exc:
        st.warning(f"Could not read data from {path}: {exc}")
        return pd.DataFrame()


if st.button("🔄 Refresh dashboard"):
    st.cache_data.clear()
    st.rerun()

stream_df = read_parquet(STREAM_PATH)
alerts_df = read_parquet(ALERT_PATH)
validated_df = read_parquet(VALIDATED_PATH)

# Separate diagnostic events from market analytics.
if not stream_df.empty:
    stream_df, test_stream_df = split_test_records(stream_df)
else:
    test_stream_df = pd.DataFrame()

if not alerts_df.empty:
    alerts_df, test_alerts_df = split_test_records(alerts_df)
else:
    test_alerts_df = pd.DataFrame()

if not validated_df.empty:
    validated_df, test_validated_df = split_test_records(validated_df)
else:
    test_validated_df = pd.DataFrame()

# Normalize date columns
for frame in (stream_df, alerts_df, validated_df):
    if not frame.empty and "date" in frame.columns:
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")

# KPI metrics
total_events = len(stream_df)
unique_crops = (
    stream_df["crop"].nunique()
    if not stream_df.empty and "crop" in stream_df.columns
    else 0
)
alert_count = len(alerts_df)

latest_price = (
    stream_df.sort_values("processed_at")["modal_price"].iloc[-1]
    if not stream_df.empty and "processed_at" in stream_df.columns
    else None
)

m1, m2, m3, m4 = st.columns(4)
m1.metric("Streaming records", total_events)
m2.metric("Crops observed", unique_crops)
m3.metric("Price alerts", alert_count)
m4.metric(
    "Latest recorded price",
    f"₹{latest_price:,.0f}" if latest_price is not None else "N/A",
)

st.divider()

# Filters
if not stream_df.empty:
    crops = sorted(stream_df["crop"].dropna().unique().tolist())
    districts = sorted(stream_df["district"].dropna().unique().tolist())

    left, right = st.columns(2)
    selected_crops = left.multiselect(
        "Filter crops",
        crops,
        default=crops,
    )
    selected_districts = right.multiselect(
        "Filter districts",
        districts,
        default=districts,
    )

    filtered = stream_df[
        stream_df["crop"].isin(selected_crops)
        & stream_df["district"].isin(selected_districts)
    ].copy()

    st.subheader("Latest Recorded Events")
    display_cols = [
        c for c in [
            "date", "district", "market", "crop",
            "modal_price", "processed_at"
        ]
        if c in filtered.columns
    ]
    st.dataframe(
        filtered[display_cols].sort_values(
            "processed_at", ascending=False
        ),
        use_container_width=True,
    )

    st.subheader("Recorded Prices by Crop and District")
    if not filtered.empty:
        chart_data = (
            filtered.groupby(["crop", "district"], as_index=False)
            ["modal_price"].mean()
        )
        fig = px.bar(
            chart_data,
            x="crop",
            y="modal_price",
            color="district",
            barmode="group",
            title="Average recorded mandi price by crop and district",
            labels={
                "crop": "Crop",
                "modal_price": "Modal price (₹)",
                "district": "District",
            },
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No records match the selected filters.")

    st.subheader("Price Trend")
    if not filtered.empty:
        trend = (
            filtered.groupby(["date", "crop"], as_index=False)
            ["modal_price"].mean()
            .sort_values("date")
        )
        fig = px.line(
            trend,
            x="date",
            y="modal_price",
            color="crop",
            markers=True,
            title="Recorded price over time",
            labels={
                "date": "Date",
                "modal_price": "Modal price (₹)",
            },
        )
        st.plotly_chart(fig, use_container_width=True)
else:
    st.warning(
        "No API records found. Check Kafka-to-Spark streaming and "
        "refresh the dashboard after records reach HDFS."
    )

st.divider()
st.subheader("Price Alerts — Review Required")

st.caption(
    "Historical alerts are calculated from stored Gujarat mandi data. "
    "They are not real-time predictions."
)

# Calculate alerts from historical HDFS data.
HISTORICAL_ALERT_PATH = (
    "hdfs://localhost:9000/krishisignal/"
    "historical/mandi_prices/gujarat_mandi_historical_2026_07.csv"
)

try:
    historical_alert_df = (
        get_spark()
        .read
        .option("header", True)
        .option("inferSchema", True)
        .csv(HISTORICAL_ALERT_PATH)
        .select(
            "date", "district", "market",
            "commodity", "variety", "modal_price"
        )
        .toPandas()
    )

    historical_alert_df["date"] = pd.to_datetime(
        historical_alert_df["date"], errors="coerce"
    )
    historical_alert_df["modal_price"] = pd.to_numeric(
        historical_alert_df["modal_price"], errors="coerce"
    )

    historical_alert_df = historical_alert_df.dropna(
        subset=[
            "date", "district", "market",
            "commodity", "modal_price"
        ]
    )
    historical_alert_df = historical_alert_df[
        historical_alert_df["modal_price"] > 0
    ]

    # Average prices for each date and market/crop/variety.
    daily_alert_prices = (
        historical_alert_df.groupby(
            [
                "date", "district", "market",
                "commodity", "variety"
            ],
            dropna=False,
            as_index=False,
        )
        .agg(modal_price=("modal_price", "mean"))
        .sort_values("date")
    )

    group_keys = [
        "district", "market", "commodity", "variety"
    ]

    daily_alert_prices["previous_price"] = (
        daily_alert_prices.groupby(
            group_keys, dropna=False
        )["modal_price"].shift(1)
    )

    daily_alert_prices["previous_date"] = (
        daily_alert_prices.groupby(
            group_keys, dropna=False
        )["date"].shift(1)
    )

    daily_alert_prices["change_percent"] = (
        (
            daily_alert_prices["modal_price"]
            - daily_alert_prices["previous_price"]
        )
        / daily_alert_prices["previous_price"]
        * 100
    )

    historical_alerts = daily_alert_prices[
        daily_alert_prices["previous_price"].notna()
        & (daily_alert_prices["previous_price"] > 0)
        & (daily_alert_prices["change_percent"].abs() >= 20)
    ].copy()

    historical_alerts["alert_type"] = historical_alerts[
        "change_percent"
    ].apply(
        lambda value: (
            "PRICE_INCREASE" if value > 0 else "PRICE_DECREASE"
        )
    )

    historical_alerts["price_change"] = (
        historical_alerts["modal_price"]
        - historical_alerts["previous_price"]
    ).round(2)

    historical_alerts["change_percent"] = (
        historical_alerts["change_percent"].round(2)
    )

    st.markdown("**Historical price movements — threshold: ±20%**")

    st.metric("Historical alerts found", len(historical_alerts))

    if not historical_alerts.empty:
        display_alerts = historical_alerts[
            [
                "date", "previous_date", "commodity",
                "district", "market", "previous_price",
                "modal_price", "price_change",
                "change_percent", "alert_type"
            ]
        ].sort_values(
            "date", ascending=False
        )

        st.dataframe(
            display_alerts,
            use_container_width=True,
            hide_index=True,
        )

        st.download_button(
            "Download historical alerts CSV",
            data=display_alerts.to_csv(
                index=False
            ).encode("utf-8"),
            file_name="krishisignal_historical_alerts.csv",
            mime="text/csv",
        )
    else:
        st.info(
            "No historical price changes crossed the ±20% threshold."
        )

except Exception as exc:
    st.warning(
        "Could not calculate historical alerts. "
        f"Details: {exc}"
    )

st.markdown("**Existing batch alerts**")

if not alerts_df.empty:
    alert_cols = [
        column for column in [
            "date", "crop", "district", "previous_price",
            "modal_price", "change_percent", "alert_type"
        ]
        if column in alerts_df.columns
    ]

    st.dataframe(
        alerts_df[alert_cols].sort_values(
            "date", ascending=False
        ),
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("No existing batch alerts are available.")
with st.expander("About this dashboard"):
    st.write(
        "Streaming records are read from HDFS Parquet output. "
        "Price alerts are generated by the batch analysis in Phase 3. "
        "The dashboard does not yet calculate alerts continuously "
        "from Kafka events."
    )


# =========================
# PHASE 5: ADVANCED ANALYTICS
# =========================

st.divider()
st.header("Advanced Price Analytics")

if not stream_df.empty:
    analytics_df = stream_df.copy()

    # Ensure prices are numeric and dates are valid
    analytics_df["modal_price"] = pd.to_numeric(
        analytics_df["modal_price"], errors="coerce"
    )
    analytics_df = analytics_df.dropna(
        subset=["date", "crop", "district", "modal_price"]
    )

    # Apply the dashboard's existing filters
    analytics_df = analytics_df[
        analytics_df["crop"].isin(selected_crops)
        & analytics_df["district"].isin(selected_districts)
    ].copy()

    if not analytics_df.empty:
        # 1. Crop-wise statistics
        st.subheader("Crop-wise Price Summary")

        crop_summary = (
            analytics_df.groupby("crop")["modal_price"]
            .agg(
                Record_Count="count",
                Average_Price="mean",
                Minimum_Price="min",
                Maximum_Price="max",
            )
            .reset_index()
        )

        crop_summary[
            ["Average_Price", "Minimum_Price", "Maximum_Price"]
        ] = crop_summary[
            ["Average_Price", "Minimum_Price", "Maximum_Price"]
        ].round(2)

        st.dataframe(crop_summary, use_container_width=True)

        # 2. District-wise comparison
        st.subheader("District-wise Price Comparison")

        district_summary = (
            analytics_df.groupby(["district", "crop"], as_index=False)
            .agg(
                Average_Price=("modal_price", "mean"),
                Record_Count=("modal_price", "count"),
            )
        )

        fig = px.bar(
            district_summary,
            x="district",
            y="Average_Price",
            color="crop",
            barmode="group",
            title="Average Recorded Price by District and Crop",
            labels={
                "district": "District",
                "Average_Price": "Average price (₹)",
                "crop": "Crop",
            },
        )
        st.plotly_chart(fig, use_container_width=True)

        # 3. Market-date-based price change analysis
        st.subheader("Price Change Analysis")

        # Aggregate repeated snapshots for the same market date.
        daily_prices = (
            analytics_df.groupby(
                ["crop", "district", "market", "date"],
                as_index=False,
            )
            .agg(
                modal_price=("modal_price", "mean"),
                record_count=("modal_price", "count"),
            )
            .sort_values(
                ["crop", "district", "market", "date"]
            )
        )

        market_keys = ["crop", "district", "market"]

        daily_prices["previous_price"] = (
            daily_prices.groupby(market_keys)["modal_price"].shift(1)
        )
        daily_prices["previous_date"] = (
            daily_prices.groupby(market_keys)["date"].shift(1)
        )

        daily_prices["change_percent"] = (
            (
                daily_prices["modal_price"]
                - daily_prices["previous_price"]
            )
            / daily_prices["previous_price"].replace(0, float("nan"))
        ) * 100

        daily_prices["change_percent"] = (
            daily_prices["change_percent"].round(2)
        )

        daily_prices["change_status"] = daily_prices["change_percent"].apply(
            lambda value: (
                "Increase" if value > 0
                else "Decrease" if value < 0
                else "No change" if pd.notna(value)
                else "Insufficient historical data"
            )
        )

        comparison_columns = [
            "date", "previous_date", "district", "market", "crop",
            "previous_price", "modal_price", "change_percent",
            "change_status", "record_count",
        ]

        comparison = daily_prices[comparison_columns].sort_values(
            ["date", "district", "market", "crop"],
            ascending=[False, True, True, True],
        )

        if comparison["previous_date"].notna().any():
            st.dataframe(
                comparison,
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info(
                "Historical price changes are not available yet. "
                "The current API dataset contains only one market date. "
                "Price changes will be calculated when earlier market-date "
                "records are available for the same crop and market."
            )

            st.dataframe(
                comparison,
                use_container_width=True,
                hide_index=True,
            )

        # 4. Download filtered records
        st.subheader("Export Analytics")

        csv_data = analytics_df.to_csv(index=False).encode("utf-8")

        st.download_button(
            label="Download filtered records as CSV",
            data=csv_data,
            file_name="krishisignal_filtered_records.csv",
            mime="text/csv",
        )

        st.caption(
            "Analytics are calculated from API-fetched mandi records. "
            "They are descriptive and do not predict market prices."
        )
    else:
        st.info("No records match the selected crop and district filters.")
else:
    st.info("Streaming data is unavailable for analytics.")


# =========================
# PHASE 7: API-BASED PRICE SUMMARY

st.divider()
st.header("Mandi Price Summary — API Data")
st.caption(
    "Summary calculated from API-fetched mandi records stored in HDFS."
)

try:
    batch_summary = get_spark().read.parquet(
        STREAM_PATH
    ).toPandas()

    if batch_summary.empty:
        st.info("No API mandi records are available yet.")
    else:
        batch_summary, _ = split_test_records(batch_summary)

        batch_summary["modal_price"] = pd.to_numeric(
            batch_summary["modal_price"], errors="coerce"
        )
        batch_summary = batch_summary.dropna(
            subset=["crop", "district", "modal_price"]
        )
        batch_summary = batch_summary[
            batch_summary["modal_price"] > 0
        ]

        if batch_summary.empty:
            st.info("No valid API mandi prices are available yet.")
        else:
            summary = (
                batch_summary.groupby(
                    ["crop", "district"], as_index=False
                )
                .agg(
                    average_price=("modal_price", "mean"),
                    record_count=("modal_price", "count"),
                    minimum_price=("modal_price", "min"),
                    maximum_price=("modal_price", "max"),
                )
            )

            price_columns = [
                "average_price", "minimum_price", "maximum_price"
            ]
            summary[price_columns] = summary[price_columns].round(2)

            st.dataframe(
                summary.sort_values(
                    "record_count", ascending=False
                ),
                use_container_width=True,
                hide_index=True,
            )

            fig_batch = px.bar(
                summary,
                x="crop",
                y="average_price",
                color="district",
                title="Average API Mandi Price by Crop and District",
                labels={
                    "crop": "Crop",
                    "average_price": "Average modal price (₹)",
                    "district": "District",
                },
            )
            st.plotly_chart(fig_batch, use_container_width=True)

except Exception as exc:
    st.warning(f"Could not calculate the API mandi summary: {exc}")


# =========================
# HISTORICAL PRICE ANALYTICS
# =========================

HISTORICAL_CSV_PATH = (
    "hdfs://localhost:9000/krishisignal/"
    "historical/mandi_prices/gujarat_mandi_historical_2026_07.csv"
)


@st.cache_data(ttl=300)
def load_historical_mandi_data(path):
    historical = (
        get_spark()
        .read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
        .toPandas()
    )

    historical["date"] = pd.to_datetime(
        historical["date"], errors="coerce"
    )
    historical["modal_price"] = pd.to_numeric(
        historical["modal_price"], errors="coerce"
    )

    historical = historical.dropna(
        subset=["date", "commodity", "market", "modal_price"]
    )
    historical = historical[historical["modal_price"] > 0].copy()

    for column in ["commodity", "market", "variety", "district"]:
        if column in historical.columns:
            historical[column] = (
                historical[column].fillna("").astype(str).str.strip()
            )

    return historical


st.divider()
st.header("Historical Price Analytics")
st.caption(
    "Gujarat mandi history from July 2026. "
    "Historical observations are separate from the live API stream."
)

try:
    historical_df = load_historical_mandi_data(HISTORICAL_CSV_PATH)

    if historical_df.empty:
        st.info("No valid historical mandi records were found.")
    else:
        crop_options = sorted(
            historical_df["commodity"].unique().tolist()
        )

        selected_historical_crop = st.selectbox(
            "Select historical crop",
            crop_options,
            key="historical_crop_filter",
        )

        crop_df = historical_df[
            historical_df["commodity"] == selected_historical_crop
        ].copy()

        market_options = sorted(crop_df["market"].unique().tolist())

        selected_historical_market = st.selectbox(
            "Select APMC market",
            market_options,
            key="historical_market_filter",
        )

        market_df = crop_df[
            crop_df["market"] == selected_historical_market
        ].copy()

        variety_options = sorted(
            market_df["variety"].unique().tolist()
        )

        selected_historical_variety = st.selectbox(
            "Select variety",
            variety_options,
            key="historical_variety_filter",
        )

        selected_history = market_df[
            market_df["variety"] == selected_historical_variety
        ].copy()

        daily_history = (
            selected_history.groupby("date", as_index=False)
            .agg(
                modal_price=("modal_price", "mean"),
                observations=("modal_price", "count"),
            )
            .sort_values("date")
        )

        daily_history["modal_price"] = daily_history[
            "modal_price"
        ].round(2)

        if daily_history.empty:
            st.info("No historical prices match these filters.")
        else:
            latest_row = daily_history.iloc[-1]
            latest_price = float(latest_row["modal_price"])

            k1, k2, k3 = st.columns(3)

            k1.metric(
                "Latest available price",
                f"₹{latest_price:,.2f}",
                help=f"Available date: {latest_row['date'].date()}",
            )

            k2.metric(
                "Historical observations",
                len(daily_history),
            )

            if len(daily_history) >= 2:
                previous_row = daily_history.iloc[-2]
                previous_price = float(previous_row["modal_price"])

                difference = latest_price - previous_price
                percent_change = (
                    difference / previous_price * 100
                    if previous_price > 0 else None
                )

                k3.metric(
                    "Change vs previous available record",
                    (
                        f"{percent_change:+.2f}%"
                        if percent_change is not None else "N/A"
                    ),
                    delta=f"₹{difference:+,.2f}",
                    help=(
                        f"Compared with {previous_row['date'].date()}; "
                        "the previous record may not be the previous "
                        "calendar day."
                    ),
                )
            else:
                k3.metric("Change vs previous record", "N/A")

            trend_fig = px.line(
                daily_history,
                x="date",
                y="modal_price",
                markers=True,
                title=(
                    f"{selected_historical_crop} Price Trend — "
                    f"{selected_historical_market}"
                ),
                labels={
                    "date": "Arrival date",
                    "modal_price": "Average modal price (₹/quintal)",
                },
                hover_data=["observations"],
            )

            st.plotly_chart(trend_fig, use_container_width=True)

            st.subheader("Historical Price Records")
            st.dataframe(
                daily_history.sort_values("date", ascending=False),
                use_container_width=True,
                hide_index=True,
            )

            st.download_button(
                label="Download selected historical prices as CSV",
                data=daily_history.to_csv(index=False).encode("utf-8"),
                file_name="krishisignal_historical_price_trend.csv",
                mime="text/csv",
            )

            st.caption(
                "Prices are historical observations in ₹ per quintal. "
                "The chart shows an average when multiple records exist "
                "for the selected date, market and variety. "
                "It does not predict future prices."
            )

except Exception as exc:
    st.error(
        "Historical analytics could not be loaded. "
        "Check that HDFS is running and the historical CSV exists. "
        f"Details: {exc}"
    )
