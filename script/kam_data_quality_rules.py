import pandas as pd

TEST_DISTRICTS = {"TEST", "TEST_PHASE6"}
TEST_CROPS = {"DIAGNOSTIC", "DUPLICATE_TEST"}
TEST_MARKETS = {"TEST", "TEST_PHASE6"}


def split_test_records(df):
    """Separate known diagnostic records from market analytics."""
    if df.empty:
        return df.copy(), df.copy()

    district = (
        df["district"].fillna("").astype(str).str.strip().str.upper()
    )
    crop = df["crop"].fillna("").astype(str).str.strip().str.upper()
    market = df["market"].fillna("").astype(str).str.strip().str.upper()

    test_mask = (
        district.isin(TEST_DISTRICTS)
        | crop.isin(TEST_CROPS)
        | market.isin(TEST_MARKETS)
    )

    market_df = df.loc[~test_mask].copy()
    test_df = df.loc[test_mask].copy()

    return market_df, test_df
