"""
Turn an event log into a conversion table.
"""

import pandas as pd
import lifelines
import convoys.single as convoys
import plotly.express as px


def events_to_conversions(
    data, unit: str, timestamp: str, event: str, conversion_events: list
):
    """Convert an event log to a conversion table, with one entry per unit.

    Args
        data (pandas.DataFrame): Input event log. A long-form data schema, with the
            columns specified in the `unit`, `timestamp`, and `event` args.
        unit, timestamp, event: Names of the corresponding columns in `data`. The
            `event` column must contain values that can be hashed, i.e. not floats. The
            `timestamp` column should be a pandas datetime type.
        conversion_events: Entries in the `event` column that should be considered
            conversions.

    Returns
        ttc (pandas.DataFrame): The conversions table. The row index corresponds to the
            unique entries of the `unit` column in the input `data`. The columns are:
            - `entry_time`: timestamp of the first observation for each
                unit.
            - `conversion_time`: timestamp of the earliest conversion event for each
                user. Missing for users with no conversion events.
            - `conversion_event`: the earliest conversion event for each user, if any.
                Missing for users with no conversion events.
            - `final_obs_time`: `conversion_time` if it exists, otherwise the latest
                timestamp in the input `data`.
            - `duration` (pandas.timedelta): the elapsed time from each unit's entry
                time to `final_obs_time`.
    """

    # Find the entry time for each unit.
    grp = df.groupby(unit)
    ttc = pd.DataFrame(grp[timestamp].min())
    ttc.rename(columns={timestamp: "entry_time"}, inplace=True)

    # Find the *earliest* conversion event for each unit.
    df_convert = df.loc[df[event].isin(conversion_events)]

    grp = df_convert.groupby(unit)
    conversions = grp[timestamp].idxmin()  # these are indices in the original DataFrame
    df_convert = df.iloc[conversions].set_index(unit)

    # Add the conversion time and event to the output DataFrame. Many units will have
    # missing values for these columns.
    ttc["conversion_time"] = df_convert[timestamp]
    ttc["conversion_event"] = df_convert[event]

    # Compute the target variable, using the censoring time as the default value for the
    # final observation time if conversion has not yet happened.
    censoring_time = df["timestamp"].max()

    ttc["final_obs_time"] = ttc["conversion_time"].copy()
    ttc["final_obs_time"].fillna(censoring_time, inplace=True)

    ttc["duration"] = ttc["final_obs_time"] - ttc["entry_time"]

    return ttc


if __name__ == "__main__":

    # Download the `events.csv` table from
    # https://www.kaggle.com/retailrocket/ecommerce-dataset?select=events.csv, and store
    # locally in the relative `data/retailrocket` folder.
    df = pd.read_csv("data/retailrocket/events.csv", dtype={"transactionid": "Int64"})
    df.index.name = "row"
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    print(df.head())

    # Explore the data.
    df["event"].value_counts()
    df.query("visitorid==1050575").sort_values("timestamp")

    # Create the conversions table.
    ttc = events_to_conversions(
        df,
        unit="visitorid",
        timestamp="timestamp",
        event="event",
        conversion_events=["transaction"],
    )

    print(ttc.head())

    # Sanity check the result. This would be good in a unit test.
    assert len(ttc) == df["visitorid"].nunique()
    assert ttc["duration"].max() <= (df["timestamp"].max() - df["timestamp"].min())
    assert (
        ttc["conversion_time"].notnull().sum()
        <= df["event"].isin(["transaction"]).sum()
    )

    # Fit a univariate nonparametric survival curve. This is just to illustrate how the
    # conversions table works, this dataset is not a good use case for survival analysis
    # because most units don't convert.
    model = lifelines.KaplanMeierFitter()

    duration_days = ttc["duration"].dt.total_seconds() / (
        60 * 60 * 24
    )  # denom is seconds per day
    model.fit(durations=duration_days, event_observed=ttc["conversion_time"].notnull())

    # Fit a univariate nonparametric conversion curve.
    model = convoys.KaplanMeier()
    model.fit(B=ttc["conversion_time"].notnull(), T=duration_days)
    conversion_rate = model.predict(list(range(0, int(duration_days.max()))))

    # Plot the conversion curve.
    std_layout = {  # Omit this from the article, for brevity
        "font": dict(size=32),
        "template": "simple_white",
        "showlegend": False,
        "xaxis": dict(showgrid=True, title_font_size=42),
        "yaxis": dict(showgrid=True, title_font_size=42),
    }

    fig = px.line(conversion_rate * 100, template="plotly_white")
    fig.update_traces(line=dict(width=7))
    fig.update_layout(std_layout)
    fig.update_layout(xaxis_title="Days elapsed", yaxis_title="Conversion rate (%)")
    fig.show()
