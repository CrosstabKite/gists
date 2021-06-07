"""
Turn an event log into a conversion table.
"""

import numpy as np
import pandas as pd
import lifelines
import plotly.express as px
from sksurv.util import Surv
from sksurv.nonparametric import SurvivalFunctionEstimator


def events_to_durations(
    event_log, unit: str, timestamp: str, event: str, endpoints: list
):
    """Convert an event log to a conversion table, with one entry per unit.

    Args
        event_log (pandas.DataFrame): Input event log. A long-form data schema, with the
            columns specified in the `unit`, `timestamp`, and `event` args.
        unit, timestamp, event: Names of the corresponding columns in `data`. The
            `event` column must contain values that can be hashed, i.e. not floats. The
            `timestamp` column should be a pandas datetime type.
        endpoints: Event types of interest, in the `event` column.

    Returns
        duration (pandas.DataFrame): The duration table. The row index corresponds to
            the unique entries of the `unit` column in the input `data`. The columns
            are:
            - `entry_time`: timestamp of the first observation for each
                unit.
            - `endpoint_time`: timestamp of the earliest endpoint event for each user.
                Missing for users with no target events.
            - `endpoint`: the earliest endpoint event type for each user, if any. 
                Missing for users with no target events.
            - `final_obs_time`: `endpoint_time` if it exists, otherwise the latest
                timestamp in the input `data`.
            - `duration` (pandas.timedelta): the elapsed time from each unit's entry
                time to `final_obs_time`.
    """

    # Find the entry time for each unit.
    grp = event_log.groupby(unit)
    durations = pd.DataFrame(grp[timestamp].min())
    durations.rename(columns={timestamp: "entry_time"}, inplace=True)

    # Find the *earliest* endpoint event for each unit.
    df_endpoint = event_log.loc[event_log[event].isin(endpoints)]

    grp = df_endpoint.groupby(unit)
    endpoint_events = grp[
        timestamp
    ].idxmin()  # these are indices in the original DataFrame
    df_endpoint = df.iloc[endpoint_events].set_index(unit)

    # Add the endpoint and endpoint time to the output DataFrame. Many units will have
    # missing values for these columns.
    durations["endpoint"] = df_endpoint[event]
    durations["endpoint_time"] = df_endpoint[timestamp]

    # Compute the target variable, using the censoring time as the default value for the
    # final observation time if an endpoint has not yet happened.
    censoring_time = df["timestamp"].max()

    durations["final_obs_time"] = durations["endpoint_time"].copy()
    durations["final_obs_time"].fillna(censoring_time, inplace=True)

    durations["duration"] = durations["final_obs_time"] - durations["entry_time"]

    return durations


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

    # Create the durations table.
    durations = events_to_durations(
        df,
        unit="visitorid",
        timestamp="timestamp",
        event="event",
        endpoints=["transaction"],
    )

    print(durations.head())

    # Sanity check the result. This would be good in a unit test.
    assert len(durations) == df["visitorid"].nunique()
    assert durations["duration"].max() <= (
        df["timestamp"].max() - df["timestamp"].min()
    )
    assert (
        durations["endpoint_time"].notnull().sum()
        <= df["event"].isin(["transaction"]).sum()
    )

    # Extra preprocessing steps
    durations["endpoint_observed"] = durations["endpoint"].notnull()
    durations["duration_days"] = (
        durations["duration"].dt.total_seconds() / (60 * 60 * 24)  # denominator is the number of seconds in a day
    )

    # Fit a univariate nonparametric cumulative hazard function with Lifelines.
    model = lifelines.NelsonAalenFitter()
    model.fit(durations["duration_days"], durations["endpoint_observed"])

    # Fit a univariate nonparametric survival function with scikit-survival.
    target = Surv().from_dataframe("endpoint_observed", "duration_days", durations)
    model = SurvivalFunctionEstimator()
    model.fit(target)

    time_grid = np.linspace(0, 120, 1000)
    proba_survival = model.predict_proba(time_grid)
    conversion_rate = 1 - proba_survival

    # Plot the conversion curve.
    std_layout = {  # Omit this from the article, for brevity
        "font": dict(size=32),
        "template": "simple_white",
        "showlegend": False,
        "xaxis": dict(showgrid=True, title_font_size=42),
        "yaxis": dict(showgrid=True, title_font_size=42),
    }

    fig = px.line(x=time_grid, y=conversion_rate * 100, template="plotly_white")
    fig.update_traces(line=dict(width=7))
    fig.update_layout(std_layout)
    fig.update_layout(xaxis_title="Days elapsed", yaxis_title="Conversion rate (%)")
    # fig.show()
    fig.write_image("rocketretail_conversion.png", height=1400, width=1400)
