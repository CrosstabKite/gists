"""
How to convert a duration table to a survival table, then compute the Kaplan-Meier and
Nelson-Aalen estimates. Accompanies the article at
https://crosstab.io/articles/durations-to-survivals.
"""

import altair as alt
import numpy as np
import pandas as pd


## Load data
durations = pd.read_parquet("data/retailrocket_durations.parquet")
print(durations.head())


## Round days down to integers
durations["duration_days"] = np.ceil(durations["duration_days"]).astype(int)

## ???
grp = durations.groupby("duration_days")
survival = pd.DataFrame(
    {"num_obs": grp.size(), "events": grp["endpoint_observed"].sum()}
)
print(survival.head())


# Get the total number of observations that had happened *before* each row. The
# complement of that is the number still at-risk of the event *in* each row.
num_subjects = len(durations)
prior_count = survival["num_obs"].cumsum().shift(1, fill_value=0)
survival.insert(0, "at_risk", num_subjects - prior_count)
print(survival.head())

# Keep only the rows with at least one event, and insert an initial row of 0's.
survival = survival.loc[survival["events"] > 0]

# survival.loc[0] = {"events": 0, "at_risk": num_subjects}
# survival.sort_index(inplace=True)

# The number of censored includes units censored at any duration between an event
# duration and the next event duration. This has to be backed out of the number at-risk:
# if the number at-risk falls from 15 to 11, but only one event was observed, then the
# number of censored in that interval must be 3.
survival["censored"] = (
    survival["at_risk"]
    - survival["at_risk"].shift(-1, fill_value=0)
    - survival["events"]
)

print(survival)


## Kaplan-Meier survival function estimate
inverse_hazard = 1 - survival["events"] / survival["at_risk"]
survival["survival_proba"] = inverse_hazard.cumprod()
print(survival)

survival["conversion_pct"] = 100 * (1 - survival["survival_proba"])


fig = (
    alt.Chart(survival.reset_index())
    .mark_line(interpolate="step-after")
    .encode(
        x=alt.X("duration_days", axis=alt.Axis(title="Duration (days)")),
        y=alt.Y(
            "survival_proba",
            axis=alt.Axis(title="Survival probability"),
            scale=alt.Scale(zero=False),
        ),
    )
)

fig.save("rocketretail_survival.svg")

fig = (
    alt.Chart(survival.reset_index())
    .mark_line(interpolate="step-after")
    .encode(
        x=alt.X("duration_days", axis=alt.Axis(title="Duration (days)")),
        y=alt.Y("conversion_pct", axis=alt.Axis(title="Conversion probability")),
    )
)

fig.save("rocketretail_conversion.svg")


## Aalen-Nelson cumulative hazard estimate
survival["cumulative_hazard"] = (survival["events"] / survival["at_risk"]).cumsum()

fig = (
    alt.Chart(survival.reset_index())
    .mark_line(interpolate="step-after")
    .encode(
        x=alt.X("duration_days", axis=alt.Axis(title="Duration (days)")),
        y=alt.Y("cumulative_hazard", axis=alt.Axis(title="Cumulative hazard")),
    )
)

fig.save("rocketretail_hazard.svg")
