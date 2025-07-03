# SleeperWarehouse
Sleeper Fantasy Football warehouse to support analytical applications


### Power Rank Calculation

**Exponentially weighted moving average (EWMA)**, used to compute a smoothed `power_rank_score` across weeks for each fantasy football roster.


$$w_i\text{ (the raw score for week }i\text{ )}$$
$$R_i\text{ (the power rank score for week }i\text{ )}$$


$$
R_i =
\begin{cases}
w_{i} & \text{if } i = 1 \\\\
\alpha \cdot w_i + (1 - \alpha) \cdot R_{i-1} & \text{if } i > 1
\end{cases}
$$

$$ \alpha = .3\text{ (weighting factor for the current week's raw score)}$$
$$1 - \alpha = .7\text{ (weight on the previous week's power rank score score)}$$

This results in a ranking that gives more weight to recent performance, but still respects past consistency.

# TODO:

1. Finish ingesting all endpoints (trending_players, playoffs)
2. Finish bronze for all endpoints. Pending (transactions, trending players, playoffs)
3. Multi league handling (Workflow + Ingestion + Model)
4. Playoff modeling and integration with Power Rankings
5. Players (draft + add/drop trend) modeling
