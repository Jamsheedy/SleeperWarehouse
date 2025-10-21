# SleeperWarehouse
Sleeper Fantasy Football warehouse to support analytical applications


### Power Rank Calculation


#### Week Raw Score
Calculates the team strength for a given week on the basis of roster strength percentile and managerial efficiency.

$$s_i\text{ (The total points scored from starters for week }i\text{ )}$$
$$b_i\text{ (The total points scored from bench players for week }i\text{ )}$$

$$S_i \text{ (Roster Strength)} = s_i + b_i \cdot \alpha$$
$$\alpha = .2\text{ (Bench Points Weight)}$$

$$SP_i \text{ (Roster Strength Percentile)} = \text{ Percentile rank of  }S_i$$
$$m_i\text{ (Missed starter points)}$$
$$E_i \text{ (Managerial Efficiency)} = \frac{s_i}{s_i + m_i}$$


$$w_i = SP_i \cdot \beta + E_i \cdot \gamma$$

$$\beta = .85\text{ (Roster Strength Percentile Weight)}$$
$$\gamma = .15\text{ (Manager Efficiency Weight)}$$



#### Power Rank
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

### Known Model Issues:

- `bench_better_than_starters` is not 100% accurate. Compares like for like positions - does not consider flex combination(s). Only picks the highest scoring bench player for the lowest scoring starter of said position ex: (`_league_id = 1124784521578180608 AND roster_id = 9 AND _matchup_week = 7`)

### TODO:

- Playoff modeling and integration with Power Rankings
- Players (draft + add/drop trend) modeling
