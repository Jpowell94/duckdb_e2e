---
title: How Lethal Was COVID in Each US State?
---

### Data Table

```sql states_by_median_lethality
Select state, median(lethality) As median_lethality From covid_cases.covid_cases_query Group by state Order by median_lethality Desc Limit 10
```

<DataTable data="{states_by_median_lethality}" rows=10/>
