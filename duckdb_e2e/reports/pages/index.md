---
title: Welcome to Evidence
---

### Data Table

```sql states_by_avg_cases
Select state, Avg(lethality) As avg_lethality From covid_cases.covid_cases_query Group by state Order by avg_lethality Desc Limit 10
```

<DataTable data="{states_by_avg_cases}" rows=10/>
