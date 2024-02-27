Select *, (deaths/cases) * 100 as lethality
From us_counties
Where lethality <= 100
Order By lethality Desc
