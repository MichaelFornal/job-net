# A closed regex list cannot find what it does not list

The tenant census answered "how many tenants does family X have?" for a **closed
list of 19 regexes**. A platform nobody had written a regex for was therefore
not merely unbuilt — it was invisible: to the census, to the build queue, to the
coverage report, and to every ranking either of them fed.

On 2026-08-20 the residue printed by
[host_census.py](../../host_census.py) held UKG (366 boards), Manatal /
`careers-page.com` (562), NEOGOV (156), Dayforce (120) and Cornerstone (63) —
none of which had ever appeared in a census, while a month of "un-piped ATS"
ranking work went straight past them. All five had fetchers within days.

## Why "add more regexes" is the wrong shape

Adding patterns keeps the blind spot and only moves it. The census has to carry
**no allow-list of things to look for**: it reports what is actually in the
apply-URL column and *subtracts* what is already piped, so the unknown tail is
the output rather than the omission. Its one hand-maintained list is that
subtraction — getting it wrong makes the report noisier, never blinder, the
opposite failure mode of the pattern list it replaced.

## Rank by employers, not by hits

The 2026-07-28 census recorded this the hard way: one platform showed 53,094 URL
hits across 10 tenants, another 123k across 132. Ranking on occurrences inverts
the queue and aims the next build at the platform with the *fewest* employers.

Counting boards is itself ambiguous: a tenant is either a host label
(`{tenant}.bamboohr.com`) or a path segment (`careers-page.com/{tenant}/`), and
the census cannot know which in advance. Counting only hosts scores every
path-tenant platform as "1 board"; counting only path segments scores
host-tenant platforms as noise. Both are counted and reported, but the ranking
column is neither — it is the count of **distinct employers on the apex**, read
from the corpus's own company column, because the two URL parses mis-fire in
opposite directions and the company column does not. One platform reads 39 hosts
/ 397 path segments / 397 employers; one employer's vanity career domain reads
0 hosts / 1,075 path segments / **one** employer, because its URL shape is
`/job/{city}/{title}/{id}`.
