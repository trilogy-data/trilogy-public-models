# Lahman Baseball Database

Major league baseball history, 1871-2025, from the [SABR Lahman Baseball
Database](https://sabr.org/lahman-database/) (December 2025 release). The data
is copyright 1996-2025 by SABR via donation from Sean Lahman, licensed under
[CC BY-SA 3.0](http://creativecommons.org/licenses/by-sa/3.0/).

The full set of raw CSVs (28 files, including Pitching, Fielding, Salaries,
HallOfFame, and the rest) is hosted publicly at
`gs://trilogy_public_models/duckdb/lahman/` — browse at
`https://storage.googleapis.com/trilogy_public_models/duckdb/lahman/<file>`.
This model covers the three core tables; the remaining files are uploaded and
ready if the model grows.

## Tables

- **player** (`People.csv`) — one row per person who has played, keyed by
  `player_id`.
- **team** (`Teams.csv`) — one row per team per season, keyed by
  `(year, team_id)`; includes standings, season totals, park, and attendance.
- **batting** (`Batting.csv`) — one row per player per season per stint,
  keyed by `(player.player_id, team.year, stint)`.

## Example

```preql
import batting as batting;

WHERE batting.team.year >= 1920
SELECT
    batting.player.name,
    batting.total_home_runs,
ORDER BY
    batting.total_home_runs desc
LIMIT 10;
```
