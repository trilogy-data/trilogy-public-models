from preql import Dialects
from preql.constants import CONFIG

from trilogy_public_models import models
from preql.hooks.query_debugger import DebuggingHook

CONFIG.hash_identifiers = True
env = models["bigquery.age_of_empires_2"]


executor = Dialects.BIGQUERY.default_executor(environment=env, hooks=[DebuggingHook()])
QA_1 = """

auto num_of_matches <- count(matches.id);


SELECT
matches.map_id,
num_of_matches
order by num_of_matches desc


LIMIT 100;"""
QA_2 = """

auto action_count <- count(match_player_actions.id);

select 

matches.map_id,
match_player_actions.event_type,
action_count

order by action_count desc


LIMIT 100;"""

QA_3 = """

SELECT
    match_event.name,
    match_event.id.count,
    objects.name
WHERE
    match_event.name = 'BUILDING'
order by
    match_event.id.count 
    desc
;
"""

QA_3 = """

SELECT
    match_event.name,
    match_event.id.count,
    objects.name
WHERE
    match_event.name = 'BUILDING'
order by
    match_event.id.count 
    desc
;
"""

QA_4 = """

key win_matches <- filter match_players.id where match_players.victory = 1;
metric win_rate <- count(win_matches) / count(match_players.id);

SELECT
    civilizations.name,
    match_players.victory,
    win_rate
order by
    win_rate desc
;
"""


QA_5 = """
select
    match_event.seconds_into_game,
    match_event.name,
    objects.name,
    match_event.id.count
order by
    match_event.seconds_into_game asc
limit 100
;
"""

QA_6 = """
select
    round((match_event.seconds_into_game/60)/60,0) -> game_hours,
    match_event.name,
    objects.name,
    match_event.id.count
where
    game_hours = 1
order by 
    match_event.id.count desc
limit 100
;
"""

WHAT_UNITS_SHOULD_I_BUILD = """

select
    tech_research.id.count,
    technology.name,
    civilizations.name
where 
    civilizations.name = 'Aztecs'
order by
    tech_research.id.count desc
;
"""  # noqa: E501
results = executor.execute_text(WHAT_UNITS_SHOULD_I_BUILD)

for row in results[0].fetchall():
    print(row)
