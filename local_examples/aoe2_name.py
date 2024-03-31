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
    win_rate,
    maps.name
order by
    win_rate desc
;
"""


results = executor.execute_text(QA_4)

for row in results[0].fetchall()[0:50]:
    print(row)
