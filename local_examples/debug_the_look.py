
from trilogy_public_models import models
from preql import Executor, Dialects
from preql.hooks.query_debugger import DebuggingHook


environment = models['bigquery.usa_names']
executor = Dialects.BIGQUERY.default_executor(environment=environment, hooks=[DebuggingHook()])
# environment.parse('auto question.answer.count <- count(answer.id) by question.id;')
# environment.parse('auto question.answer.count.avg <- answer.count/ question.count;')

# test = environment.concepts['question.answer.count.avg']
# # print(test.lineage)
# print(test.derivation)
# print(test.lineage.concept_arguments)
# parents = resolve_function_parent_concepts(environment.concepts['question.answer.count.avg'])

# for x in parents:
#     print(x)
text = """
key vermont_names <- filter name where state = 'VT';


SELECT
name_count.sum,
year
where
year = 1950

LIMIT 100;"""
results = executor.execute_text(text


)
for row in results:
    answers = row.fetchall()
    for x in answers:
        print(x)






