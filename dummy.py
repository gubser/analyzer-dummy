"""
This analyzer reads line-seperated text files and every line is stored as a dummy observation with the line as its value.
"""
import dateutil.parser

from ptocore.analyzercontext import AnalyzerContext

ac = AnalyzerContext()

max_action_id, timespans = ac.sensitivity.basic()
ac.set_result_info(max_action_id, timespans)

print("timespans: ", timespans)
print("max_action_id: ", max_action_id)

uploads_rdd = ac.spark_uploads(['dummy'])

def create_docs(kv):
    filename, (metadata, data) = kv

    # HACK because pymongo-hadoop cannot serialize datetime and objectid
    # store as string now then transform into datetime later
    start_time = metadata['meta']['start_time'].isoformat()
    sources = [metadata['action_id']]

    for line in data.split(b'\n'):
        yield {
            'condition': 'dummy',
            'time': start_time,
            'path': ['*'],
            'value': line.decode('utf-8'),
            'sources': sources,
            'analyzer_id': 'analyzer-dummy'
        }

docs_rdd = uploads_rdd.flatMap(create_docs)

if not docs_rdd.isEmpty():
    docs_rdd.saveToMongoDB(ac.temporary_uri)

# datetime hack see above.
todo = list(ac.temporary_coll.find({}, {'_id': 1, 'time': 1}))
for obs in todo:
    ac.temporary_coll.update_one({'_id': obs['_id']}, {'$set': {'time': dateutil.parser.parse(obs['time'])}})