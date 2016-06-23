"""
This analyzer reads line-seperated text files and every line is stored as a dummy observation with the line as its value.
"""

from ptocore.analyzercontext import AnalyzerContext

ac = AnalyzerContext()

uploads = ac.spark_uploads(['dummy'])

def create_docs(kv):
    filename, (metadata, data) = kv

    start_time = metadata['meta.start_time']
    for line in data.split(b'\n'):
        doc = {
            'condition': 'dummy',
            'time': start_time,
            'path': ['*'],
            'value': line.decode('utf-8'),
            'sources': {'rawdata': metadata['_id']},
            'analyzer_id': 'dummy'
        }

uploads.flatMap(create_docs).saveToMongoDB(ac.temporary_uri)