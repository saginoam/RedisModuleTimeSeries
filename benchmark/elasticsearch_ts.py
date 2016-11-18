from datetime import datetime
from elasticsearch import Elasticsearch

es = Elasticsearch()

# es.index(index="my-index", doc_type="test-type", id=42, body={"any": "data", "timestamp": datetime.now()})
#
# res = es.get(index="my-index", doc_type="test-type", id=42)['_source']

es.update(index="my-index", doc_type="test-type", id=1, body={
    "script" : "ctx._source.count += count; ctx._source.sum += sum ",
    "params" : {
        "count" : 4,
        "sum": 1.5
    },
    "upsert" : {
        "count" : 1,
        "sum": 0
    }
})

res = es.get(index="my-index", doc_type="test-type", id=1)['_source']

print res
