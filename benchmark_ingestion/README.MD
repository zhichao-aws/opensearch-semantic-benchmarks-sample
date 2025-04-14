# Guide to benchmark ingestion

1. Set `HOSTS` environment variable to OpenSearch endpoint. For example:
```
export HOSTS='localhost:9200'
```

2. Prepare the corpus jsonl file. Each line is an OpenSearch document. For example:
```
{"field_A": "xxxx xxxx", "field_B": "yyyy yyy", "field_C": 1},
{"field_A": "zzzz zzzz", "field_B": "yyyy yyy", "field_C": 2}
...
```
See `prepare_datasets.ipynb` to prepare a corpus of 1 million documents.

3. Prepare the index and ingest pipeline in OpenSearch. Register a ml model inadvance if you need it. For example:
```
PUT /test-index
{
  "settings": {
    "default_pipeline": "nlp-ingest-pipeline-sparse",
    "index.number_of_shards": 2
  },
  "mappings": {
    "properties": {
      "id": {
        "type": "text"
      },
      "embedding": {
        "type": "rank_features"
      },
      "text": {
        "type": "text"
      }
    }
  }
}

PUT /_ingest/pipeline/nlp-ingest-pipeline-sparse
{
  "description": "An sparse encoding ingest pipeline",
  "processors": [
    {
      "sparse_encoding": {
        "model_id": "YkeYY5QBDkchh1paceC6",
        "field_map": {
          "text": "embedding"
        }
      }
    }
  ]
}
```

4. Modify ingest_workload.json. Especially index name, document-count (use `wc -l [filename]` to count) and uncompressed-bytes (use `ls -l [filename]` to count).

5. Run benchmark tool. Add more params like username/password on demand.
``` 
opensearch-benchmark execute-test --target-host $HOSTS --pipeline benchmark-only --workload-path ./ingest_workload.json --on-error abort --kill-running-processes
```