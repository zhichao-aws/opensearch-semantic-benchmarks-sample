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

4. Run ingest command. example:
```
python run_bulk.py --index_name test-index --file_name nfcorpus
```

## To benchmark search relevance

1. use refresh API to refresh the index, or wait the index update in AOSS.

2. prepare queries json file and qrels json file
- If using nfcorpus, this step can be skipped

3. Run relevance command. example:
```
python search_relevance.py --queries_file nfcorpus-queries.json --qrels_file nfcorpus-qrels.json --index_name test-index
```