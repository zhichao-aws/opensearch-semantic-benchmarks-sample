import json
import argparse
from concurrent.futures import ThreadPoolExecutor
from utils import get_os_client
from tqdm import tqdm
from beir.retrieval.evaluation import EvaluateRetrieval
from dotenv import load_dotenv

load_dotenv()


def load_queries_and_qrels(queries_file, qrels_file):
    """
    Load queries and qrels from JSON files

    Args:
        queries_file: Path to queries JSON file
        qrels_file: Path to qrels JSON file

    Returns:
        tuple: (queries dict, qrels dict)
    """
    with open(queries_file, "r") as f:
        queries = json.load(f)
    with open(qrels_file, "r") as f:
        qrels = json.load(f)
    return queries, qrels


def create_query_body(
    query_text, query_type="neural_sparse", embedding_field="embedding"
):
    """
    Create query body for neural sparse search

    Args:
        query_text: Text to search for
        embedding_field: Field name for embedding

    Returns:
        dict: Query body for OpenSearch
    """
    if query_type == "neural_sparse":
        return {
            "query": {
                "neural_sparse": {
                    embedding_field: {
                        "query_text": query_text,
                    }
                },
            },
            "_source": ["id", "text"],
            "size": 15,
        }
    elif query_type == "match":
        return {
            "query": {
                "match": {
                    "text": query_text,
                }
            },
            "_source": ["id", "text"],
            "size": 15,
        }
    else:
        raise ValueError(f"Invalid query type: {query_type}")


def search_query(
    client, index_name, query_item, embedding_field, query_type="neural_sparse"
):
    """
    Execute search query for a single query

    Args:
        client: OpenSearch client
        index_name: Name of the index to search
        query_item: Tuple of (query_id, query_text)
        embedding_field: Field name for embedding

    Returns:
        tuple: (query_id, scores dict)
    """
    query_id, query_text = query_item
    query_body = create_query_body(query_text, query_type, embedding_field)
    response = client.search(index=index_name, body=query_body)

    hits = response["hits"]["hits"]
    scores = {hit["_source"]["id"]: hit["_score"] for hit in hits}
    return query_id, scores


def evaluate_search_relevance(
    client,
    index_name,
    queries,
    qrels,
    embedding_field,
    max_workers,
    query_type="neural_sparse",
):
    """
    Evaluate search relevance using BEIR evaluation metrics

    Args:
        client: OpenSearch client
        index_name: Name of the index to search
        queries: Dictionary of queries
        qrels: Dictionary of relevance labels
        embedding_field: Field name for embedding
        max_workers: Number of concurrent workers

    Returns:
        tuple: (ndcg, map_, recall, precision)
    """
    results = {}

    # Execute searches in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                search_query,
                client,
                index_name,
                item,
                embedding_field,
                query_type,
            )
            for item in queries.items()
        ]
        for future in tqdm(futures, total=len(futures), desc="Executing searches"):
            query_id, response = future.result()
            results[query_id] = response

    # Evaluate using BEIR metrics
    ndcg, map_, recall, precision = EvaluateRetrieval.evaluate(qrels, results, [10])

    return ndcg, map_, recall, precision


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Evaluate search relevance using neural sparse search"
    )
    parser.add_argument(
        "--queries_file", type=str, required=True, help="Path to queries JSON file"
    )
    parser.add_argument(
        "--qrels_file", type=str, required=True, help="Path to qrels JSON file"
    )
    parser.add_argument(
        "--index_name", type=str, required=True, help="Name of the index to search"
    )
    parser.add_argument(
        "--embedding_field",
        type=str,
        default="embedding",
        help="Field name for embedding",
    )
    parser.add_argument(
        "--max_workers", type=int, default=20, help="Number of concurrent workers"
    )
    parser.add_argument(
        "--use_aws_auth", action="store_true", help="Whether to use AWS authentication"
    )
    parser.add_argument("--region", type=str, default="us-east-1", help="AWS region")
    parser.add_argument(
        "--query_type", type=str, default="neural_sparse", help="Query type"
    )
    args = parser.parse_args()
    print(args)

    try:
        # Initialize OpenSearch client
        client = get_os_client(use_aws_auth=args.use_aws_auth, region=args.region)

        # Load queries and qrels
        queries, qrels = load_queries_and_qrels(args.queries_file, args.qrels_file)
        print(f"Loaded {len(queries)} queries and {len(qrels)} qrels")

        # Evaluate search relevance
        ndcg, map_, recall, precision = evaluate_search_relevance(
            client=client,
            index_name=args.index_name,
            queries=queries,
            qrels=qrels,
            embedding_field=args.embedding_field,
            max_workers=args.max_workers,
            query_type=args.query_type,
        )

        # Print results
        print("\nEvaluation Results:")
        print(f"NDCG@10: {ndcg['NDCG@10']}")

    except KeyboardInterrupt:
        print("\nEvaluation interrupted by user")
    except Exception as e:
        print(f"Evaluation failed: {e}")
