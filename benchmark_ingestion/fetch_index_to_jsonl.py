import json
import time
import argparse
import os
from utils import get_os_client


def export_to_jsonl(client, index_name, output_file, scroll_time="5m", batch_size=1000):
    """
    Export all documents from an OpenSearch index to a JSONL file

    Args:
        client: OpenSearch client instance
        index_name: Name of the index to export
        output_file: Path to output JSONL file
        scroll_time: Scroll timeout
        batch_size: Number of documents per batch
    """
    # Initialize scroll
    try:
        # Get the initial scroll ID
        result = client.search(
            index=index_name,
            scroll=scroll_time,
            size=batch_size,
            body={"query": {"match_all": {}}},
        )
        scroll_id = result["_scroll_id"]
        hits = result["hits"]["hits"]

        # Counter for progress tracking
        total_docs = result["hits"]["total"]["value"]
        processed_docs = 0
        start_time = time.time()

        print(f"Total documents to export: {total_docs}")

        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        # Open file and write documents
        with open(output_file, "w", encoding="utf-8") as f:
            while hits:
                # Process current batch
                for hit in hits:
                    # Write document to file
                    f.write(json.dumps(hit["_source"]) + "\n")
                    processed_docs += 1

                # Print progress
                if processed_docs % 10000 == 0:
                    elapsed_time = time.time() - start_time
                    docs_per_second = processed_docs / elapsed_time
                    print(
                        f"Processed {processed_docs}/{total_docs} documents "
                        f"({(processed_docs/total_docs*100):.2f}%) "
                        f"- {docs_per_second:.2f} docs/sec"
                    )

                # Get next batch of results
                result = client.scroll(scroll_id=scroll_id, scroll=scroll_time)
                scroll_id = result["_scroll_id"]
                hits = result["hits"]["hits"]

        # Final progress update
        elapsed_time = time.time() - start_time
        docs_per_second = processed_docs / elapsed_time
        print(f"\nExport completed!")
        print(f"Total documents exported: {processed_docs}")
        print(f"Total time: {elapsed_time:.2f} seconds")
        print(f"Average speed: {docs_per_second:.2f} docs/sec")
        print(f"Output file: {output_file}")

    except Exception as e:
        print(f"Error during export: {e}")
        raise
    finally:
        # Clear scroll
        try:
            client.clear_scroll(scroll_id=scroll_id)
        except:
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export OpenSearch index to JSONL file"
    )
    parser.add_argument(
        "--index_name", type=str, required=True, help="Name of the index to export"
    )
    parser.add_argument(
        "--output_file", type=str, required=True, help="Path to output JSONL file"
    )
    parser.add_argument("--scroll_time", type=str, default="5m", help="Scroll timeout")
    parser.add_argument(
        "--batch_size", type=int, default=1000, help="Number of documents per batch"
    )
    parser.add_argument(
        "--use_aws_auth", action="store_true", help="Whether to use AWS authentication"
    )
    parser.add_argument("--region", type=str, default="us-east-1", help="AWS region")

    args = parser.parse_args()
    print(args)

    try:
        # Initialize OpenSearch client
        client = get_os_client(use_aws_auth=args.use_aws_auth, region=args.region)

        # Export index to JSONL
        export_to_jsonl(
            client=client,
            index_name=args.index_name,
            output_file=args.output_file,
            scroll_time=args.scroll_time,
            batch_size=args.batch_size,
        )
    except KeyboardInterrupt:
        print("\nExport interrupted by user")
    except Exception as e:
        print(f"Export failed: {e}")
