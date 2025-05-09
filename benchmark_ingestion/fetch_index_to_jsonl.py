from opensearchpy import OpenSearch
import json
import time

SCROLL_TIME = "5m"
BATCH_SIZE = 1000
OUTPUT_FILE = f"test.jsonl"
index = "marco"
endpoint = "localhost:9200"


def export_to_jsonl():
    client = OpenSearch(endpoint)

    # Initialize scroll
    try:
        # Get the initial scroll ID
        result = client.search(
            index=index,
            scroll=SCROLL_TIME,
            size=BATCH_SIZE,
            body={"query": {"match_all": {}}},
        )
        scroll_id = result["_scroll_id"]
        hits = result["hits"]["hits"]

        # Counter for progress tracking
        total_docs = result["hits"]["total"]["value"]
        processed_docs = 0
        start_time = time.time()

        print(f"Total documents to export: {total_docs}")

        # Open file and write documents
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
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
                result = client.scroll(scroll_id=scroll_id, scroll=SCROLL_TIME)
                scroll_id = result["_scroll_id"]
                hits = result["hits"]["hits"]

        # Final progress update
        elapsed_time = time.time() - start_time
        docs_per_second = processed_docs / elapsed_time
        print(f"\nExport completed!")
        print(f"Total documents exported: {processed_docs}")
        print(f"Total time: {elapsed_time:.2f} seconds")
        print(f"Average speed: {docs_per_second:.2f} docs/sec")
        print(f"Output file: {OUTPUT_FILE}")

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
    try:
        export_to_jsonl()
    except KeyboardInterrupt:
        print("\nExport interrupted by user")
    except Exception as e:
        print(f"Export failed: {e}")
