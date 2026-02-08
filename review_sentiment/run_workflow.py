"""
CLI client for executing the review sentiment analysis workflow.

This script starts a workflow execution on the Temporal server and waits
for the result. The workflow must have a worker running to process it
(see run_worker.py).

Usage:
    python -m review_sentiment.run_workflow 14977205582
    python -m review_sentiment.run_workflow 14977205582 --max-reviews 50

Example output:
    ===========================================================
    Review Sentiment Analysis Result
    ============================================================
    Product Name  : Samsung Galaxy S25 Ultra 256GB Unlocked Android Cell Phone with 200MP Camera, Titanium Blue
    Sale Price    : $1049.99
    Product URL   : https://www.walmart.com/ip/14977205582
    Review Count  : 300
    Avg Sentiment : 3.962 / 5.0
    ============================================================
"""

import asyncio
import uuid
import tempfile
import argparse

from temporalio.client import Client

from review_sentiment.workflows import ReviewSentiment
from review_sentiment.models import ReviewSentimentInput, ReviewSentimentResult

# Default configuration
DEFAULT_MAX_REVIEWS = 10000
WALMART_REVIEWS_PER_PAGE = 10
TEMPORAL_HOST = "localhost:7233"
TASK_QUEUE = "review-sentiment-analysis"


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for workflow execution.
    
    Returns:
        Parsed arguments with product_id and max_reviews
        
    Raises:
        SystemExit: If max_reviews is not a multiple of 10
    """
    parser = argparse.ArgumentParser(
        description="Run the Review Sentiment Analysis workflow on Temporal",
        epilog="Example: python -m review_sentiment.run_workflow 14977205582 --max-reviews 50"
    )
    parser.add_argument(
        "product_id",
        help="Walmart product ID to analyze (e.g., 14977205582)",
    )
    parser.add_argument(
        "--max-reviews",
        type=int,
        default=DEFAULT_MAX_REVIEWS,
        help=f"Maximum number of reviews to process (default: {DEFAULT_MAX_REVIEWS})",
    )

    args = parser.parse_args()

    # Validate max_reviews is compatible with Walmart API pagination
    if args.max_reviews % WALMART_REVIEWS_PER_PAGE != 0:
        parser.error(
            f"--max-reviews must be a multiple of {WALMART_REVIEWS_PER_PAGE} "
            f"(Walmart API page size)"
        )

    return args


async def main(product_id: str, max_reviews: int) -> None:
    """
    Execute the review sentiment analysis workflow.
    
    Args:
        product_id: Walmart product identifier
        max_reviews: Maximum number of reviews to process
    """
    run_id = str(uuid.uuid4())
    temp_path = tempfile.gettempdir()

    print(f"Starting workflow execution...")
    print(f"  Run ID: {run_id}")
    print(f"  Product ID: {product_id}")
    print(f"  Max reviews: {max_reviews}")
    print(f"  Temp path: {temp_path}")
    print()

    # Connect to Temporal server
    client = await Client.connect(TEMPORAL_HOST)

    # Create workflow input
    workflow_input = ReviewSentimentInput(
        run_id=run_id,
        product_id=product_id,
        temp_path=temp_path,
        max_reviews=max_reviews,
    )

    # Execute workflow and wait for completion
    result = await client.execute_workflow(
        ReviewSentiment.run,
        workflow_input,
        id=run_id,
        task_queue=TASK_QUEUE, 
    )

    print_result(result)


def print_result(result: ReviewSentimentResult) -> None:
    """
    Pretty-print workflow execution results.
    
    Args:
        result: Completed workflow result with product metadata and sentiment score
    """
    print("=" * 60)
    print("Review Sentiment Analysis Result")
    print("=" * 60)
    print(f"Product Name  : {result.name or 'N/A'}")
    
    if result.sale_price is not None:
        print(f"Sale Price    : ${result.sale_price:.2f}")
    else:
        print(f"Sale Price    : N/A")
    
    print(f"Product URL   : {result.url or 'N/A'}")
    print(f"Review Count  : {result.review_count or 0}")
    
    if result.avg_sentiment is not None:
        print(f"Avg Sentiment : {result.avg_sentiment:.3f} / 5.0")
    else:
        print(f"Avg Sentiment : N/A")
    
    print("=" * 60 + "\n")


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main(args.product_id, args.max_reviews))