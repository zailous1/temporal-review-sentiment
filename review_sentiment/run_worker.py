"""
Temporal worker process for the review sentiment analysis workflow.

Workers poll the Temporal server for tasks and execute workflow and activity
code. This worker handles both the ReviewSentiment workflow and its activities
(get_reviews, score_reviews).

To run:
    python -m review_sentiment.run_worker

Prerequisites:
    - Temporal server running on localhost:7233
    - Environment variables configured in .env (see .env.example)
    - AWS credentials configured for Comprehend access
"""

import asyncio
from dotenv import load_dotenv

from temporalio.client import Client
from temporalio.worker import Worker

from review_sentiment.activities import get_reviews, score_reviews
from review_sentiment.workflows import ReviewSentiment

# Load environment variables from .env file
load_dotenv()

# Task queue name shared between worker and workflow execution
TASK_QUEUE = "review-sentiment-analysis"

# Temporal server connection (default local development setup)
TEMPORAL_HOST = "localhost:7233"


async def main() -> None:
    """
    Start the Temporal worker and begin polling for tasks.
    
    The worker will run indefinitely until interrupted (Ctrl+C) or the
    process is terminated.
    """
    # Connect to Temporal server
    client = await Client.connect(TEMPORAL_HOST)

    # Create worker with workflow and activity registrations
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[ReviewSentiment],
        activities=[get_reviews, score_reviews],
    )
    
    # Run worker (blocks until shutdown signal)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())