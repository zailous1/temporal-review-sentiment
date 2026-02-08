"""
Temporal activities for fetching and scoring Walmart product reviews.

Activities are the building blocks of Temporal workflows. They perform
the actual work (API calls, file I/O, AWS operations) and can be retried
independently with configurable policies.

DESIGN CHOICES
===============

Walmart API vs Web Scraping:
-----------------------------
The exercise suggested building a pipeline that "scrapes" reviews. However,
I opted to use Walmart's public Product API instead of web scraping because:

- Web scraping is notoriously difficult (anti-bot measures, CAPTCHAs, dynamic
  rendering, DOM parsing, rate limiting, legal gray areas)
- These challenges are tangential to learning Temporal's execution model
- Walmart is one of the few major retailers offering a public reviews API
- Using a simple, stable API lets us focus on Temporal patterns rather than
  scraping infrastructure

This aligns with the spirit of the exercise: demonstrating Temporal concepts,
not building production-grade web scraping infrastructure.

AWS Comprehend vs LLM Inference:
---------------------------------
For sentiment analysis, I deliberately chose AWS Comprehend over LLM-based
approaches (local models, OpenAI, Anthropic, etc.) because:

- Purpose-built: Comprehend is specifically designed for sentiment analysis
- Performance: Processes batches in milliseconds vs seconds for LLM inference
- Cost: ~$0.0001 per text vs $0.001-0.01 per LLM call (10-100x cheaper)
- Deterministic: Consistent scores without prompt engineering variability
- Simple: No prompt engineering, model selection, or temperature tuning

While LLMs offer richer analysis (e.g., extracting specific themes), Comprehend
provides exactly what we need - fast, cheap, reliable sentiment scoring - making
it the right tool for this use case.

Alternative: For a more complex approach using various LLM providers, we could
consider the LiteLLM library (https://github.com/BerriAI/litellm) which provides
a unified interface for Anthropic, OpenAI, Cohere, and 100+ other providers.
"""

import os
from typing import Dict

from temporalio import activity
from temporalio.exceptions import ApplicationError

from review_sentiment.walmart_client.api import WalmartAPI
from review_sentiment.aws_client.comprehend import ComprehendClient
from review_sentiment.storage import FileManager
from review_sentiment.models import (
    GetReviewsInput,
    ScoreReviewsInput,
    ReviewPageMeta,
)


def sentiment_to_score(sentiment_scores: Dict[str, float]) -> float:
    """
    Convert AWS Comprehend sentiment scores to a 1-5 scale.
    
    Weights:
    - Positive: 5.0 (best)
    - Negative: 1.0 (worst)
    - Neutral/Mixed: 3.0 (middle)
    
    Example:
        >>> sentiment_to_score({"Positive": 0.9, "Negative": 0.1, "Neutral": 0.0, "Mixed": 0.0})
        4.6  # (5.0 * 0.9) + (1.0 * 0.1)
    
    Args:
        sentiment_scores: Dict with Positive, Negative, Neutral, Mixed keys (0.0-1.0)
        
    Returns:
        Weighted score from 1.0 to 5.0
    """
    return (
        5.0 * sentiment_scores.get("Positive", 0.0)
        + 1.0 * sentiment_scores.get("Negative", 0.0)
        + 3.0 * sentiment_scores.get("Neutral", 0.0)
        + 3.0 * sentiment_scores.get("Mixed", 0.0)
    )


@activity.defn
async def get_reviews(input: GetReviewsInput) -> ReviewPageMeta:
    """
    Fetch one page of reviews from Walmart API and persist to disk.
    
    This activity separates data fetching from processing to enable
    independent retry policies. If fetching fails (rate limits, network),
    we can retry without wasting AWS Comprehend quota.
    
    Args:
        input: Activity input parameters
        
    Returns:
        Metadata about the fetched page
        
    Raises:
        ApplicationError: Non-retryable if file system fails
    """
    walmart_api = WalmartAPI(
        consumer_id=os.environ["WM_CONSUMER_ID"],
        key_version=os.environ["WM_KEY_VERSION"],
        private_key_path=os.environ["WM_PRIVATE_KEY_PATH"],
        api_base=os.environ["WM_API_BASE"],
    )

    review_page = await walmart_api.get_reviews(input.product_id, f"page={input.page}")

    # Persist to disk for durability between activities
    file_manager = FileManager(input.temp_path, input.run_id)

    try:
        await file_manager.write_atomic(input.page, review_page)
    except Exception as e:
        # Non-retryable: file system full/corrupted, or permissions issue
        raise ApplicationError(
            f"Failed to persist review page for product={input.product_id}, page={input.page}",
            non_retryable=True,
        ) from e

    reviews = review_page.get("reviews") or []
    
    # Extract total review count from reviewStatistics
    review_statistics = review_page.get("reviewStatistics") or {}
    total_review_count_raw = review_statistics.get("totalReviewCount")

    # Ensure it's an integer
    total_review_count = int(total_review_count_raw) if total_review_count_raw is not None else None    

    return ReviewPageMeta(
        name=review_page.get("name"),
        sale_price=review_page.get("salePrice"),
        count=len(reviews),
        has_next=bool(review_page.get("nextPage")),
        total_review_count=total_review_count,
    )


@activity.defn
async def score_reviews(input: ScoreReviewsInput) -> float:
    """
    Score sentiment for one persisted review page using AWS Comprehend.
    
    Reads reviews from disk (written by get_reviews activity), analyzes
    sentiment, and returns the average score. Separating this from fetching
    enables independent retry policies for AWS API failures.
    
    Optimizations:
    - Batch processing for reviews under 5KB (AWS Comprehend limit)
    - Single calls for oversized reviews
    - Handles up to 10 reviews per page (Walmart API page size)
    
    Args:
        input: Activity input parameters
        
    Returns:
        Average sentiment score (1.0-5.0), or 0.0 if no valid reviews
        
    Raises:
        ApplicationError: 
            - Non-retryable if file is missing/corrupt
            - Retryable if AWS Comprehend fails (throttling, network)
    """
    file_manager = FileManager(input.temp_path, input.run_id)
    comprehend_client = ComprehendClient()

    # Load persisted review data
    try:
        review_page = await file_manager.read(input.page)
    except Exception as e:
        # Non-retryable: file corruption or missing (data loss)
        raise ApplicationError(
            f"Failed to load review page for product={input.product_id}, page={input.page}",
            non_retryable=True,
        ) from e

    try:
        reviews = review_page.get("reviews") or []
        if not reviews:
            return 0.0

        scores: list[float] = []
        batch_texts: list[str] = []

        # Separate oversized texts from batch-eligible texts
        for review in reviews:
            text = review.get("reviewText") if isinstance(review, dict) else None

            if not isinstance(text, str) or not text.strip():
                continue

            if comprehend_client.is_text_oversized(text):
                # Oversized review → individual API call
                sentiment_scores = comprehend_client.detect_sentiment(text, input.language_code)
                scores.append(sentiment_to_score(sentiment_scores))
            else:
                batch_texts.append(text)

        # Batch process remaining reviews (more efficient)
        if batch_texts:
            batch_results = comprehend_client.batch_detect_sentiment(batch_texts, input.language_code)
            for sentiment_scores in batch_results:
                scores.append(sentiment_to_score(sentiment_scores))

    except Exception as e:
        # Retryable: AWS throttling, network issues, transient failures
        raise ApplicationError(
            f"Failed to score reviews for product={input.product_id}, page={input.page}",
            non_retryable=False,
        ) from e
    finally:
        # Clean up temp file regardless of success/failure
        await file_manager.delete(input.page)

    if not scores:
        return 0.0

    avg_score = sum(scores) / len(scores)
    activity.logger.info(f"Scored page {input.page}: {avg_score:.3f} (from {len(scores)} reviews)")
    return avg_score