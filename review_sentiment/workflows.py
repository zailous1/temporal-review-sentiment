"""
Temporal workflow for sentiment analysis of Walmart product reviews.

Workflows are the orchestration layer in Temporal - they coordinate activities,
manage state durably, and survive worker restarts. Workflow code must be
deterministic (no random numbers, current time, or direct I/O).

For design rationale on:
- API choice and sentiment analysis approach: See activities.py module docstring
- Temp file storage between activities: See storage.py module docstring
"""

from datetime import timedelta
from typing import Optional

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from review_sentiment.activities import get_reviews, score_reviews
    from review_sentiment.models import (
        GetReviewsInput,
        ScoreReviewsInput,
        ReviewPageMeta,
        ReviewSentimentInput,
        ReviewSentimentResult,
    )

# Walmart product page URL template
WALMART_PRODUCT_URL_BASE = "https://www.walmart.com/ip"


@workflow.defn
class ReviewSentiment:
    """
    Analyzes sentiment of Walmart product reviews using AWS Comprehend.
    
    This workflow fetches reviews page-by-page, scores each page's sentiment,
    and computes a weighted average across all reviews. State is maintained
    durably in Temporal's event history, enabling the workflow to resume after
    worker crashes or restarts.
    
    Supports runtime control via signals:
    - pause: Temporarily halt processing (useful during maintenance)
    - resume: Continue processing after pause
    - Request cancellation via Temporal's standard cancel mechanism
    
    Architecture Decisions:
    -----------------------
    Sequential page processing: Fetches and scores one page at a time for
    simplicity and natural backpressure. For production scale (1000+ reviews),
    this could be replaced with batched concurrent fetching (10-20 pages at a
    time) or child workflows for independent retry policies per page.
    
    Separate activities: Fetch and score are split into two activities to enable
    independent retry policies (see activities.py for detailed rationale on
    activity separation and temp file storage approach).
    
    Weighted average: Maintains running sum rather than storing all individual
    scores to keep workflow state compact (important for Temporal's event history).
    """

    def __init__(self) -> None:
        """Initialize workflow state for query and signal support."""
        self._current_page = 0
        self._total_reviews = 0
        self._weighted_score_sum = 0.0
        self._product_name: Optional[str] = None
        self._is_paused = False
        self._total_review_count: Optional[int] = None 

    @workflow.signal
    def pause(self) -> None:
        """
        Signal to pause workflow execution.
        
        Workflow will complete the current page and then wait until resumed.
        Useful for maintenance windows or rate limit management.
        """
        self._is_paused = True
        workflow.logger.info("Workflow paused by signal")

    @workflow.signal
    def resume(self) -> None:
        """
        Signal to resume paused workflow execution.
        
        Workflow will continue processing from where it was paused.
        """
        self._is_paused = False
        workflow.logger.info("Workflow resumed by signal")

    @workflow.query
    def get_progress(self) -> dict:
        """
        Query current workflow progress.
        
        Allows external monitoring of workflow state without waiting for
        completion. Useful for long-running workflows processing 1000+ reviews.
        
        Returns:
            Dictionary with current progress:
                - current_page: Page currently being processed
                - reviews_processed: Total reviews analyzed so far
                - total_review_count: Total reviews available for this product
                - current_avg_sentiment: Running average (or None if no reviews yet)
                - product_name: Product name (or None if not yet fetched)
        """
        return {
            "current_page": self._current_page,
            "reviews_processed": self._total_reviews,
            "total_review_count": self._total_review_count,
            "current_avg_sentiment": (
                self._weighted_score_sum / self._total_reviews
                if self._total_reviews > 0
                else None
            ),
            "product_name": self._product_name,
        }

    @workflow.run
    async def run(self, input: ReviewSentimentInput) -> ReviewSentimentResult:
        """
        Analyze sentiment across product reviews up to max_reviews limit.
        
        Args:
            input: Workflow input parameters
            
        Returns:
            Aggregated sentiment analysis result with product metadata
        """

        # Retry policy for Walmart API calls (external third-party service)
        # Conservative approach to avoid rate limiting and account throttling
        walmart_retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),      # Start with 2s delay (respectful of rate limits)
            maximum_interval=timedelta(minutes=5),      # Cap at 5 minutes for severe throttling
            backoff_coefficient=3.0,                     # Aggressive backoff (2s → 6s → 18s → 54s)
            maximum_attempts=15,                         # More attempts (API might be temporarily down)
        )

        # Retry policy for AWS Comprehend calls (our own AWS account)
        # Aggressive approach since we control the service and have quota visibility
        comprehend_retry_policy = RetryPolicy(
            initial_interval=timedelta(milliseconds=500), # Start fast (500ms) for transient errors
            maximum_interval=timedelta(seconds=30),       # Cap at 30s (shorter than Walmart)
            backoff_coefficient=2.0,                      # Standard exponential backoff
            maximum_attempts=8,                           # Fewer attempts (we can monitor/fix quota)
        )

        # Durable workflow state - survives worker restarts
        # Using instance variables for query support
        self._current_page = 1
        self._total_reviews = 0
        self._weighted_score_sum = 0.0
        workflow_result: Optional[ReviewSentimentResult] = None

        # Sequential processing for simplicity. Production at scale would fetch
        # multiple pages concurrently (Walmart API provides total count upfront).
        while True:
            # Check for pause signal - wait until resumed
            await workflow.wait_condition(lambda: not self._is_paused)

            # Fetch one page of reviews and persist to disk
            page_meta: ReviewPageMeta = await workflow.execute_activity(
                get_reviews,
                GetReviewsInput(
                    run_id=input.run_id,
                    product_id=input.product_id,
                    page=self._current_page,
                    temp_path=input.temp_path,
                ),
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=walmart_retry_policy,
            )

            page_count = page_meta.count

            # Score reviews if page is non-empty
            if page_count > 0:
                page_avg_score = await workflow.execute_activity(
                    score_reviews,
                    ScoreReviewsInput(
                        run_id=input.run_id,
                        product_id=input.product_id,
                        page=self._current_page,
                        temp_path=input.temp_path,
                    ),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=comprehend_retry_policy,
                )

                # Update running weighted average
                self._weighted_score_sum += page_avg_score * page_count
                self._total_reviews += page_count

            # Capture product metadata from first page
            if workflow_result is None:
                self._product_name = page_meta.name
                self._total_review_count = page_meta.total_review_count
                workflow_result = ReviewSentimentResult(
                    name=page_meta.name,
                    sale_price=page_meta.sale_price,
                    url=f"{WALMART_PRODUCT_URL_BASE}/{input.product_id}",
                    review_count=None,  # Set after processing complete
                    avg_sentiment=None,  # Set after processing complete
                )

            # Stop if we've reached the requested limit
            if input.max_reviews is not None and self._total_reviews >= input.max_reviews:
                break

            # Advance to next page (only after successful processing)
            self._current_page += 1

            # Stop if no more pages available
            if not page_meta.has_next:
                break

        # Compute final average and populate result
        avg_score = (
            self._weighted_score_sum / self._total_reviews
            if self._total_reviews > 0
            else 0.0
        )

        workflow_result.review_count = self._total_reviews
        workflow_result.avg_sentiment = avg_score

        return workflow_result