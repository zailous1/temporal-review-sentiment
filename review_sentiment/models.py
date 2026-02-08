"""
Data models for workflow execution and activity parameters.

Following Temporal best practices, activities and workflows use single
dataclass parameters for better versioning and backward compatibility.
This allows adding optional fields in the future without breaking existing
workflow executions.
"""

from dataclasses import dataclass
from typing import Optional


# Activity Parameters
# -------------------

@dataclass(frozen=True)
class GetReviewsInput:
    """
    Input parameters for the get_reviews activity.
    
    Attributes:
        run_id: Unique workflow execution ID (for temp file naming)
        product_id: Walmart product identifier
        page: Page number (1-indexed)
        temp_path: Directory for temporary file storage
    """
    run_id: str
    product_id: str
    page: int
    temp_path: str


@dataclass(frozen=True)
class ScoreReviewsInput:
    """
    Input parameters for the score_reviews activity.
    
    Attributes:
        run_id: Unique workflow execution ID (for temp file naming)
        product_id: Walmart product identifier
        page: Page number (1-indexed)
        temp_path: Directory for temporary file storage
        language_code: ISO 639-1 language code for sentiment analysis
    """
    run_id: str
    product_id: str
    page: int
    temp_path: str
    language_code: str = "en"


# Activity Return Types
# ---------------------

@dataclass(frozen=True)
class ReviewPageMeta:
    """
    Metadata extracted from a page of Walmart reviews.
    
    Returned by get_reviews activity to provide workflow with
    pagination info without passing bulk review data through
    Temporal's event history.
    
    Attributes:
        name: Product name
        sale_price: Current sale price in USD
        count: Number of reviews on this page
        has_next: Whether more pages are available
        total_review_count: Total reviews available for this product
    """
    name: Optional[str]
    sale_price: Optional[float]
    count: int
    has_next: bool
    total_review_count: Optional[int] = None


# Workflow Parameters
# -------------------

@dataclass(frozen=True)
class ReviewSentimentInput:
    """
    Input parameters for the ReviewSentiment workflow.
    
    Attributes:
        run_id: Unique workflow execution ID (for temp file naming)
        product_id: Walmart product identifier
        temp_path: Directory for temporary file storage
        max_reviews: Maximum number of reviews to process (None = all available)
    """
    run_id: str
    product_id: str
    temp_path: str
    max_reviews: Optional[int] = None


# Workflow Return Types
# ---------------------

@dataclass(frozen=False)
class ReviewSentimentResult:
    """
    Final result of sentiment analysis workflow.
    
    Note: frozen=False allows workflow to update review_count and avg_sentiment
    fields after processing completes.
    
    Attributes:
        name: Product name
        sale_price: Current sale price in USD
        url: Direct link to Walmart product page
        review_count: Total number of reviews analyzed
        avg_sentiment: Average sentiment score (1.0-5.0)
    """
    name: Optional[str]
    sale_price: Optional[float]
    url: Optional[str]
    review_count: Optional[int]
    avg_sentiment: Optional[float]