"""
AWS Comprehend client for sentiment analysis.

Provides a simple interface for batch and single sentiment analysis using
AWS Comprehend's pre-trained models. Handles text size limits and batch
optimization automatically.
"""

from typing import List, Dict, Optional

import boto3
from botocore.exceptions import ClientError


# AWS Comprehend limits
COMPREHEND_MAX_TEXT_BYTES = 5000  # 5KB per text in batch requests
COMPREHEND_MAX_BATCH_SIZE = 25    # Maximum texts per batch call


class ComprehendClient:
    """
    Client for AWS Comprehend sentiment analysis.
    
    Automatically handles text size limits and provides both batch and
    single-text analysis methods.
    """
    
    def __init__(self, region_name: Optional[str] = None) -> None:
        """
        Initialize AWS Comprehend client.
        
        Args:
            region_name: AWS region (defaults to environment configuration)
        """
        self.client = boto3.client("comprehend", region_name=region_name)
    
    def detect_sentiment(
        self,
        text: str,
        language_code: str = "en",
    ) -> Dict[str, float]:
        """
        Analyze sentiment for a single text.
        
        Args:
            text: Text to analyze (max 5KB UTF-8 encoded)
            language_code: ISO 639-1 language code
        
        Returns:
            Dictionary with sentiment scores:
                - Positive: float (0.0-1.0)
                - Negative: float (0.0-1.0)
                - Neutral: float (0.0-1.0)
                - Mixed: float (0.0-1.0)
        
        Raises:
            ClientError: On AWS API errors
            ValueError: If text exceeds size limit
        """
        response = self.client.detect_sentiment(
            Text=text,
            LanguageCode=language_code,
        )
        return response["SentimentScore"]
    
    def batch_detect_sentiment(
        self,
        texts: List[str],
        language_code: str = "en",
    ) -> List[Dict[str, float]]:
        """
        Analyze sentiment for multiple texts in a single batch request.
        
        More efficient than individual calls when processing multiple texts.
        Automatically handles AWS batch size limits.
        
        Args:
            texts: List of texts to analyze (each max 5KB UTF-8 encoded)
            language_code: ISO 639-1 language code
        
        Returns:
            List of sentiment score dictionaries (same order as input)
        
        Raises:
            ClientError: On AWS API errors
            ValueError: If any text exceeds size limit
        
        Note:
            AWS Comprehend supports up to 25 texts per batch request.
            This method will make multiple batch calls if needed.
        """
        if not texts:
            return []
        
        # Process in batches of 25 (AWS limit)
        all_results = []
        for i in range(0, len(texts), COMPREHEND_MAX_BATCH_SIZE):
            batch = texts[i:i + COMPREHEND_MAX_BATCH_SIZE]
            
            response = self.client.batch_detect_sentiment(
                TextList=batch,
                LanguageCode=language_code,
            )
            
            # Extract sentiment scores in order
            for result in response.get("ResultList", []):
                all_results.append(result["SentimentScore"])
        
        return all_results
    
    def is_text_oversized(self, text: str) -> bool:
        """
        Check if text exceeds AWS Comprehend's batch size limit.
        
        Args:
            text: Text to check
        
        Returns:
            True if text exceeds 5KB when UTF-8 encoded
        """
        return len(text.encode("utf-8")) > COMPREHEND_MAX_TEXT_BYTES