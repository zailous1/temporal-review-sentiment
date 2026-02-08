"""
Unit tests for Temporal activities.

Tests cover:
- Review fetching with mocked Walmart API responses
- Sentiment scoring with mocked AWS Comprehend responses
- File persistence and atomic writes
- Error handling and edge cases
"""

import json
import pytest
import tempfile
import os
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import timedelta

from review_sentiment.activities import (
    get_reviews,
    score_reviews,
    sentiment_to_score,
)
from review_sentiment.models import (
    GetReviewsInput,
    ScoreReviewsInput,
    ReviewPageMeta,
)
from review_sentiment.storage import FileManager


class TestSentimentToScore:
    """Test sentiment score conversion logic."""
    
    def test_perfect_positive(self):
        """100% positive sentiment should return 5.0."""
        scores = {"Positive": 1.0, "Negative": 0.0, "Neutral": 0.0, "Mixed": 0.0}
        assert sentiment_to_score(scores) == 5.0
    
    def test_perfect_negative(self):
        """100% negative sentiment should return 1.0."""
        scores = {"Positive": 0.0, "Negative": 1.0, "Neutral": 0.0, "Mixed": 0.0}
        assert sentiment_to_score(scores) == 1.0
    
    def test_perfect_neutral(self):
        """100% neutral sentiment should return 3.0."""
        scores = {"Positive": 0.0, "Negative": 0.0, "Neutral": 1.0, "Mixed": 0.0}
        assert sentiment_to_score(scores) == 3.0
    
    def test_mixed_sentiment(self):
        """Mixed sentiment should compute weighted average correctly."""
        scores = {"Positive": 0.6, "Negative": 0.2, "Neutral": 0.1, "Mixed": 0.1}
        expected = (5.0 * 0.6) + (1.0 * 0.2) + (3.0 * 0.1) + (3.0 * 0.1)
        assert sentiment_to_score(scores) == expected
        assert sentiment_to_score(scores) == 3.8


class TestGetReviewsActivity:
    """Test the get_reviews activity with mocked API calls."""
    
    @pytest.fixture
    def mock_walmart_api(self):
        """Mock WalmartAPI class."""
        with patch('review_sentiment.activities.WalmartAPI') as mock:
            yield mock
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir
    
    @pytest.fixture
    def sample_review_page(self):
        """Sample Walmart API response."""
        return {
            "name": "Test Product",
            "salePrice": 99.99,
            "reviews": [
                {"reviewText": "Great product!", "rating": 5},
                {"reviewText": "Not bad", "rating": 4},
            ],
            "reviewStatistics": {
                "totalReviewCount": 150
            },
            "nextPage": "page=2"
        }
    
    @pytest.mark.asyncio
    async def test_get_reviews_success(
        self, mock_walmart_api, temp_dir, sample_review_page
    ):
        """Test successful review fetching and persistence."""
        # Setup mock
        mock_instance = mock_walmart_api.return_value
        mock_instance.get_reviews = AsyncMock(return_value=sample_review_page)
        
        # Set environment variables
        os.environ.update({
            "WM_CONSUMER_ID": "test_id",
            "WM_KEY_VERSION": "1",
            "WM_PRIVATE_KEY_PATH": "/fake/path",
            "WM_API_BASE": "https://test.walmart.com/api"
        })
        
        # Execute activity
        input_params = GetReviewsInput(
            run_id="test-run-123",
            product_id="12345",
            page=1,
            temp_path=temp_dir
        )
        
        result = await get_reviews(input_params)
        
        # Verify result metadata
        assert isinstance(result, ReviewPageMeta)
        assert result.name == "Test Product"
        assert result.sale_price == 99.99
        assert result.count == 2
        assert result.has_next is True
        assert result.total_review_count == 150
        
        # Verify file was persisted
        file_path = os.path.join(temp_dir, "reviews_test-run-123_1.json")
        assert os.path.exists(file_path)
        
        # Verify file contents
        with open(file_path, 'r') as f:
            saved_data = json.load(f)
            assert saved_data == sample_review_page
    
    @pytest.mark.asyncio
    async def test_get_reviews_no_next_page(
        self, mock_walmart_api, temp_dir
    ):
        """Test handling of last page (no nextPage field)."""
        response = {
            "name": "Final Page Product",
            "salePrice": 49.99,
            "reviews": [{"reviewText": "Last review", "rating": 5}],
            "reviewStatistics": {"totalReviewCount": 10}
            # No nextPage field
        }
        
        mock_instance = mock_walmart_api.return_value
        mock_instance.get_reviews = AsyncMock(return_value=response)
        
        os.environ.update({
            "WM_CONSUMER_ID": "test_id",
            "WM_KEY_VERSION": "1",
            "WM_PRIVATE_KEY_PATH": "/fake/path",
            "WM_API_BASE": "https://test.walmart.com/api"
        })
        
        input_params = GetReviewsInput(
            run_id="test-run-456",
            product_id="67890",
            page=10,
            temp_path=temp_dir
        )
        
        result = await get_reviews(input_params)
        
        assert result.has_next is False
        assert result.count == 1


class TestScoreReviewsActivity:
    """Test the score_reviews activity with mocked AWS calls."""
    
    @pytest.fixture
    def mock_comprehend(self):
        """Mock ComprehendClient class."""
        with patch('review_sentiment.activities.ComprehendClient') as mock:
            yield mock
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir
    
    @pytest.fixture
    async def setup_review_file(self, temp_dir):
        """Create a test review file."""
        file_manager = FileManager(temp_dir, "test-run-789")
        review_data = {
            "reviews": [
                {"reviewText": "Excellent product, highly recommend!"},
                {"reviewText": "Terrible quality, broke immediately."},
                {"reviewText": "It's okay, nothing special."},
            ]
        }
        await file_manager.write_atomic(1, review_data)
        return file_manager
    
    @pytest.mark.asyncio
    async def test_score_reviews_batch_processing(
        self, mock_comprehend, temp_dir, setup_review_file
    ):
        """Test batch sentiment scoring with normal-sized reviews."""
        # setup_review_file fixture already created the file
        
        # Mock Comprehend client
        mock_instance = mock_comprehend.return_value
        mock_instance.is_text_oversized = MagicMock(return_value=False)
        mock_instance.batch_detect_sentiment = MagicMock(return_value=[
            {"Positive": 0.9, "Negative": 0.05, "Neutral": 0.05, "Mixed": 0.0},
            {"Positive": 0.1, "Negative": 0.8, "Neutral": 0.05, "Mixed": 0.05},
            {"Positive": 0.3, "Negative": 0.2, "Neutral": 0.5, "Mixed": 0.0},
        ])
        
        input_params = ScoreReviewsInput(
            run_id="test-run-789",
            product_id="12345",
            page=1,
            temp_path=temp_dir,
            language_code="en"
        )
        
        result = await score_reviews(input_params)
        
        # Verify batch was called
        mock_instance.batch_detect_sentiment.assert_called_once()
        
        # Calculate expected average
        score1 = sentiment_to_score({"Positive": 0.9, "Negative": 0.05, "Neutral": 0.05, "Mixed": 0.0})
        score2 = sentiment_to_score({"Positive": 0.1, "Negative": 0.8, "Neutral": 0.05, "Mixed": 0.05})
        score3 = sentiment_to_score({"Positive": 0.3, "Negative": 0.2, "Neutral": 0.5, "Mixed": 0.0})
        expected_avg = (score1 + score2 + score3) / 3
        
        assert result == pytest.approx(expected_avg, rel=1e-3)
        
        # Verify temp file was cleaned up
        file_path = os.path.join(temp_dir, "reviews_test-run-789_1.json")
        assert not os.path.exists(file_path)
    
    @pytest.mark.asyncio
    async def test_score_reviews_oversized_handling(
        self, mock_comprehend, temp_dir
    ):
        """Test handling of oversized reviews (>5KB)."""
        # Create file with mix of normal and oversized reviews
        file_manager = FileManager(temp_dir, "test-run-oversized")
        review_data = {
            "reviews": [
                {"reviewText": "Normal review"},
                {"reviewText": "x" * 10000},  # Oversized
                {"reviewText": "Another normal review"},
            ]
        }
        await file_manager.write_atomic(1, review_data)
        
        # Mock Comprehend client
        mock_instance = mock_comprehend.return_value
        
        def is_oversized(text):
            return len(text.encode('utf-8')) > 5000
        
        mock_instance.is_text_oversized = MagicMock(side_effect=is_oversized)
        mock_instance.detect_sentiment = MagicMock(return_value={
            "Positive": 0.5, "Negative": 0.3, "Neutral": 0.2, "Mixed": 0.0
        })
        mock_instance.batch_detect_sentiment = MagicMock(return_value=[
            {"Positive": 0.8, "Negative": 0.1, "Neutral": 0.1, "Mixed": 0.0},
            {"Positive": 0.7, "Negative": 0.2, "Neutral": 0.1, "Mixed": 0.0},
        ])
        
        input_params = ScoreReviewsInput(
            run_id="test-run-oversized",
            product_id="12345",
            page=1,
            temp_path=temp_dir,
            language_code="en"
        )
        
        result = await score_reviews(input_params)
        
        # Verify individual call for oversized text
        mock_instance.detect_sentiment.assert_called_once()
        
        # Verify batch call for normal texts
        mock_instance.batch_detect_sentiment.assert_called_once()
        
        # Result should be average of all three
        assert isinstance(result, float)
        assert 0.0 <= result <= 5.0
    
    @pytest.mark.asyncio
    async def test_score_reviews_empty_reviews(
        self, mock_comprehend, temp_dir
    ):
        """Test handling of empty review list."""
        file_manager = FileManager(temp_dir, "test-run-empty")
        review_data = {"reviews": []}
        await file_manager.write_atomic(1, review_data)
        
        input_params = ScoreReviewsInput(
            run_id="test-run-empty",
            product_id="12345",
            page=1,
            temp_path=temp_dir,
            language_code="en"
        )
        
        result = await score_reviews(input_params)
        
        assert result == 0.0
    
    @pytest.mark.asyncio
    async def test_score_reviews_invalid_text(
        self, mock_comprehend, temp_dir
    ):
        """Test handling of reviews with invalid/missing text."""
        file_manager = FileManager(temp_dir, "test-run-invalid")
        review_data = {
            "reviews": [
                {"reviewText": "Valid review"},
                {"reviewText": None},  # Invalid
                {"reviewText": ""},    # Empty
                {"rating": 5},         # Missing reviewText
                {"reviewText": "   "}, # Whitespace only
            ]
        }
        await file_manager.write_atomic(1, review_data)
        
        mock_instance = mock_comprehend.return_value
        mock_instance.is_text_oversized = MagicMock(return_value=False)
        mock_instance.batch_detect_sentiment = MagicMock(return_value=[
            {"Positive": 0.8, "Negative": 0.1, "Neutral": 0.1, "Mixed": 0.0},
        ])
        
        input_params = ScoreReviewsInput(
            run_id="test-run-invalid",
            product_id="12345",
            page=1,
            temp_path=temp_dir,
            language_code="en"
        )
        
        result = await score_reviews(input_params)
        
        # Should only process the one valid review
        assert mock_instance.batch_detect_sentiment.call_count == 1
        call_args = mock_instance.batch_detect_sentiment.call_args[0][0]
        assert len(call_args) == 1
        assert call_args[0] == "Valid review"


class TestFileManager:
    """Test file persistence utilities."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir
    
    @pytest.mark.asyncio
    async def test_atomic_write_success(self, temp_dir):
        """Test atomic file write operation."""
        file_manager = FileManager(temp_dir, "atomic-test")
        test_data = {"key": "value", "number": 42}
        
        await file_manager.write_atomic(1, test_data)
        
        # Verify file exists
        file_path = file_manager.get_file_path(1)
        assert os.path.exists(file_path)
        
        # Verify no staging file left behind
        staging_path = f"{file_path}.tmp"
        assert not os.path.exists(staging_path)
        
        # Verify contents
        with open(file_path, 'r') as f:
            loaded_data = json.load(f)
            assert loaded_data == test_data
    
    @pytest.mark.asyncio
    async def test_read_success(self, temp_dir):
        """Test reading persisted data."""
        file_manager = FileManager(temp_dir, "read-test")
        test_data = {"reviews": ["review1", "review2"]}
        
        await file_manager.write_atomic(5, test_data)
        loaded_data = await file_manager.read(5)
        
        assert loaded_data == test_data
    
    @pytest.mark.asyncio
    async def test_delete_success(self, temp_dir):
        """Test file deletion."""
        file_manager = FileManager(temp_dir, "delete-test")
        test_data = {"temp": "data"}
        
        await file_manager.write_atomic(3, test_data)
        file_path = file_manager.get_file_path(3)
        assert os.path.exists(file_path)
        
        await file_manager.delete(3)
        assert not os.path.exists(file_path)
    
    @pytest.mark.asyncio
    async def test_delete_nonexistent_file(self, temp_dir):
        """Test deleting non-existent file (should not raise)."""
        file_manager = FileManager(temp_dir, "delete-missing-test")
        
        # Should not raise exception
        await file_manager.delete(999)
    
    def test_get_file_path_format(self, temp_dir):
        """Test file path generation."""
        file_manager = FileManager(temp_dir, "path-test-123")
        
        path = file_manager.get_file_path(42)
        
        assert path.startswith(temp_dir)
        assert "reviews_path-test-123_42.json" in path
