# Running Tests

## Installation

Install test dependencies:

```bash
pip install -e ".[dev]"
```

Or install directly:
```bash
pip install pytest pytest-asyncio
```

## Running Tests

**Run all tests**:
```bash
pytest
```

**Run specific test file**:
```bash
pytest tests/test_activities.py
pytest tests/test_workflows.py
```

**Run specific test class**:
```bash
pytest tests/test_activities.py::TestSentimentToScore
pytest tests/test_workflows.py::TestReviewSentimentWorkflow
```

**Run specific test method**:
```bash
pytest tests/test_activities.py::TestGetReviewsActivity::test_get_reviews_success
pytest tests/test_workflows.py::TestReviewSentimentWorkflow::test_workflow_completes_successfully
```

**Run with verbose output**:
```bash
pytest -v
```

**Run with output capture disabled** (see print statements):
```bash
pytest -s
```

**Run only fast tests** (exclude slow integration tests):
```bash
pytest -m "not slow"
```

## Test Coverage

The test suite covers:

### Activities (`test_activities.py`)
- Sentiment score conversion logic
- Review fetching with mocked Walmart API
- File persistence and atomic writes
- Sentiment scoring with mocked AWS Comprehend
- Batch processing and oversized text handling
- Edge cases (empty reviews, invalid text, missing fields)
- File manager utilities (read/write/delete)

### Workflows (`test_workflows.py`)
- Complete workflow execution through multiple pages
- Max reviews limit enforcement
- Running weighted average calculation
- Query functionality (progress monitoring)
- Signal handling (pause/resume)
- Pagination logic and termination
- Edge cases (empty pages, single page, varied counts)

## Test Structure

```
tests/
├── test_activities.py      # Activity unit tests with mocked external services
├── test_workflows.py        # Workflow tests using Temporal test framework
└── pytest.ini              # Pytest configuration
```

## Key Testing Patterns

**Mocking External Services**:
Tests mock Walmart API and AWS Comprehend to avoid external dependencies:

```python
@patch('review_sentiment.activities.WalmartAPI')
async def test_get_reviews_success(self, mock_walmart_api):
    mock_instance = mock_walmart_api.return_value
    mock_instance.get_reviews = AsyncMock(return_value=sample_data)
    # ... test logic
```

**Temporal Workflow Testing**:
Uses Temporal's test environment for deterministic workflow testing:

```python
async with await WorkflowEnvironment.start_time_skipping() as env:
    async with Worker(env.client, ...) as worker:
        result = await env.client.execute_workflow(...)
```

**Temporary File Handling**:
Tests use temporary directories that are automatically cleaned up:

```python
@pytest.fixture
def temp_dir(self):
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir
```

## Troubleshooting

**"ModuleNotFoundError: No module named 'pytest'"**
- Install test dependencies: `pip install pytest pytest-asyncio`

**"ImportError: cannot import name 'WorkflowEnvironment'"**
- Ensure temporalio is installed: `pip install temporalio`

**Async test warnings**
- Make sure `pytest-asyncio` is installed
- Tests are decorated with `@pytest.mark.asyncio`

**Environment variable errors in tests**
- Tests mock environment variables where needed
- No actual API credentials required for testing
