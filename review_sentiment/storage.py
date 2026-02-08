"""
Temporary file management for workflow activities.

ARCHITECTURAL DECISION: Why Use Temp Files?
============================================

This workflow uses the file system as durable storage between activities
to enable independent retry policies for fetch vs. score operations.

ALTERNATIVES CONSIDERED:

1. Single fetch-and-score activity:
   ✓ Simpler (no file I/O dependency)
   ✗ Couples failure domains (Walmart API + AWS Comprehend)
   ✗ Retry wastes quota on both services

2. Return reviews directly from activity:
   ✓ No file system dependency
   ✗ Bloats Temporal's event history (anti-pattern)
   ✗ Event history is for small state/metadata, not bulk data
   ✗ Large payloads (100 pages × 10KB = 1MB+) cause performance issues

3. S3/object storage:
   ✓ Better horizontal scaling
   ✗ Adds network I/O and external dependency
   ✗ Overkill for this exercise

TRADEOFFS:
This approach prioritizes proper use of Temporal's event history and
independent retry policies over simplicity. It's appropriate for this
sequential processing demo but has limitations:
- Requires disk space and cleanup logic
- Local storage limits horizontal scaling
- More code than single-activity approach

For production at scale (1000+ reviews), consider S3 or object storage.
"""
import os
import json
from typing import Any, Dict

import aiofiles
import aiofiles.os


class FileManager:
    """
    Manages temporary file operations for workflow activities.
    
    Provides atomic writes and automatic cleanup for durability between
    activity executions. Files are named per workflow run and page to
    enable future concurrent processing.
    """
    
    def __init__(self, temp_path: str, run_id: str) -> None:
        """
        Args:
            temp_path: Base directory for temporary files
            run_id: Unique workflow execution identifier
        """
        self.temp_path = temp_path
        self.run_id = run_id
    
    def get_file_path(self, page: int) -> str:
        """
        Get the file path for a specific review page.
        
        Args:
            page: Page number
            
        Returns:
            Absolute path to the page's JSON file
        """
        return os.path.join(
            self.temp_path,
            f"reviews_{self.run_id}_{page}.json"
        )
    
    async def write_atomic(self, page: int, data: Dict[str, Any]) -> None:
        """
        Atomically write JSON data to prevent partial write corruption.
        
        Uses staging file + os.replace() to ensure the file is either
        fully written or not present at all (no partial corruption).
        
        Args:
            page: Page number for file naming
            data: JSON-serializable data to write
            
        Raises:
            Exception: Any file I/O error during write
        """
        temp_file = self.get_file_path(page)
        staging_file = f"{temp_file}.tmp"
        
        # Write to staging file asynchronously
        async with aiofiles.open(staging_file, "w", encoding="utf-8") as f:
            await f.write(json.dumps(data))
        
        # Atomic swap guarantees all-or-nothing semantics, preventing partial
        # file writes and data corruption if the worker is interrupted
        await aiofiles.os.replace(staging_file, temp_file)
    
    async def read(self, page: int) -> Dict[str, Any]:
        """
        Read JSON data from a page file.
        
        Args:
            page: Page number for file naming
            
        Returns:
            Parsed JSON data as dictionary
            
        Raises:
            Exception: If file doesn't exist or JSON is corrupt
        """
        temp_file = self.get_file_path(page)
        
        async with aiofiles.open(temp_file, "r", encoding="utf-8") as f:
            content = await f.read()
            return json.loads(content)
    
    async def delete(self, page: int) -> None:
        """
        Delete a page file. Silently succeeds if file doesn't exist.
        
        Args:
            page: Page number for file naming
        """
        temp_file = self.get_file_path(page)
        
        try:
            await aiofiles.os.remove(temp_file)
        except FileNotFoundError:
            # Already deleted - not an error
            pass