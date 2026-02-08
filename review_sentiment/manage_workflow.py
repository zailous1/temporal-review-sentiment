"""
Management CLI for interacting with running workflows.

Provides commands to query workflow state, cancel workflows, and monitor
progress. Useful for operational tasks and demonstrating Temporal's
interactive capabilities.

Usage:
    # Query workflow progress
    python -m review_sentiment.manage_workflow progress <workflow_id>
    
    # List recent workflows
    python -m review_sentiment.manage_workflow list
    
    # Pause a workflow
    python -m review_sentiment.manage_workflow pause <workflow_id>
    
    # Resume a paused workflow
    python -m review_sentiment.manage_workflow resume <workflow_id>
    
    # Cancel a workflow
    python -m review_sentiment.manage_workflow cancel <workflow_id>

Examples:
    python -m review_sentiment.manage_workflow progress 8f7d3c2a-1b4e-4f5a-9c8d-2e6f1a3b5c7d
    python -m review_sentiment.manage_workflow list --limit 10
    python -m review_sentiment.manage_workflow pause 8f7d3c2a-1b4e-4f5a-9c8d-2e6f1a3b5c7d
"""

import asyncio
import argparse

from temporalio.client import Client, WorkflowHandle

from review_sentiment.workflows import ReviewSentiment

# Temporal server connection
TEMPORAL_HOST = "localhost:7233"


async def query_progress(workflow_id: str) -> None:
    """
    Query and display current progress of a running workflow.
    
    Args:
        workflow_id: Workflow execution ID
    """
    client = await Client.connect(TEMPORAL_HOST)
    
    try:
        # Get workflow handle
        handle: WorkflowHandle = client.get_workflow_handle(workflow_id)
        
        # Query progress
        progress = await handle.query(ReviewSentiment.get_progress)
        
        # Display results
        print("=" * 60)
        print(f"Workflow Progress: {workflow_id}")
        print("=" * 60)
        print(f"Product Name       : {progress['product_name'] or 'Not yet fetched'}")
        print(f"Current Page       : {progress['current_page']}")
        print(f"Reviews Processed  : {progress['reviews_processed']}", end="")
        
        if progress['total_review_count'] is not None:
            print(f" / {progress['total_review_count']}")
        else:
            print()
        
        if progress['current_avg_sentiment'] is not None:
            print(f"Current Avg Score  : {progress['current_avg_sentiment']:.3f} / 5.0")
        else:
            print(f"Current Avg Score  : N/A (no reviews yet)")
        
        print("=" * 60 + "\n")
        
    except Exception as e:
        print(f"Error querying workflow: {e}")
        print(f"Workflow ID may be invalid or workflow may have completed.")


async def list_workflows(limit: int = 10) -> None:
    """
    List recent workflow executions.
    
    Args:
        limit: Maximum number of workflows to display
    """
    client = await Client.connect(TEMPORAL_HOST)
    
    try:
        # Query workflows (most recent first)
        workflows = client.list_workflows(
            f"WorkflowType = 'ReviewSentiment'"
        )
        
        print("=" * 100)
        print(f"{'Workflow ID':<40} {'Status':<15} {'Start Time':<25}")
        print("=" * 100)
        
        count = 0
        async for workflow in workflows:
            if count >= limit:
                break
            
            # Get status name directly from the status value
            status_name = workflow.status.name
            start_time = workflow.start_time.strftime("%Y-%m-%d %H:%M:%S") if workflow.start_time else "N/A"
            
            print(f"{workflow.id:<40} {status_name:<15} {start_time:<25}")
            count += 1
        
        print("=" * 100)
        print(f"\nShowing {count} workflow(s)")
        
    except Exception as e:
        print(f"Error listing workflows: {e}")


async def cancel_workflow(workflow_id: str) -> None:
    """
    Cancel a running workflow.
    
    Args:
        workflow_id: Workflow execution ID
    """
    client = await Client.connect(TEMPORAL_HOST)
    
    try:
        handle: WorkflowHandle = client.get_workflow_handle(workflow_id)
        
        # Request cancellation
        await handle.cancel()
        
        print(f"✓ Cancellation requested for workflow: {workflow_id}")
        print(f"  The workflow will stop gracefully at the next checkpoint.")
        
    except Exception as e:
        print(f"Error cancelling workflow: {e}")


async def pause_workflow(workflow_id: str) -> None:
    """
    Pause a running workflow.
    
    Args:
        workflow_id: Workflow execution ID
    """
    client = await Client.connect(TEMPORAL_HOST)
    
    try:
        handle: WorkflowHandle = client.get_workflow_handle(workflow_id)
        
        # Send pause signal - use string name
        await handle.signal("pause")
        
        print(f"✓ Pause signal sent to workflow: {workflow_id}")
        print(f"  The workflow will pause after completing the current page.")
        
    except Exception as e:
        print(f"Error pausing workflow: {e}")


async def resume_workflow(workflow_id: str) -> None:
    """
    Resume a paused workflow.
    
    Args:
        workflow_id: Workflow execution ID
    """
    client = await Client.connect(TEMPORAL_HOST)
    
    try:
        handle: WorkflowHandle = client.get_workflow_handle(workflow_id)
        
        # Send resume signal - use string name
        await handle.signal("resume")
        
        print(f"✓ Resume signal sent to workflow: {workflow_id}")
        print(f"  The workflow will continue processing.")
        
    except Exception as e:
        print(f"Error resuming workflow: {e}")


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Manage and monitor Temporal workflows",
        epilog="Example: python -m review_sentiment.manage_workflow progress <workflow_id>"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Progress command
    progress_parser = subparsers.add_parser(
        "progress",
        help="Query workflow progress"
    )
    progress_parser.add_argument(
        "workflow_id",
        help="Workflow execution ID"
    )
    
    # List command
    list_parser = subparsers.add_parser(
        "list",
        help="List recent workflows"
    )
    list_parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum number of workflows to display (default: 10)"
    )
    
    # Cancel command
    cancel_parser = subparsers.add_parser(
        "cancel",
        help="Cancel a running workflow"
    )
    cancel_parser.add_argument(
        "workflow_id",
        help="Workflow execution ID to cancel"
    )
    
    # Pause command
    pause_parser = subparsers.add_parser(
        "pause",
        help="Pause a running workflow"
    )
    pause_parser.add_argument(
        "workflow_id",
        help="Workflow execution ID to pause"
    )
    
    # Resume command
    resume_parser = subparsers.add_parser(
        "resume",
        help="Resume a paused workflow"
    )
    resume_parser.add_argument(
        "workflow_id",
        help="Workflow execution ID to resume"
    )
    
    return parser.parse_args()


async def main() -> None:
    """Main entry point for management CLI."""
    args = parse_args()
    
    if args.command == "progress":
        await query_progress(args.workflow_id)
    elif args.command == "list":
        await list_workflows(args.limit)
    elif args.command == "cancel":
        await cancel_workflow(args.workflow_id)
    elif args.command == "pause":
        await pause_workflow(args.workflow_id)
    elif args.command == "resume":
        await resume_workflow(args.workflow_id)
    else:
        print("Error: No command specified")
        print("Use --help for usage information")


if __name__ == "__main__":
    asyncio.run(main())