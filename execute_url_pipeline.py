# execute_url_pipeline_enhanced.py

import asyncio
import logging
import argparse
import json
import sys
import os
import traceback
from datetime import datetime
from typing import List, Dict, Any, Optional

# Load environment variables from .env file if present
try:
    from dotenv import load_dotenv
    load_dotenv()  # Load environment variables from .env file
    print("Loaded environment variables from .env file")
except ImportError:
    print("python-dotenv package not installed. Environment variables must be set manually.")

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import configuration
from config import *

# Import all pipeline components
from EventURLAgent import EventURLAgent
from EventImageAttacher import process_event_batch
from EventDataEnhancer import enhance_events
from EventValidationChecker import validate_events
from RuniuniJWTClient import RuniUniJWTClient
from EventEditorAgent import fix_invalid_events, enhance_event  # Import the new editor functions

# Configure logging based on settings in config
log_level = getattr(logging, LOG_LEVEL)
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Output to console
        logging.FileHandler(f"url_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")  # Output to file
    ]
)
logger = logging.getLogger('URLPipeline')

async def process_urls(
    urls: List[str],
    sources_file: Optional[str],
    client: RuniUniJWTClient,
    max_events: int = DEFAULT_EVENT_LIMIT,
    dry_run: bool = False,
    save_to_file: bool = SAVE_FILES,
    fix_invalid: bool = True
) -> Dict[str, Any]:
    """
    Process URLs through the entire pipeline.
    
    Args:
        urls: List of URLs to process
        sources_file: Optional file with additional URL sources
        client: RuniUni API client
        max_events: Maximum number of events to process
        dry_run: If True, don't actually post events to the API
        save_to_file: If True, save events to JSON files at each stage
        fix_invalid: If True, attempt to fix invalid events
        
    Returns:
        Dictionary with processing results
    """
    pipeline_start = datetime.now()
    
    # Generate a unique identifier for this batch
    batch_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    results = {
        "urls": urls,
        "sources_file": sources_file,
        "max_events": max_events,
        "dry_run": dry_run,
        "start_time": pipeline_start.isoformat(),
        "end_time": None,
        "duration_seconds": None,
        "events_extracted": 0,
        "events_with_images": 0,
        "events_enhanced": 0,
        "events_edited": 0,
        "events_valid": 0,
        "events_posted": 0,
        "events_failed": 0
    }
    
    try:
        # Step 1: Extract events from URLs
        logger.info(f"🔍 Step 1: Extracting events from URLs")
        url_count = len(urls) if urls else 0
        sources_info = f" and sources file {sources_file}" if sources_file else ""
        logger.info(f"Processing {url_count} URLs{sources_info}")
        
        # Initialize and run the URL agent
        agent = EventURLAgent(sources_file=sources_file, urls=urls)
        events = await agent.run()
        
        results["events_extracted"] = len(events)
        logger.info(f"Extracted {len(events)} events from URLs")
        
        if save_to_file:
            extracted_file = f"1_extracted_events_{batch_id}.json"
            with open(extracted_file, 'w') as f:
                json.dump(events, f, indent=2)
            logger.info(f"Saved extracted events to {extracted_file}")
        
        if not events:
            logger.warning(f"No events extracted from URLs, stopping pipeline")
            return results
        
        # Step 2: Limit events if needed
        if max_events and len(events) > max_events:
            logger.info(f"Limiting to {max_events} events (from {len(events)} total)")
            events = events[:max_events]
        
        # Step 3: Attach images (if missing)
        events_needing_images = [event for event in events if not event.get('image') and not event.get('imageURL')]
        logger.info(f"🖼️ Step 3: Attaching images to {len(events_needing_images)} events (out of {len(events)} total)")
        
        if events_needing_images:
            events_with_new_images = process_event_batch(events_needing_images, batch_size=IMAGE_BATCH_SIZE)
            
            # Replace the events that needed images with the updated versions
            event_map = {e.get('title', ''): i for i, e in enumerate(events)}
            for updated_event in events_with_new_images:
                title = updated_event.get('title', '')
                if title in event_map:
                    events[event_map[title]] = updated_event
        
        # Make sure all events have both image and imageURL fields
        for event in events:
            if 'image' in event and not event.get('imageURL'):
                event['imageURL'] = event['image']
            elif 'imageURL' in event and not event.get('image'):
                event['image'] = event['imageURL']
        
        results["events_with_images"] = len(events)
        logger.info(f"Now all {len(events)} events have images")
        
        if save_to_file:
            images_file = f"2_events_with_images_{batch_id}.json"
            with open(images_file, 'w') as f:
                json.dump(events, f, indent=2)
            logger.info(f"Saved events with images to {images_file}")
        
        # Step 4: Add descriptions and other essential fields using EventEditorAgent
        logger.info(f"📝 Step 4: Adding descriptions and essential fields to {len(events)} events")
        edited_events = []
        
        for event in events:
            # Check if the event needs editing
            needs_editing = (
                not event.get('description') or
                event.get('description', '').strip() == '' or
                not event.get('name') or
                'tag_ids' not in event
            )
            
            if needs_editing:
                edited_event = await enhance_event(event)
                edited_events.append(edited_event)
            else:
                edited_events.append(event)
        
        events = edited_events
        results["events_edited"] = len(events)
        
        if save_to_file:
            edited_file = f"3_edited_events_{batch_id}.json"
            with open(edited_file, 'w') as f:
                json.dump(events, f, indent=2)
            logger.info(f"Saved edited events to {edited_file}")
        
        # Step 5: Enhance events with additional data
        logger.info(f"🔄 Step 5: Enhancing {len(events)} events with location data")
        enhanced_events = enhance_events(events)
        
        results["events_enhanced"] = len(enhanced_events)
        logger.info(f"Enhanced {len(enhanced_events)} events with additional data")
        
        if save_to_file:
            enhanced_file = f"4_enhanced_events_{batch_id}.json"
            with open(enhanced_file, 'w') as f:
                json.dump(enhanced_events, f, indent=2)
            logger.info(f"Saved enhanced events to {enhanced_file}")
        
        # Step 6: Validate events
        logger.info(f"✅ Step 6: Validating {len(enhanced_events)} events")
        valid_events, invalid_events = validate_events(enhanced_events, fix_issues=True)
        
        # Step 6b: Try to fix invalid events if requested
        if fix_invalid and invalid_events:
            logger.info(f"🔧 Attempting to fix {len(invalid_events)} invalid events")
            fixed_events = await fix_invalid_events(enhanced_events, invalid_events)
            
            if fixed_events:
                logger.info(f"Fixed {len(fixed_events)} events, validating again")
                # Validate the fixed events
                newly_valid, still_invalid = validate_events(fixed_events, fix_issues=True)
                
                if newly_valid:
                    logger.info(f"Successfully fixed {len(newly_valid)} events")
                    valid_events.extend(newly_valid)
                
                if still_invalid:
                    logger.info(f"Could not fix {len(still_invalid)} events")
                    invalid_events = still_invalid
        
        results["events_valid"] = len(valid_events)
        logger.info(f"Final validation results: {len(valid_events)} valid, {len(invalid_events)} invalid")
        
        if save_to_file:
            valid_file = f"5_valid_events_{batch_id}.json"
            with open(valid_file, 'w') as f:
                json.dump(valid_events, f, indent=2)
            logger.info(f"Saved valid events to {valid_file}")
            
            if invalid_events:
                invalid_file = f"5_invalid_events_{batch_id}.json"
                with open(invalid_file, 'w') as f:
                    json.dump(invalid_events, f, indent=2)
                logger.info(f"Saved invalid events to {invalid_file}")
        
        if not valid_events:
            logger.warning(f"No valid events, stopping pipeline")
            return results
        
        # Step 7: Post to RuniUni API
        if not dry_run:
            logger.info(f"📤 Step 7: Posting {len(valid_events)} events to RuniUni API")
            post_results = await client.post_multiple_events(valid_events, delay_between_requests=REQUEST_DELAY)
            
            results["events_posted"] = post_results["posted"]
            results["events_failed"] = post_results["failed"]
            logger.info(f"Posted {post_results['posted']} events, {post_results['failed']} failed")
            
            if save_to_file and post_results.get("failed_events"):
                failed_file = f"6_failed_posts_{batch_id}.json"
                with open(failed_file, 'w') as f:
                    json.dump(post_results["failed_events"], f, indent=2)
                logger.info(f"Saved failed posts to {failed_file}")
        else:
            logger.info(f"📝 Step 7: DRY RUN - Would have posted {len(valid_events)} events to RuniUni API")
            results["events_posted"] = 0
            results["events_failed"] = 0
        
        # Calculate duration
        pipeline_end = datetime.now()
        results["end_time"] = pipeline_end.isoformat()
        results["duration_seconds"] = (pipeline_end - pipeline_start).total_seconds()
        
        logger.info(f"✨ Pipeline completed in {results['duration_seconds']:.2f} seconds")
        return results
    
    except Exception as e:
        logger.error(f"Error in pipeline: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Calculate duration even on error
        pipeline_end = datetime.now()
        results["end_time"] = pipeline_end.isoformat()
        results["duration_seconds"] = (pipeline_end - pipeline_start).total_seconds()
        results["error"] = str(e)
        
        return results

async def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="RuniUni URL Event Pipeline")

    # URL source options (can use either or both)
    sources_group = parser.add_argument_group('URL Sources')
    sources_group.add_argument("--urls", type=str, nargs="+", default=[],
                              help="Specific URLs to scan for events")
    sources_group.add_argument("--sources-file", type=str,
                              help="JSON file containing URL sources to scan")

    # RuniUni API credentials
    api_group = parser.add_argument_group('API Options')
    api_group.add_argument("--username", type=str, default=RUNIUNI_USERNAME,
                          help="RuniUni username (default: from environment or config)")
    api_group.add_argument("--password", type=str, default=RUNIUNI_PASSWORD,
                          help="RuniUni password (default: from environment or config)")
    api_group.add_argument("--api-url", type=str, default=RUNIUNI_BASE_URL,
                          help="RuniUni API URL (default: from environment or config)")

    # Pipeline options
    pipeline_group = parser.add_argument_group('Pipeline Options')
    pipeline_group.add_argument("--max-events", type=int, default=DEFAULT_EVENT_LIMIT,
                               help=f"Maximum events to process (default: {DEFAULT_EVENT_LIMIT})")
    pipeline_group.add_argument("--dry-run", action="store_true",
                               help="Don't actually post events to the API")
    pipeline_group.add_argument("--save-files", action="store_true", default=SAVE_FILES,
                               help="Save intermediate JSON files for each step")
    pipeline_group.add_argument("--output", type=str, default="url_results.json",
                               help="Output file for pipeline results (default: url_results.json)")
    pipeline_group.add_argument("--no-fix-invalid", action="store_true",
                               help="Skip the step that attempts to fix invalid events")

    args = parser.parse_args()

    # Check for API keys
    if not os.environ.get("OPENAI_API_KEY"):
        logger.warning("OPENAI_API_KEY not set. AI-powered features may be limited or fail.")
    if not os.environ.get("GOOGLE_API_KEY") or not os.environ.get("SEARCH_ENGINE_ID"):
        logger.warning("GOOGLE_API_KEY or SEARCH_ENGINE_ID not set. Image attachment may use default images.")
    if not os.environ.get("GOOGLE_PLACES_API_KEY"):
        logger.warning("GOOGLE_PLACES_API_KEY not set. Location data enhancement may be limited.")

    # Ensure at least one URL source is provided
    if not args.urls and not args.sources_file:
        logger.error("No URLs or sources file provided. Please specify at least one URL source.")
        parser.print_help()
        return

    # Validate credentials if not in dry run mode
    if not args.dry_run and (not args.username or not args.password):
        logger.error("RuniUni username and password are required when not in dry-run mode")
        return
    
    # Initialize RuniUni client
    client = RuniUniJWTClient(
        username=args.username,
        password=args.password,
        base_url=args.api_url
    )
    
    # Set up the pipeline
    url_count = len(args.urls) if args.urls else 0
    sources_info = f" and sources file {args.sources_file}" if args.sources_file else ""
    logger.info(f"Starting URL pipeline with {url_count} URLs{sources_info}")
    logger.info(f"Max events: {args.max_events}")
    
    if args.dry_run:
        logger.info("DRY RUN MODE - Events will not be posted to RuniUni API")
    
    # Process the URLs
    results = await process_urls(
        urls=args.urls,
        sources_file=args.sources_file,
        client=client,
        max_events=args.max_events,
        dry_run=args.dry_run,
        save_to_file=args.save_files,
        fix_invalid=not args.no_fix_invalid
    )
    
    # Compile summary
    summary = {
        "timestamp": datetime.now().isoformat(),
        "urls_processed": len(args.urls) if args.urls else 0,
        "sources_file": args.sources_file,
        "total_events_extracted": results["events_extracted"],
        "total_events_edited": results["events_edited"],
        "total_events_enhanced": results["events_enhanced"],
        "total_events_valid": results["events_valid"],
        "total_events_posted": results["events_posted"],
        "total_events_failed": results["events_failed"],
        "dry_run": args.dry_run,
        "max_events": args.max_events,
        "pipeline_results": results
    }
    
    # Save results
    with open(args.output, 'w') as f:
        json.dump(summary, f, indent=2)
    
    # Print summary
    logger.info("======= PIPELINE SUMMARY =======")
    logger.info(f"URLs processed: {summary['urls_processed']}")
    if args.sources_file:
        logger.info(f"Sources file: {args.sources_file}")
    logger.info(f"Total events extracted: {summary['total_events_extracted']}")
    logger.info(f"Total events edited: {summary['total_events_edited']}")
    logger.info(f"Total events enhanced: {summary['total_events_enhanced']}")
    logger.info(f"Total valid events: {summary['total_events_valid']}")
    
    if not args.dry_run:
        logger.info(f"Total events posted: {summary['total_events_posted']}")
        logger.info(f"Total events failed: {summary['total_events_failed']}")
    else:
        logger.info(f"Dry run - would have posted: {summary['total_events_valid']} events")
    
    logger.info(f"Results saved to: {args.output}")
    logger.info("==============================")

if __name__ == "__main__":
    asyncio.run(main())