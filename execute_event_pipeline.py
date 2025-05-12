# execute_event_pipeline.py

import asyncio
import logging
import argparse
import json
# Add parent directory to path
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

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Now import from the parent directory
from config import *
# Import all pipeline components
from EventSearcherAgent import search_events
from EventImageAttacher import process_event_batch
from EventDataEnhancer import enhance_events
from EventValidationChecker import validate_events
from RuniuniJWTClient import RuniUniJWTClient  # Note lowercase 'u' in Runiuni

# Configure logging based on settings in config
log_level = getattr(logging, LOG_LEVEL)
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Output to console
        logging.FileHandler(f"event_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")  # Output to file
    ]
)
logger = logging.getLogger('EventPipeline')

async def process_location(
    location: str,
    client: RuniUniJWTClient,
    max_events: int = DEFAULT_EVENT_LIMIT,
    dry_run: bool = False,
    save_to_file: bool = SAVE_FILES
) -> Dict[str, Any]:
    """
    Process a single location through the entire pipeline.
    
    Args:
        location: Location to search for events
        client: RuniUni API client
        max_events: Maximum number of events to process
        dry_run: If True, don't actually post events to the API
        save_to_file: If True, save events to JSON files at each stage
        
    Returns:
        Dictionary with processing results
    """
    pipeline_start = datetime.now()
    results = {
        "location": location,
        "max_events": max_events,
        "dry_run": dry_run,
        "start_time": pipeline_start.isoformat(),
        "end_time": None,
        "duration_seconds": None,
        "events_found": 0,
        "events_with_images": 0,
        "events_enhanced": 0,
        "events_valid": 0,
        "events_posted": 0,
        "events_failed": 0
    }
    
    try:
        # Step 1: Search for events
        logger.info(f"🔍 Step 1: Searching for events in {location}")
        events = await search_events(location, max_events)
        
        results["events_found"] = len(events)
        logger.info(f"Found {len(events)} events in {location}")
        
        if save_to_file:
            with open(f"1_search_results_{location.replace(' ', '_')}.json", 'w') as f:
                json.dump(events, f, indent=2)
        
        if not events:
            logger.warning(f"No events found for {location}, stopping pipeline")
            return results
        
        # Step 2: Attach images
        logger.info(f"🖼️ Step 2: Attaching images to {len(events)} events")
        events_with_images = process_event_batch(events, batch_size=IMAGE_BATCH_SIZE)
        
        results["events_with_images"] = len(events_with_images)
        logger.info(f"Added images to {len(events_with_images)} events")
        
        if save_to_file:
            with open(f"2_events_with_images_{location.replace(' ', '_')}.json", 'w') as f:
                json.dump(events_with_images, f, indent=2)
        
        # Step 3: Enhance with additional data
        logger.info(f"🔄 Step 3: Enhancing {len(events_with_images)} events with additional data")
        enhanced_events = enhance_events(events_with_images)
        
        results["events_enhanced"] = len(enhanced_events)
        logger.info(f"Enhanced {len(enhanced_events)} events with additional data")
        
        if save_to_file:
            with open(f"3_enhanced_events_{location.replace(' ', '_')}.json", 'w') as f:
                json.dump(enhanced_events, f, indent=2)
        
        # Step 4: Validate events
        logger.info(f"✅ Step 4: Validating {len(enhanced_events)} events")
        valid_events, invalid_events = validate_events(enhanced_events, fix_issues=True)
        
        results["events_valid"] = len(valid_events)
        logger.info(f"Validation results: {len(valid_events)} valid, {len(invalid_events)} invalid")
        
        if save_to_file:
            with open(f"4_valid_events_{location.replace(' ', '_')}.json", 'w') as f:
                json.dump(valid_events, f, indent=2)
            
            if invalid_events:
                with open(f"4_invalid_events_{location.replace(' ', '_')}.json", 'w') as f:
                    json.dump(invalid_events, f, indent=2)
        
        if not valid_events:
            logger.warning(f"No valid events for {location}, stopping pipeline")
            return results
        
        # Step 5: Post to RuniUni API
        if not dry_run:
            logger.info(f"📤 Step 5: Posting {len(valid_events)} events to RuniUni API")
            post_results = await client.post_multiple_events(valid_events, delay_between_requests=REQUEST_DELAY)
            
            results["events_posted"] = post_results["posted"]
            results["events_failed"] = post_results["failed"]
            logger.info(f"Posted {post_results['posted']} events, {post_results['failed']} failed")
            
            if save_to_file and post_results.get("failed_events"):
                with open(f"5_failed_posts_{location.replace(' ', '_')}.json", 'w') as f:
                    json.dump(post_results["failed_events"], f, indent=2)
        else:
            logger.info(f"📝 Step 5: DRY RUN - Would have posted {len(valid_events)} events to RuniUni API")
            results["events_posted"] = 0
            results["events_failed"] = 0
        
        # Calculate duration
        pipeline_end = datetime.now()
        results["end_time"] = pipeline_end.isoformat()
        results["duration_seconds"] = (pipeline_end - pipeline_start).total_seconds()
        
        logger.info(f"✨ Pipeline completed for {location} in {results['duration_seconds']:.2f} seconds")
        return results
    
    except Exception as e:
        logger.error(f"Error in pipeline for {location}: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Calculate duration even on error
        pipeline_end = datetime.now()
        results["end_time"] = pipeline_end.isoformat()
        results["duration_seconds"] = (pipeline_end - pipeline_start).total_seconds()
        results["error"] = str(e)
        
        return results

async def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="RuniUni Event Pipeline")
    parser.add_argument("--locations", type=str, nargs="+", default=DEFAULT_LOCATIONS,
                      help=f"Locations to search for events (default: {DEFAULT_LOCATIONS})")
    parser.add_argument("--username", type=str, default=RUNIUNI_USERNAME,
                      help="RuniUni username (default: from environment or config)")
    parser.add_argument("--password", type=str, default=RUNIUNI_PASSWORD,
                      help="RuniUni password (default: from environment or config)")
    parser.add_argument("--api-url", type=str, default=RUNIUNI_BASE_URL,
                      help="RuniUni API URL (default: from environment or config)")
    parser.add_argument("--max-events", type=int, default=DEFAULT_EVENT_LIMIT,
                      help=f"Maximum events to process per location (default: {DEFAULT_EVENT_LIMIT})")
    parser.add_argument("--dry-run", action="store_true",
                      help="Don't actually post events to the API")
    parser.add_argument("--save-files", action="store_true", default=SAVE_FILES,
                      help="Save intermediate JSON files for each step")
    parser.add_argument("--output", type=str, default=DEFAULT_OUTPUT_FILE,
                      help=f"Output file for pipeline results (default: {DEFAULT_OUTPUT_FILE})")

    args = parser.parse_args()

    # Check for API keys
    if not os.environ.get("OPENAI_API_KEY"):
        logger.warning("OPENAI_API_KEY not set. AI-powered features may be limited or fail.")
    if not os.environ.get("GOOGLE_API_KEY") or not os.environ.get("SEARCH_ENGINE_ID"):
        logger.warning("GOOGLE_API_KEY or SEARCH_ENGINE_ID not set. Image attachment may use default images.")
    if not os.environ.get("GOOGLE_PLACES_API_KEY"):
        logger.warning("GOOGLE_PLACES_API_KEY not set. Location data enhancement may be limited.")

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
    
    # Set up the pipeline for all locations
    logger.info(f"Starting event pipeline for {len(args.locations)} locations")
    logger.info(f"Max events per location: {args.max_events}")
    
    if args.dry_run:
        logger.info("DRY RUN MODE - Events will not be posted to RuniUni API")
    
    # Process each location
    all_results = []
    for location in args.locations:
        logger.info(f"Processing location: {location}")
        result = await process_location(
            location=location,
            client=client,
            max_events=args.max_events,
            dry_run=args.dry_run,
            save_to_file=args.save_files
        )
        all_results.append(result)
    
    # Compile summary
    summary = {
        "timestamp": datetime.now().isoformat(),
        "total_locations": len(args.locations),
        "total_events_found": sum(r["events_found"] for r in all_results),
        "total_events_valid": sum(r["events_valid"] for r in all_results),
        "total_events_posted": sum(r["events_posted"] for r in all_results),
        "total_events_failed": sum(r["events_failed"] for r in all_results),
        "dry_run": args.dry_run,
        "max_events_per_location": args.max_events,
        "location_results": all_results
    }
    
    # Save results
    with open(args.output, 'w') as f:
        json.dump(summary, f, indent=2)
    
    # Print summary
    logger.info("======= PIPELINE SUMMARY =======")
    logger.info(f"Total locations processed: {summary['total_locations']}")
    logger.info(f"Total events found: {summary['total_events_found']}")
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