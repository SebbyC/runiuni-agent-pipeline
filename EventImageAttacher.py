# EventImageAttacher.py

import os
import time
from typing import List, Dict, Any, Optional
from googleapiclient.discovery import build
import logging
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('EventImageAttacher')

# API Configuration
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
SEARCH_ENGINE_ID = os.environ.get("SEARCH_ENGINE_ID")

# Log warning if API keys are missing
if not GOOGLE_API_KEY or not SEARCH_ENGINE_ID:
    logger.warning("GOOGLE_API_KEY or SEARCH_ENGINE_ID environment variables are not set. Image search will be disabled.")

def find_event_image(event_title: str, event_location: str) -> Optional[str]:
    """
    Find an image for the event using Google Custom Search API.

    Args:
        event_title: Title of the event
        event_location: Location of the event (city, state)

    Returns:
        URL of the image if found, None otherwise
    """
    if not GOOGLE_API_KEY or not SEARCH_ENGINE_ID:
        logger.error("GOOGLE_API_KEY or SEARCH_ENGINE_ID is not set. Cannot search for images.")
        return None

    query = f"{event_title} {event_location} event"
    logger.info(f"Searching for image with query: '{query}'")

    try:
        # Use cache_discovery=False to avoid oauth2client warning
        service = build("customsearch", "v1", developerKey=GOOGLE_API_KEY, cache_discovery=False)
        
        # Execute the search with the correct case for imgSize (UPPERCASE)
        results = service.cse().list(
            q=query,
            cx=SEARCH_ENGINE_ID,
            searchType='image',
            num=1,
            imgSize='LARGE',  # Note: UPPERCASE is required by the API
            safe='active'     # Safe search
        ).execute()
        
        images = results.get('items', [])
        if images:
            image_url = images[0]['link']
            logger.info(f"Found image for '{event_title}': {image_url}")
            return image_url
        else:
            logger.warning(f"No images found for '{event_title}'")
            
    except Exception as e:
        logger.error(f"Error finding image for '{event_title}': {str(e)}")
        logger.error(traceback.format_exc())
    
    return None

def attach_images(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Process a list of events and attach image URLs to each.
    
    Args:
        events: List of event dictionaries
        
    Returns:
        List of event dictionaries with added image and imageURL fields
    """
    if not events:
        logger.warning("No events to process for image attachment")
        return events
    
    logger.info(f"Attaching images to {len(events)} events")
    
    for i, event in enumerate(events):
        # Skip if event already has an image URL
        if event.get('imageURL') and event['imageURL'] != "":
            logger.info(f"Event {i+1}/{len(events)} already has an image: {event.get('name', event.get('title', 'Unnamed event'))}")
            # Make sure 'image' field is also set to maintain compatibility with the API
            event['image'] = event['imageURL']
            continue
        
        # Get event information for the image search
        event_name = event.get('name', event.get('title', ''))
        event_location = f"{event.get('city', '')}, {event.get('state', '')}"
        
        if not event_name:
            logger.warning(f"Event {i+1}/{len(events)} has no name/title, skipping image search")
            default_image = "https://picsum.photos/800/600"  # Default image
            event['imageURL'] = default_image
            event['image'] = default_image  # Add 'image' field for API compatibility
            continue
        
        # Find image for the event
        logger.info(f"Finding image for event {i+1}/{len(events)}: {event_name}")
        image_url = find_event_image(event_name, event_location)
        
        # Use default if no image found
        image_url = image_url or "https://picsum.photos/800/600"
        
        # Add the image URL to both fields for compatibility
        event['imageURL'] = image_url  # For internal pipeline use
        event['image'] = image_url     # For API compatibility
        
        # Add a small delay to avoid rate limiting
        if i < len(events) - 1:  # No need to delay after the last item
            time.sleep(1)  # 1 second delay
    
    logger.info("Finished attaching images to all events")
    return events

def process_event_batch(events: List[Dict[str, Any]], batch_size: int = 5) -> List[Dict[str, Any]]:
    """
    Process events in batches to avoid rate limiting with the Google API.
    
    Args:
        events: List of event dictionaries
        batch_size: Number of events to process in each batch
        
    Returns:
        List of event dictionaries with added image and imageURL fields
    """
    total_events = len(events)
    processed_events = []
    
    for i in range(0, total_events, batch_size):
        batch = events[i:i+batch_size]
        logger.info(f"Processing batch {i//batch_size + 1}/{(total_events+batch_size-1)//batch_size}")
        
        # Process this batch
        processed_batch = attach_images(batch)
        processed_events.extend(processed_batch)
        
        # Add a delay between batches to avoid rate limiting
        if i + batch_size < total_events:  # No need to delay after the last batch
            logger.info("Waiting between batches to avoid rate limiting...")
            time.sleep(3)  # 3 second delay between batches
    
    return processed_events

# Example usage if run directly
if __name__ == "__main__":
    # Test with a few sample events
    sample_events = [
        {
            "name": "WSRE Wine & Food Classic",
            "city": "Pensacola",
            "state": "Florida",
            "country": "United States",
            "description": "A celebration of regional flavors featuring fine wines and gourmet dishes."
        },
        {
            "name": "Gulf Coast Whale Festival",
            "city": "Pensacola Beach",
            "state": "Florida",
            "country": "United States",
            "description": "Celebrate the wonders of the Gulf with educational exhibits, a marine life puppet parade, and live music."
        }
    ]
    
    # Attach images to the sample events
    events_with_images = process_event_batch(sample_events)
    
    # Print the results
    for event in events_with_images:
        print(f"Event: {event.get('name')}")
        print(f"Image URL: {event.get('imageURL')}")
        print(f"Image field: {event.get('image')}")
        print("-----")