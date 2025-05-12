# EventDataEnhancer.py

import os
import requests
import logging
from typing import List, Dict, Any, Optional
import re
from datetime import datetime, time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('EventDataEnhancer')

# API Configuration
GOOGLE_PLACES_API_KEY = os.environ.get("GOOGLE_PLACES_API_KEY")

# Log warning if API key is missing
if not GOOGLE_PLACES_API_KEY:
    logger.warning("GOOGLE_PLACES_API_KEY environment variable is not set. Location enhancement will be limited.")

def get_location_details(location_query: str) -> Dict[str, Any]:
    """
    Get detailed location information from Google Places API using a query string.

    Args:
        location_query: Location query string (e.g., "Pensacola, FL" or "Saenger Theatre, Pensacola")

    Returns:
        Dictionary containing location details (city, state, country, district, lat, lng, formatted_address)
    """
    # Initialize the result dictionary with default values
    location_data = {
        "city": "",
        "state": "",
        "country": "",
        "district": "",
        "lat": None,
        "lng": None,
        "formatted_address": ""  # Add formatted_address field for full venue address
    }

    if not GOOGLE_PLACES_API_KEY:
        logger.error("GOOGLE_PLACES_API_KEY is not set. Cannot fetch location details.")
        return location_data

    if not location_query:
        logger.warning("Empty location query provided")
        return location_data

    logger.info(f"Getting location details for: '{location_query}'")

    # Geocoding API endpoint
    endpoint = "https://maps.googleapis.com/maps/api/geocode/json"

    # Parameters for the API request
    params = {
        "address": location_query,
        "key": GOOGLE_PLACES_API_KEY
    }
    
    try:
        # Make the API request
        response = requests.get(endpoint, params=params)
        data = response.json()
        
        # Check if the request was successful
        if data["status"] == "OK" and len(data["results"]) > 0:
            # Get the first result
            result = data["results"][0]
            
            # Store the formatted address
            location_data["formatted_address"] = result.get("formatted_address", "")
            
            # Extract coordinates
            location = result["geometry"]["location"]
            location_data["lat"] = location["lat"]
            location_data["lng"] = location["lng"]
            
            # Extract address components
            address_components = result["address_components"]
            
            for component in address_components:
                # City (locality)
                if "locality" in component["types"]:
                    location_data["city"] = component["long_name"]
                
                # District (administrative_area_level_2)
                elif "administrative_area_level_2" in component["types"]:
                    location_data["district"] = component["long_name"]
                
                # State (administrative_area_level_1)
                elif "administrative_area_level_1" in component["types"]:
                    location_data["state"] = component["long_name"]
                    # Also store the state code (e.g., "FL")
                    location_data["state_code"] = component["short_name"]
                
                # Country
                elif "country" in component["types"]:
                    location_data["country"] = component["long_name"]
                    # Also store the country code (e.g., "US")
                    location_data["country_code"] = component["short_name"]
            
            logger.info(f"Found location details for '{location_query}': {location_data['city']}, {location_data['state']}")
            if location_data["formatted_address"]:
                logger.info(f"Found address: {location_data['formatted_address']}")
        else:
            logger.warning(f"Could not find location details for '{location_query}': {data['status']}")
    
    except Exception as e:
        logger.error(f"Error getting location details for '{location_query}': {str(e)}")
    
    return location_data

def format_dates_and_times(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format and validate date and time fields in an event.
    
    Args:
        event: Event dictionary
        
    Returns:
        Event with formatted date and time fields
    """
    # Default values
    today = datetime.now().strftime('%Y-%m-%d')
    default_start_time = "18:00:00"
    default_end_time = "23:59:59"
    
    # Ensure start_date exists
    if not event.get('start_date'):
        # Try to extract from date field if it exists
        if event.get('date'):
            # Try different date formats
            date_patterns = [
                r'(\d{4}-\d{2}-\d{2})',  # YYYY-MM-DD
                r'(\d{1,2}/\d{1,2}/\d{4})',  # MM/DD/YYYY
                r'([A-Za-z]+ \d{1,2}, \d{4})'  # Month DD, YYYY
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, event['date'])
                if match:
                    # Found a date match, try to parse it
                    try:
                        if '-' in match.group(1):  # YYYY-MM-DD
                            event['start_date'] = match.group(1)
                        elif '/' in match.group(1):  # MM/DD/YYYY
                            parts = match.group(1).split('/')
                            event['start_date'] = f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
                        else:  # Month DD, YYYY
                            date_obj = datetime.strptime(match.group(1), '%B %d, %Y')
                            event['start_date'] = date_obj.strftime('%Y-%m-%d')
                        break
                    except (ValueError, IndexError):
                        continue
        
        # If still no start_date, use today
        if not event.get('start_date'):
            event['start_date'] = today
    
    # Ensure end_date exists
    if not event.get('end_date'):
        event['end_date'] = event['start_date']
    
    # Ensure start_time exists
    if not event.get('start_time'):
        # Try to extract from date field if it exists
        if event.get('date'):
            # Try to find time
            time_pattern = r'(\d{1,2}):(\d{2})(?:\s*)(AM|PM)?'
            match = re.search(time_pattern, event.get('date', ''))
            if match:
                hour = int(match.group(1))
                minute = match.group(2)
                am_pm = match.group(3)
                
                # Handle AM/PM
                if am_pm and am_pm.upper() == 'PM' and hour < 12:
                    hour += 12
                elif am_pm and am_pm.upper() == 'AM' and hour == 12:
                    hour = 0
                
                event['start_time'] = f"{hour:02}:{minute}:00"
            else:
                event['start_time'] = default_start_time
        else:
            event['start_time'] = default_start_time
    
    # Ensure end_time exists
    if not event.get('end_time'):
        event['end_time'] = default_end_time
    
    return event

def get_event_address(event: Dict[str, Any], location_data: Dict[str, Any]) -> str:
    """
    Format a full address for the event based on available data.
    
    Args:
        event: Event dictionary
        location_data: Location data from Google API
        
    Returns:
        Formatted address string
    """
    # If we have a formatted address from the API, use it
    if location_data.get('formatted_address'):
        return location_data['formatted_address']
    
    # Otherwise, try to build a reasonable address from venue and location
    venue = event.get('venue', '')
    city = event.get('city', location_data.get('city', ''))
    state = event.get('state', location_data.get('state', ''))
    country = event.get('country', location_data.get('country', ''))
    
    # Construct address components
    address_parts = []
    
    if venue:
        address_parts.append(venue)
    
    location_part = ", ".join(p for p in [city, state] if p)
    if location_part:
        address_parts.append(location_part)
    
    if country and country != "United States":  # Only add country if not US (common convention)
        address_parts.append(country)
    
    if not address_parts:
        return ""
    
    return ", ".join(address_parts)



from typing import Dict, Any, List

def infer_event_tags(event: Dict[str, Any]) -> List[int]:
    """
    Infer event tags based on description and title.
    
    Args:
        event: Event dictionary
        
    Returns:
        List of tag IDs
    """
    # Define tag categories and associated keywords using the correct tag IDs
    tag_categories = {
        # Live Music (ID: 1)
        1: ['live music', 'concert', 'musician', 'band', 'performance', 'singer', 'gig'],
        
        # Nightlife (ID: 2)
        2: ['nightlife', 'club', 'bar', 'pub', 'party', 'nightclub', 'disco', 'DJ'],
        
        # Comedy (ID: 3)
        3: ['comedy', 'comedian', 'stand-up', 'improv', 'humorous', 'funny', 'laugh'],
        
        # Family-Friendly (ID: 4)
        4: ['family', 'kids', 'children', 'child', 'youth', 'family-friendly', 'all ages'],
        
        # Food Festival (ID: 5)
        5: ['food', 'culinary', 'cuisine', 'tasting', 'dining', 'restaurant', 'chef', 'wine', 'beer'],
        
        # Sports (ID: 6)
        6: ['sports', 'game', 'match', 'tournament', 'athletics', 'competition', 'team', 'league'],
        
        # Art Exhibition (ID: 7)
        7: ['art', 'gallery', 'exhibition', 'museum', 'artist', 'painting', 'sculpture'],
        
        # Networking (ID: 8)
        8: ['network', 'networking', 'social', 'meetup', 'mixer', 'professional', 'business', 'entrepreneur'],
        
        # Tech Meetup (ID: 9)
        9: ['tech', 'technology', 'coding', 'programming', 'developer', 'software', 'hardware', 'startup', 'innovation'],
        
        # Charity Event (ID: 10)
        10: ['charity', 'fundraiser', 'nonprofit', 'donation', 'cause', 'benefit', 'volunteer'],
        
        # Educational (ID: 11)
        11: ['education', 'learning', 'workshop', 'class', 'seminar', 'lecture', 'training', 'conference'],
        
        # Dance Party (ID: 12)
        12: ['dance', 'dancing', 'choreography', 'ballroom', 'salsa', 'hip-hop', 'ballet'],
        
        # Outdoor (ID: 13)
        13: ['outdoor', 'outside', 'park', 'nature', 'garden', 'field', 'yard', 'plaza'],
        
        # Indoor (ID: In: 14)
        14: ['indoor', 'inside', 'venue', 'hall', 'center', 'building', 'auditorium'],
        
        # Virtual Event (ID: 15)
        15: ['virtual', 'online', 'digital', 'remote', 'zoom', 'stream', 'webinar'],
        
        # Gaming Tournament (ID: 16)
        16: ['gaming', 'game', 'tournament', 'esports', 'video game', 'console', 'competition'],
        
        # Health & Wellness (ID: 17)
        17: ['health', 'wellness', 'fitness', 'well-being', 'mindfulness', 'self-care', 'spa', 'retreat'],
        
        # Yoga (ID: 18)
        18: ['yoga', 'meditation', 'mindfulness', 'stretching', 'poses', 'asana'],
        
        # Meditation (ID: 19)
        19: ['meditation', 'mindfulness', 'relaxation', 'spiritual', 'zen', 'calm', 'peace'],
        
        # Concert (ID: 20)
        20: ['concert', 'symphony', 'orchestra', 'philharmonic', 'recital', 'show', 'musical'],
        
        # Theater (ID: 21)
        21: ['theater', 'theatre', 'play', 'drama', 'performance', 'stage', 'acting', 'broadway']
    }
    
    # Get text to analyze
    title = event.get('name', event.get('title', '')).lower()
    description = event.get('description', '').lower()
    venue = event.get('venue', event.get('address', '')).lower()
    full_text = f"{title} {description} {venue}"
    
    # Find matching tags
    matched_tags = set()
    
    # First pass: look for exact keyword matches
    for tag_id, keywords in tag_categories.items():
        for keyword in keywords:
            if keyword in full_text:
                matched_tags.add(tag_id)
                break
    
    # Second pass: Check for related content if we haven't found any tags yet
    if not matched_tags:
        # Check for outdoor vs indoor
        if any(word in full_text for word in ['park', 'garden', 'outside', 'outdoors', 'nature']):
            matched_tags.add(13)  # Outdoor
        elif any(word in full_text for word in ['hall', 'theater', 'venue', 'center', 'inside']):
            matched_tags.add(14)  # Indoor
            
        # Check for event type based on common patterns
        if any(word in full_text for word in ['music', 'song', 'audio', 'listen']):
            matched_tags.add(1)  # Live Music
        
        if any(word in full_text for word in ['laugh', 'joke', 'funny']):
            matched_tags.add(3)  # Comedy
            
        if any(word in full_text for word in ['workshop', 'learn', 'education', 'knowledge']):
            matched_tags.add(11)  # Educational
    
    # Apply heuristics for common combinations
    if 1 in matched_tags and 20 not in matched_tags:
        # If we have live music but not concert, add concert
        matched_tags.add(20)
        
    if 18 in matched_tags or 19 in matched_tags:
        # Yoga or Meditation suggests Health & Wellness
        matched_tags.add(17)
        
    # If event has only Indoor or Outdoor tag, try to infer at least one content tag
    if matched_tags == {13} or matched_tags == {14} or not matched_tags:
        # Look for any words that might indicate the type of event
        if any(word in full_text for word in ['music', 'band', 'concert', 'performance']):
            matched_tags.add(1)  # Live Music
        elif any(word in full_text for word in ['art', 'gallery', 'exhibition']):
            matched_tags.add(7)  # Art Exhibition
        elif any(word in full_text for word in ['learn', 'education', 'workshop']):
            matched_tags.add(11)  # Educational
    
    return list(matched_tags)

def enhance_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enhance a single event with additional data.
    
    Args:
        event: Original event dictionary
        
    Returns:
        Enhanced event dictionary
    """
    # Make a copy to avoid modifying the original
    enhanced = event.copy()
    
    # 1. Format name/title field
    if 'title' in enhanced and not enhanced.get('name'):
        enhanced['name'] = enhanced.pop('title')
    
    # 2. Format dates and times
    enhanced = format_dates_and_times(enhanced)
    
    # 3. Get location details
    venue_location = enhanced.get('venue', '')
    city = enhanced.get('city', '')
    state = enhanced.get('state', '')
    
    # Build location query based on available information
    if venue_location and (city or state):
        location_query = f"{venue_location}, {city}, {state}".strip(", ")
    elif venue_location:
        location_query = venue_location
    elif city and state:
        location_query = f"{city}, {state}"
    else:
        location_query = enhanced.get('location', '')
    
    # Get location details from Google API
    location_data = {}
    if location_query:
        location_data = get_location_details(location_query)
        
        # Update with location details if found
        if location_data['city']:
            enhanced['city'] = location_data['city']
        if location_data['state']:
            enhanced['state'] = location_data['state']
        if location_data['country']:
            enhanced['country'] = location_data['country']
        if location_data['district']:
            enhanced['district'] = location_data['district']
        if location_data['lat'] is not None:
            enhanced['lat'] = location_data['lat']
        if location_data['lng'] is not None:
            enhanced['lng'] = location_data['lng']
    
    # 4. Set default country if still missing
    if not enhanced.get('country'):
        enhanced['country'] = 'United States'
    
    # 5. Add or update address field with full venue address
    address = get_event_address(enhanced, location_data)
    if address:
        enhanced['address'] = address
    
    # 6. Infer tags based on event content
    tag_ids = infer_event_tags(enhanced)
    if tag_ids:
        enhanced['tag_ids'] = tag_ids
    
    return enhanced

def enhance_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Enhance a list of events with additional data.
    
    Args:
        events: List of event dictionaries
        
    Returns:
        List of enhanced event dictionaries
    """
    if not events:
        logger.warning("No events to enhance")
        return events
    
    logger.info(f"Enhancing {len(events)} events with additional data")
    enhanced_events = []
    
    for i, event in enumerate(events):
        logger.info(f"Enhancing event {i+1}/{len(events)}: {event.get('name', event.get('title', 'Unnamed event'))}")
        enhanced_event = enhance_event(event)
        enhanced_events.append(enhanced_event)
    
    logger.info("Finished enhancing all events")
    return enhanced_events

# Example usage if run directly
if __name__ == "__main__":
    # Test with a few sample events
    sample_events = [
        {
            "title": "WSRE Wine & Food Classic",
            "date": "March 29, 2025, 7 PM - 10 PM",
            "venue": "WSRE Jean & Paul Amos Performance Studio",
            "description": "A celebration of regional flavors featuring fine wines and gourmet dishes."
        },
        {
            "title": "Pensacola Symphony Orchestra Presents Strauss & Schubert",
            "date": "March 29, 2025, 7:30 PM - 9:30 PM",
            "venue": "Saenger Theatre, Pensacola",
            "description": "Experience Richard Strauss' Oboe Concerto performed by Titus Underwood, along with Strauss' Serenade for Winds and Schubert's Ninth Symphony, 'The Great.'"
        }
    ]
    
    # Enhance the sample events
    enhanced_events = enhance_events(sample_events)
    
    # Print the results
    import json
    for event in enhanced_events:
        print(json.dumps(event, indent=2))
        print("-----")