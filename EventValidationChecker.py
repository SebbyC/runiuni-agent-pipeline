# EventValidationChecker.py

import logging
import re
from datetime import datetime
from typing import Dict, List, Any, Tuple, Set

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('EventValidationChecker')

# Define required fields - all are mandatory except venue and address
REQUIRED_FIELDS = {
    'name': str,
    'description': str,
    'url': str,
    'imageURL': str,
    'start_date': str,
    'start_time': str,
    'end_date': str,
    'end_time': str,
    'city': str,
    'state': str,
    'country': str,
    'district': str,
    'lat': (float, type(None)),  # Can be None initially but should be populated
    'lng': (float, type(None)),  # Can be None initially but should be populated
    'tag_ids': list
}

# Define optional fields
OPTIONAL_FIELDS = {
    'venue': str,
    'address': str  # Added optional address field for full venue address
}

def validate_date_format(date_str: str) -> bool:
    """
    Validate if a string is in YYYY-MM-DD format.
    
    Args:
        date_str: Date string to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not isinstance(date_str, str):
        return False
        
    # Check format
    if not re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        return False
        
    # Check if it's a valid date
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def validate_time_format(time_str: str) -> bool:
    """
    Validate if a string is in HH:MM:SS format.
    
    Args:
        time_str: Time string to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not isinstance(time_str, str):
        return False
        
    # Check format
    if not re.match(r'^\d{2}:\d{2}:\d{2}$', time_str):
        return False
        
    # Check if it's a valid time
    try:
        datetime.strptime(time_str, '%H:%M:%S')
        return True
    except ValueError:
        return False

def validate_url(url: str) -> bool:
    """
    Validate if a string is a valid URL.
    
    Args:
        url: URL string to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not isinstance(url, str):
        return False
        
    # Simple URL validation
    url_pattern = re.compile(
        r'^(https?:\/\/)?'  # http:// or https://
        r'([a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}'  # domain
        r'(\/[a-zA-Z0-9-._~:/?#[\]@!$&\'()*+,;=]*)?$'  # path, query, fragment
    )
    
    return bool(url_pattern.match(url))

def validate_image_url(url: str) -> bool:
    """
    Validate if a string is a valid image URL.
    
    Args:
        url: Image URL string to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not validate_url(url):
        return False
        
    # Check if URL ends with common image extensions or is a picsum URL
    image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg']
    return any(url.lower().endswith(ext) for ext in image_extensions) or 'picsum.photos' in url

def validate_coordinates(lat: float, lng: float) -> bool:
    """
    Validate latitude and longitude coordinates.
    
    Args:
        lat: Latitude value
        lng: Longitude value
        
    Returns:
        True if valid, False otherwise
    """
    # We still allow None for coordinates because they might be populated later
    if lat is None or lng is None:
        return True
        
    try:
        lat_float = float(lat)
        lng_float = float(lng)
        
        return -90 <= lat_float <= 90 and -180 <= lng_float <= 180
    except (ValueError, TypeError):
        return False

def validate_address(address: str) -> bool:
    """
    Validate if a string is a reasonable address.
    This is a basic validation - in production, consider using a proper address validation service.
    
    Args:
        address: Address string to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not isinstance(address, str):
        return False
    
    # Check if address has minimum required length and isn't just whitespace
    if len(address.strip()) < 5:
        return False
    
    # Check if address contains some basic address elements 
    # (this is very basic - production code would use better validation)
    address_elements = ['street', 'road', 'avenue', 'blvd', 'drive', 'lane', 'way', 
                        'plaza', 'square', 'park', 'st.', 'rd.', 'ave.', 'dr.']
    
    # Convert to lowercase for case-insensitive matching
    address_lower = address.lower()
    
    # Check for numeric elements (possible house/building numbers)
    has_numeric = any(char.isdigit() for char in address)
    
    # Check for address elements
    has_address_element = any(element in address_lower for element in address_elements)
    
    # Simple check: either has a numeric part or contains an address element term
    return has_numeric or has_address_element

def validate_event(event: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate a single event against required fields and format rules.
    
    Args:
        event: Event dictionary to validate
        
    Returns:
        Tuple of (is_valid, list_of_error_messages)
    """
    errors = []
    
    # Check required fields
    for field, field_type in REQUIRED_FIELDS.items():
        if field not in event:
            errors.append(f"Missing required field: {field}")
        elif isinstance(field_type, tuple):
            if not any(isinstance(event[field], t) for t in field_type):
                type_names = [t.__name__ for t in field_type]
                errors.append(f"Field {field} must be one of types: {type_names}")
        elif not isinstance(event[field], field_type):
            errors.append(f"Field {field} must be of type {field_type.__name__}")
        elif isinstance(event[field], str) and not event[field].strip():
            errors.append(f"Field {field} cannot be empty")
    
    # Check optional fields if present
    for field, field_type in OPTIONAL_FIELDS.items():
        if field in event and event[field] is not None:
            if isinstance(field_type, tuple):
                if not any(isinstance(event[field], t) for t in field_type):
                    errors.append(f"Field {field} must be one of types: {[t.__name__ for t in field_type]}")
            elif not isinstance(event[field], field_type):
                errors.append(f"Field {field} must be of type {field_type.__name__}")
    
    # Validate date formats
    if 'start_date' in event and not validate_date_format(event['start_date']):
        errors.append(f"Invalid start_date format: {event['start_date']}. Must be YYYY-MM-DD")
    
    if 'end_date' in event and not validate_date_format(event['end_date']):
        errors.append(f"Invalid end_date format: {event['end_date']}. Must be YYYY-MM-DD")
    
    # Validate time formats
    if 'start_time' in event and not validate_time_format(event['start_time']):
        errors.append(f"Invalid start_time format: {event['start_time']}. Must be HH:MM:SS")
    
    if 'end_time' in event and not validate_time_format(event['end_time']):
        errors.append(f"Invalid end_time format: {event['end_time']}. Must be HH:MM:SS")
    
    # Validate URL
    if 'url' in event and not validate_url(event['url']):
        errors.append(f"Invalid URL format: {event['url']}")
    
    # Validate imageURL
    if 'imageURL' in event and not validate_url(event['imageURL']):
        errors.append(f"Invalid imageURL format: {event['imageURL']}")
    
    # Validate coordinates
    if 'lat' in event and 'lng' in event:
        if not validate_coordinates(event['lat'], event['lng']):
            errors.append(f"Invalid coordinates: lat={event['lat']}, lng={event['lng']}")
    
    # Validate tag_ids
    if 'tag_ids' in event and not isinstance(event['tag_ids'], list):
        errors.append(f"tag_ids must be a list, got {type(event['tag_ids']).__name__}")
    
    # Validate address if present (optional field)
    if 'address' in event and event['address'] and not validate_address(event['address']):
        errors.append(f"Invalid address format: {event['address']}")
    
    # Check date consistency
    if ('start_date' in event and 'end_date' in event and 
        validate_date_format(event['start_date']) and validate_date_format(event['end_date'])):
        if event['start_date'] > event['end_date']:
            errors.append(f"End date {event['end_date']} cannot be before start date {event['start_date']}")
    
    return len(errors) == 0, errors

def validate_events(events: List[Dict[str, Any]], fix_issues: bool = True) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Validate a list of events and optionally attempt to fix minor issues.
    
    Args:
        events: List of event dictionaries
        fix_issues: Whether to attempt to fix minor issues
        
    Returns:
        Tuple of (valid_events, invalid_events)
    """
    if not events:
        logger.warning("No events to validate")
        return [], []
    
    logger.info(f"Validating {len(events)} events")
    
    valid_events = []
    invalid_events = []
    
    for i, event in enumerate(events):
        event_name = event.get('name', event.get('title', f"Event {i+1}"))
        logger.info(f"Validating event {i+1}/{len(events)}: {event_name}")
        
        # Make a copy of the event for potential fixes
        event_copy = event.copy()
        
        # Apply automatic fixes if requested
        if fix_issues:
            # Fix field names (title -> name)
            if 'title' in event_copy and not event_copy.get('name'):
                event_copy['name'] = event_copy.pop('title')
            
            # Ensure district is present
            if 'district' not in event_copy or not event_copy['district']:
                event_copy['district'] = ''
            
            # Ensure tag_ids is a list
            if 'tag_ids' not in event_copy or not isinstance(event_copy['tag_ids'], list):
                if 'tag_ids' not in event_copy or event_copy['tag_ids'] is None:
                    event_copy['tag_ids'] = []
                else:
                    try:
                        event_copy['tag_ids'] = [int(event_copy['tag_ids'])]
                    except (ValueError, TypeError):
                        event_copy['tag_ids'] = []
            
            # Make sure coordinates are numeric or None
            for coord in ['lat', 'lng']:
                if coord in event_copy and event_copy[coord] is not None:
                    try:
                        event_copy[coord] = float(event_copy[coord])
                    except (ValueError, TypeError):
                        event_copy[coord] = None
                        
            # If venue exists but address doesn't, try to use venue as address
            if ('venue' in event_copy and event_copy['venue'] and 
                ('address' not in event_copy or not event_copy['address'])):
                city = event_copy.get('city', '')
                state = event_copy.get('state', '')
                venue = event_copy.get('venue', '')
                if venue and (city or state):
                    event_copy['address'] = f"{venue}, {city}, {state}".strip(", ")
        
        # Validate the event
        is_valid, errors = validate_event(event_copy)
        
        if is_valid:
            valid_events.append(event_copy)
            logger.info(f"Event {event_name} is valid")
        else:
            logger.warning(f"Event {event_name} is invalid: {', '.join(errors)}")
            invalid_events.append({
                "event": event_copy,
                "errors": errors
            })
    
    logger.info(f"Validation complete: {len(valid_events)} valid, {len(invalid_events)} invalid")
    return valid_events, invalid_events

# Example usage if run directly
if __name__ == "__main__":
    # Test with a mix of valid and invalid events
    sample_events = [
        # Valid event with all required fields
        {
            "name": "WSRE Wine & Food Classic",
            "description": "A celebration of regional flavors featuring fine wines and gourmet dishes.",
            "url": "https://pensacolaflorida.com/upcoming-events/",
            "imageURL": "https://example.com/image.jpg",
            "start_date": "2025-03-29",
            "start_time": "19:00:00",
            "end_date": "2025-03-29",
            "end_time": "22:00:00",
            "city": "Pensacola",
            "state": "Florida",
            "country": "United States",
            "district": "Escambia County",
            "lat": 30.421309,
            "lng": -87.216915,
            "tag_ids": [3, 6],
            "venue": "WSRE Jean & Paul Amos Performance Studio",
            "address": "1000 College Blvd, Pensacola, FL 32504"
        },
        # Invalid event - missing fields that are now required
        {
            "name": "Invalid Event",
            "description": "This event is missing required fields",
            "start_date": "2025-04-15",
            "start_time": "18:00:00",
            "end_date": "2025-04-15",
            "end_time": "23:00:00",
            "city": "Pensacola",
            "state": "Florida",
            "country": "United States"
            # Missing url, imageURL, district, lat, lng, tag_ids
        }
    ]
    
    # Validate the sample events
    valid_events, invalid_events = validate_events(sample_events)
    
    # Print the results
    print(f"\nValid events ({len(valid_events)}):")
    for event in valid_events:
        print(f"- {event['name']}")
        if 'address' in event:
            print(f"  Address: {event['address']}")
    
    print(f"\nInvalid events ({len(invalid_events)}):")
    for invalid in invalid_events:
        print(f"- {invalid['event'].get('name', 'Unnamed event')}:")
        for error in invalid['errors']:
            print(f"  * {error}")