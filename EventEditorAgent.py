# EventEditorAgent.py

import asyncio
import logging
import json
import os
from typing import Dict, Any, List, Optional
# Ensure the 'agents' library is installed and configured correctly
# pip install agents-dev # Or however the library is named/installed
try:
    from agents import Agent, Runner
    from agents.model_settings import ModelSettings
except ImportError:
    logging.error("The 'agents' library is not installed or accessible. Please install it.")
    # Define dummy classes to allow the script to load without the library for basic structure review
    class Agent:
        def __init__(self, name, instructions, model_settings, **kwargs): pass
    class Runner:
        @staticmethod
        async def run(agent, prompt):
            logging.warning("Dummy Runner.run called. 'agents' library not fully functional.")
            # Simulate a response structure
            class DummyResult:
                final_output = "Dummy description generated because 'agents' library is missing."
            await asyncio.sleep(0.1) # Simulate async work
            return DummyResult()
    class ModelSettings:
        def __init__(self, temperature, max_tokens): pass


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('EventEditorAgent')

# --- API Key Handling ---
# Ensure the OpenAI API key is set via environment variable.
# The 'agents' library or the underlying OpenAI client library typically
# looks for the OPENAI_API_KEY environment variable automatically.
# We add an explicit check here for clarity and early failure if needed.
if "OPENAI_API_KEY" not in os.environ:
    logger.warning(
        "OPENAI_API_KEY environment variable not found. "
        "The agent WILL likely fail if it requires OpenAI API access for description generation. "
        "Ensure the key is set in your environment (e.g., export OPENAI_API_KEY='your_key')."
    )
    # Depending on requirements, you might want to raise an error immediately:
    # raise ValueError("FATAL: OPENAI_API_KEY environment variable is not set.")
# --- End API Key Handling ---


# Define the agent with specific instructions for generating event descriptions
event_editor_agent = Agent(
    name="EventEditor",
    instructions="""
    You are an event description generator specialized in creating engaging, accurate descriptions
    for events based on the available information. Your task is to enhance incomplete event data
    by filling in missing fields, with a primary focus on creating compelling descriptions.

    Given partial event information, you will:

    1. Generate a concise but informative description (1-3 sentences, 100-200 characters)
    2. Fill in any other missing essential fields when possible based *only* on the provided context.
    3. Ensure the content is factual and based on the provided information.

    When writing descriptions:
    - Highlight the key elements of the event (what, who [if known], why)
    - Include the venue and location context if relevant and provided
    - Mention any special features or notable aspects if provided
    - Keep the tone appropriate for the event type
    - Be concise but informative (strictly 100-200 characters)

    Your output should be a complete JSON object representing the enhanced event data if asked to fill multiple fields.
    If specifically asked *only* for the description, return *only* the description text, nothing else.

    IMPORTANT: Do not invent specific details like performers, speakers, precise activities, exact times, or specific costs unless
    they are explicitly mentioned in the original data. Use general descriptions that are highly likely to be accurate
    based *only* on the event title, venue, and type provided. If essential details are missing, generate a description
    that reflects this uncertainty (e.g., "Join us for [Event Title] at [Venue]. More details coming soon!").
    """,
    model_settings=ModelSettings(
        temperature=0.7,  # Slightly higher for more creative but grounded descriptions
        max_tokens=500     # Reduced max_tokens as we expect concise output
    ),
    # Note: If the 'agents' library requires the API key to be passed explicitly,
    # you might modify the Agent initialization like this:
    # api_key=os.environ.get("OPENAI_API_KEY"),
    # However, most modern libraries handle this automatically via environment variables.
)

async def generate_event_description(event: Dict[str, Any]) -> str:
    """
    Generate a description for an event using the AI agent.

    Args:
        event: Event dictionary with available information

    Returns:
        Generated description string or a default if generation fails.
    """
    # Check if API key is available before proceeding
    if "OPENAI_API_KEY" not in os.environ:
        logger.error("Cannot generate description: OPENAI_API_KEY is not set.")
        return "Event description generation requires API key. Please check configuration."

    event_name = event.get('name', event.get('title', ''))
    if not event_name:
        logger.warning("Cannot generate description for event with no name or title.")
        return "Details for this event are being finalized. Check back soon!"

    # Create a summary of the event to give context to the agent
    # Filter out None or empty values for a cleaner summary
    summary_parts = [
        f"Title: {event_name}",
        f"Date/Time: {event.get('start_date', 'Unknown')} at {event.get('start_time', 'Unknown')}",
        f"Venue: {event.get('venue', 'Unknown')}",
        # Combine location parts carefully
        f"Location: {', '.join(filter(None, [event.get('city', ''), event.get('state', ''), event.get('country', '')])) or 'Unknown'}",
        f"Current Description: {event.get('description') if event.get('description') else '(Missing)'}"
    ]
    event_summary = "\n".join(summary_parts)

    # Prepare the prompt for the agent, specifically asking only for the description
    prompt = f"""
    Based *only* on the information below, generate a concise, engaging description (1-3 sentences, 100-200 characters) for the event.
    Return *only* the description text, nothing else. Do not add any preamble or explanation.

    Event Information:
    {event_summary}

    Description:
    """

    logger.info(f"Generating description for: {event_name}")

    try:
        # Run the agent
        result = await Runner.run(event_editor_agent, prompt)

        # Extract the description from the agent's response
        description = result.final_output.strip().replace('"', '') # Clean quotes just in case

        # Basic validation
        if not description or len(description) < 10:
            logger.warning(f"Generated description seems too short or invalid: '{description}'. Using default.")
            description = f"Join us for {event_name} at {event.get('venue', 'the venue')}. More details coming soon."
        elif len(description) > 250: # Allow slightly more than 200 just in case, but log
             logger.warning(f"Generated description is long ({len(description)} chars). Truncating may occur elsewhere.")
             # You could truncate here if needed: description = description[:200] + "..."

        logger.info(f"Generated description ({len(description)} chars): {description[:70]}...")
        return description

    except Exception as e:
        logger.error(f"Error generating description for '{event_name}' using AI: {e}")
        logger.info(f"Providing default description for '{event_name}' due to generation error.")
        # Provide a safe fallback description
        fallback_desc = f"Join us for {event_name}"
        if event.get('venue'):
            fallback_desc += f" at {event.get('venue')}"
        fallback_desc += ". Check back soon for more details!"
        return fallback_desc


async def enhance_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enhance an event by adding or improving its description and filling/normalizing other fields.

    Args:
        event: Original event dictionary

    Returns:
        Enhanced event dictionary
    """
    # Make a copy of the event to avoid modifying the original
    enhanced_event = event.copy()

    # --- Name/Title Normalization ---
    if not enhanced_event.get('name') and enhanced_event.get('title'):
        enhanced_event['name'] = enhanced_event.get('title')
    if not enhanced_event.get('title') and enhanced_event.get('name'):
        enhanced_event['title'] = enhanced_event.get('name')
    # Ensure there's at least a placeholder if both are missing
    if not enhanced_event.get('name'):
        enhanced_event['name'] = "Untitled Event"
        enhanced_event['title'] = "Untitled Event" # Keep them consistent

    # --- Description Enhancement ---
    current_desc = enhanced_event.get('description', '').strip()
    if not current_desc:
        logger.info(f"Event '{enhanced_event['name']}' needs a description. Generating...")
        # Generate description only if needed
        generated_description = await generate_event_description(enhanced_event)
        enhanced_event['description'] = generated_description
    else:
        # Optional: Could add logic here to regenerate if description is too short/generic
        # e.g., if len(current_desc) < 20: ...
        pass

    # --- Tag IDs ---
    if 'tag_ids' not in enhanced_event or not enhanced_event['tag_ids']:
        # Consider deriving tags from title/description later if possible
        enhanced_event['tag_ids'] = [1]  # Default to a generic event tag (e.g., 'Music Events' if 1 means that)
        logger.debug(f"Set default tag_ids for event {enhanced_event['name']}")

    # --- Image URL Normalization ---
    image = enhanced_event.get('image')
    image_url = enhanced_event.get('imageURL')
    if image and not image_url:
        enhanced_event['imageURL'] = image
    elif image_url and not image:
        enhanced_event['image'] = image_url
    # If neither exists, leave them as potentially None/missing

    # --- District ---
    if enhanced_event.get('district') is None: # Check specifically for None, allow empty string ''
        # Default based on location if possible
        state = str(enhanced_event.get('state', '')).strip().upper()
        city = str(enhanced_event.get('city', '')).strip()
        if state in ['FLORIDA', 'FL'] and city.lower() == 'pensacola':
             enhanced_event['district'] = 'Escambia County'
             logger.debug(f"Set default district 'Escambia County' for Pensacola event {enhanced_event['name']}")
        else:
             enhanced_event['district'] = '' # Use empty string instead of null for potentially required fields
             logger.debug(f"Set default empty district for event {enhanced_event['name']}")

    # --- Coordinate Normalization and Validation ---
    lat, lng = None, None
    lat_keys = ['latitude', 'lat']
    lng_keys = ['longitude', 'lng']

    # Try to find valid lat/lng from available keys
    for key in lat_keys:
        val = enhanced_event.get(key)
        if val is not None:
            try:
                lat = float(val)
                break # Found valid lat
            except (ValueError, TypeError):
                logger.warning(f"Invalid value '{val}' for key '{key}' in event '{enhanced_event['name']}'. Ignoring.")
    for key in lng_keys:
        val = enhanced_event.get(key)
        if val is not None:
            try:
                lng = float(val)
                break # Found valid lng
            except (ValueError, TypeError):
                logger.warning(f"Invalid value '{val}' for key '{key}' in event '{enhanced_event['name']}'. Ignoring.")

    # If valid coordinates were found, ensure all standard keys are set
    if lat is not None and lng is not None:
        # Add basic range check
        if -90 <= lat <= 90 and -180 <= lng <= 180:
            enhanced_event['latitude'] = lat
            enhanced_event['longitude'] = lng
            enhanced_event['lat'] = lat
            enhanced_event['lng'] = lng
            logger.debug(f"Normalized coordinates for event {enhanced_event['name']}")
        else:
            logger.warning(f"Coordinates ({lat}, {lng}) out of valid range for event '{enhanced_event['name']}'. Setting to default.")
            lat, lng = None, None # Invalidate them

    # If coordinates are still missing or were invalidated, try setting defaults
    if lat is None or lng is None:
        logger.warning(f"Missing or invalid coordinates for event '{enhanced_event['name']}'. Attempting default based on location.")
        is_pensacola = enhanced_event.get('city', '').lower() == 'pensacola' or \
                       'pensacola' in enhanced_event.get('address', '').lower()

        if is_pensacola:
            default_lat, default_lng = 30.421309, -87.216915
            enhanced_event['latitude'] = default_lat
            enhanced_event['longitude'] = default_lng
            enhanced_event['lat'] = default_lat
            enhanced_event['lng'] = default_lng
            logger.info(f"Set default Pensacola coordinates for {enhanced_event['name']}")
        else:
            # Set to None if no default is applicable; avoid using (0,0) unless specifically intended
            enhanced_event['latitude'] = None
            enhanced_event['longitude'] = None
            enhanced_event['lat'] = None
            enhanced_event['lng'] = None
            logger.warning(f"Could not determine default coordinates for {enhanced_event['name']}. Coordinates set to None.")

    # --- Final Null Value Check (Optional) ---
    # Remove keys with None values if they might cause issues downstream,
    # but be cautious as None might be acceptable/intended.
    # Example: If 'district' MUST NOT be null, we already defaulted it to ''.
    # keys_to_remove_if_null = ['some_optional_field']
    # for key in keys_to_remove_if_null:
    #     if key in enhanced_event and enhanced_event[key] is None:
    #         del enhanced_event[key]

    return enhanced_event


async def enhance_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Enhance multiple events in parallel.

    Args:
        events: List of event dictionaries

    Returns:
        List of enhanced event dictionaries
    """
    if not events:
        return []

    logger.info(f"Enhancing {len(events)} events with the EventEditorAgent")

    # Run enhancements concurrently
    tasks = [enhance_event(event) for event in events]
    # return_exceptions=True allows processing to continue even if one task fails
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Process results, filtering out potential exceptions and logging them
    enhanced_events_list = []
    for i, result in enumerate(results):
        original_event_name = events[i].get("name", events[i].get("title", f"Event at index {i}"))
        if isinstance(result, Exception):
            logger.error(f"Error enhancing event '{original_event_name}': {result}", exc_info=False) # Set exc_info=True for full traceback
            # Decide how to handle failures: skip, include original, include partially enhanced?
            # Option: Append original event if enhancement failed
            # enhanced_events_list.append(events[i])
            # Option: Skip the failed event
            continue
        else:
            enhanced_events_list.append(result)

    successful_count = len(enhanced_events_list)
    failed_count = len(events) - successful_count
    logger.info(f"Successfully enhanced {successful_count} events.")
    if failed_count > 0:
        logger.warning(f"Failed to enhance {failed_count} events.")

    return enhanced_events_list


async def fix_invalid_events(invalid_events_info: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Attempt to fix invalid events based on their error messages using enhance_event logic.
    This assumes 'enhance_event' contains the necessary fixes for common validation errors.

    Args:
        invalid_events_info: List of dictionaries, each containing 'event' (the data)
                               and 'errors' (list of error strings).

    Returns:
        List of potentially fixed event dictionaries. These might still not pass validation.
    """
    if not invalid_events_info:
        return []

    logger.info(f"Attempting to fix {len(invalid_events_info)} invalid events using enhancement logic.")

    tasks = []
    original_event_data = [] # Keep track of original data for logging/debugging

    for invalid_item in invalid_events_info:
        event_data = invalid_item.get('event')
        errors = invalid_item.get('errors', [])

        if not event_data:
            logger.warning("Skipping invalid item with no event data.")
            continue

        event_name = event_data.get('name', event_data.get('title', 'Unknown Event'))
        logger.info(f"Queueing fix attempt for event: '{event_name}' with errors: {', '.join(errors)}")

        # We reuse enhance_event as it's designed to fill gaps and normalize data.
        # Pass a copy to avoid modifying the dictionary within invalid_events_info
        tasks.append(enhance_event(event_data.copy()))
        original_event_data.append(event_data) # Store original for comparison if needed

    if not tasks:
        return []

    # Run the enhancements (fix attempts) in parallel
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Process results
    potentially_fixed_events = []
    for i, result in enumerate(results):
        event_name = original_event_data[i].get('name', original_event_data[i].get('title', f'Event at index {i}'))
        if isinstance(result, Exception):
            logger.error(f"Error trying to fix/enhance event '{event_name}': {result}", exc_info=False)
            # Decide if you want to include the original event data even if fixing failed
            # potentially_fixed_events.append(original_event_data[i])
        else:
            logger.info(f"Applied potential fixes/enhancements to event: '{event_name}'")
            potentially_fixed_events.append(result) # Add the potentially fixed event

    logger.info(f"Finished attempting fixes. Produced {len(potentially_fixed_events)} potentially fixed events.")
    return potentially_fixed_events


# Example usage if run directly
async def main():
    # Test case 1: Event missing description, has null coords but also lat/lng
    sample_event_1 = {
        "title": "Stellar Spectrum Society Presents: Yewz",
        "start_date": "2025-03-29", "start_time": "20:00:00",
        "end_date": "2025-03-30", "end_time": "02:00:00",
        "venue": "850 Fusion",
        "address": "7250 Plantation Rd, Pensacola, FL 32504, USA",
        "city": "Pensacola", "state": "Florida", "country": "United States",
        "description": "",  # Empty description
        "url": "https://www.stellarspectrumsociety.com/event-details/yewz",
        "image": "https://...", "imageURL": "https://...",
        "latitude": None, "longitude": None, # Null coordinates
        "lat": 30.497538, "lng": -87.2260669, # But has these valid ones
        "tag_ids": [] # Empty tags
    }

    # Test case 2: Event with minimal info, missing coords, tags, district
    sample_event_2 = {
        "name": "Community Workshop",
        "start_date": "2024-09-15", "start_time": "14:00:00",
        "venue": "Downtown Library",
        "city": "Anytown", "state": "CA",
        "description": "A brief workshop description.", # Has a description
        # Missing coords, tags, district, etc.
    }

    # Test case 3: Event with invalid coords
    sample_event_3 = {
        "name": "Data Glitch Fest",
        "start_date": "2024-10-01", "start_time": "10:00:00",
        "venue": "Server Room B",
        "city": "Tech City", "state": "TX",
        "description": "",
        "latitude": "invalid", "longitude": 999 # Invalid lat format, invalid lng value
    }

    print("--- Enhancing Single Event (Event 1) ---")
    enhanced_event_1 = await enhance_event(sample_event_1)
    print("\nOriginal event 1 (subset):")
    print(json.dumps({k: v for k, v in sample_event_1.items() if k in ['title', 'description', 'latitude', 'longitude', 'lat', 'lng', 'tag_ids', 'district']}, indent=2))
    print("\nEnhanced event 1 (subset):")
    print(json.dumps({k: v for k, v in enhanced_event_1.items() if k in ['title', 'description', 'latitude', 'longitude', 'lat', 'lng', 'tag_ids', 'district']}, indent=2))

    print("\n--- Enhancing Multiple Events ---")
    events_to_enhance = [sample_event_1, sample_event_2, sample_event_3]
    enhanced_list = await enhance_events(events_to_enhance)
    print(f"\nSuccessfully processed {len(enhanced_list)} events:")
    for i, event in enumerate(enhanced_list):
         print(f"\nEnhanced Event {i+1} ('{event.get('name')}') (subset):")
         print(json.dumps({k: v for k, v in event.items() if k in ['name', 'title', 'description', 'latitude', 'longitude', 'lat', 'lng', 'tag_ids', 'district']}, indent=2))

    print("\n--- Fixing Invalid Events (Simulated) ---")
    # Simulate finding invalid events after an API call attempt
    invalid_event_info = [
         {
             "event": sample_event_1, # Missing desc, null coords (but had lat/lng), empty tags
             "errors": ["description cannot be empty", "tag_ids cannot be empty"] # Example errors
         },
         {
             "event": sample_event_3, # Invalid coords, missing desc
             "errors": ["description cannot be empty", "Invalid coordinates"]
         }
    ]

    fixed_list = await fix_invalid_events(invalid_event_info)
    print(f"\nAttempted to fix {len(invalid_event_info)} events, resulting in {len(fixed_list)} potentially fixed events:")
    for i, event in enumerate(fixed_list):
         print(f"\nPotentially Fixed Event {i+1} ('{event.get('name')}') (subset):")
         print(json.dumps({k: v for k, v in event.items() if k in ['name', 'title', 'description', 'latitude', 'longitude', 'lat', 'lng', 'tag_ids', 'district']}, indent=2))


if __name__ == "__main__":
    # To run the example with AI description generation, ensure OPENAI_API_KEY is set in your environment:
    # E.g., in Linux/macOS: export OPENAI_API_KEY='your_actual_api_key'
    # E.g., in Windows (cmd): set OPENAI_API_KEY=your_actual_api_key
    # E.g., in Windows (PowerShell): $env:OPENAI_API_KEY='your_actual_api_key'
    # Then run: python EventEditorAgent.py
    # If the key is not set, it will log warnings and use default/fallback logic.
    asyncio.run(main())