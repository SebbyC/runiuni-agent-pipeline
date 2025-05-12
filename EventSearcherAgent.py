# EventSearcherAgent.py

import asyncio
import re
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from agents import Agent, WebSearchTool, Runner
from agents.model_settings import ModelSettings

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('EventSearcherAgent')

# Check for OpenAI API key
if not os.environ.get("OPENAI_API_KEY"):
    logger.warning("OPENAI_API_KEY environment variable is not set. The agent will not function correctly.")

# Create folder for saving outputs
OUTPUTS_FOLDER = Path("EventSearcherAgentOutputs")
OUTPUTS_FOLDER.mkdir(exist_ok=True)

# Define the agent with specific instructions for structured event data
event_search_agent = Agent(
    name="EventSearcher",
    instructions="""
    You are an event search assistant specialized in finding upcoming events based on location.
    For any location query, search the web to find 5 upcoming events in that area. Focus on:
    1. Concerts, festivals, and live music
    2. Sports events
    3. Arts and cultural events
    4. Food and drink festivals
    5. Community events and fundraisers
    
    For each event, extract and provide the following information:
    - Title of the event
    - Date and time (provide exact start_date, start_time, end_date, end_time in format YYYY-MM-DD and HH:MM:SS)
    - Venue or location (include specific venue name)
    - Full address of the venue (provide as much detail as possible)
    - City, state, and country
    - Brief description (1-2 sentences, maximum 150 characters)
    - Link to the event page if available
    
    Always prioritize events happening in the near future (next 2-3 months).
    
    IMPORTANT: Find EXACTLY 5 events. Return ONLY a JSON array with these 5 events - no introductory text, 
    no conclusions, no explanations. Each event should have these exact keys:
    "title", "start_date", "start_time", "end_date", "end_time", "venue", "address", "city", "state", "country", 
    "description", "url"
    
    For dates, use YYYY-MM-DD format.
    For times, use HH:MM:SS format (24-hour).
    If end date is unknown, use the same as start date.
    If end time is unknown, use 23:59:59.
    The address field should contain the full venue address when available.
    
    KEEP YOUR RESPONSE CONCISE. DO NOT EXCEED 5 EVENTS.
    """,
    tools=[WebSearchTool()],
    model_settings=ModelSettings(
        temperature=0.2,
        tool_choice="required",  # Force usage of the WebSearchTool
        max_tokens=2500  # Explicitly limit the response size
    ),
)

def save_output(location: str, stage: str, content: Any) -> str:
    """
    Save agent output or parsed events to a file for debugging.
    
    Args:
        location: The location being searched
        stage: The processing stage (e.g., "raw_output", "parsed_events")
        content: The content to save
    
    Returns:
        Path to the saved file
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_location = location.replace(", ", "_").replace(" ", "_")
    filename = f"{safe_location}_{stage}_{timestamp}.json"
    filepath = OUTPUTS_FOLDER / filename
    
    with open(filepath, "w", encoding="utf-8") as f:
        if isinstance(content, str):
            f.write(content)
        else:
            json.dump(content, f, indent=2)
    
    logger.info(f"Saved {stage} to {filepath}")
    return str(filepath)

def fix_incomplete_json(json_str: str) -> str:
    """
    Attempt to fix incomplete JSON, like missing closing brackets.
    
    Args:
        json_str: A potentially incomplete JSON string
        
    Returns:
        Fixed JSON string if possible, original string otherwise
    """
    # Count opening and closing brackets
    open_brackets = json_str.count('[')
    close_brackets = json_str.count(']')
    open_braces = json_str.count('{')
    close_braces = json_str.count('}')
    
    # Fix missing closing brackets if needed
    if open_brackets > close_brackets:
        json_str += ']' * (open_brackets - close_brackets)
        logger.info(f"Added {open_brackets - close_brackets} missing closing brackets")
    
    # Fix missing closing braces if needed
    if open_braces > close_braces:
        json_str += '}' * (open_braces - close_braces)
        logger.info(f"Added {open_braces - close_braces} missing closing braces")
    
    # Handle truncated JSON at a comma
    if json_str.rstrip().endswith(','):
        json_str = json_str.rstrip()[:-1]  # Remove trailing comma
        logger.info("Removed trailing comma")
    
    # Check and fix truncated property
    if '"' in json_str:
        last_quote_pos = json_str.rfind('"')
        last_brace_pos = json_str.rfind('}')
        last_bracket_pos = json_str.rfind(']')
        
        # If the last quote is after the last closing structure, we might have a truncated property
        if last_quote_pos > max(last_brace_pos, last_bracket_pos):
            # Find the last valid closing structure position
            if last_brace_pos > last_bracket_pos:
                json_str = json_str[:last_brace_pos+1]
                if open_brackets > close_brackets:
                    json_str += ']'
            else:
                json_str = json_str[:last_bracket_pos+1]
            
            logger.info("Fixed truncated property")
    
    return json_str

def parse_event_data(event_text: str, location: str) -> List[Dict[str, Any]]:
    """
    Robust parser for extracting event data from the agent's output text.
    Tries multiple parsing strategies to handle different formats.
    
    Args:
        event_text: Raw text from the agent
        location: Location being searched, for saving debug files
        
    Returns:
        List of event dictionaries
    """
    # Save raw output for debugging
    save_output(location, "raw_output", event_text)
    
    events = []
    parse_method = "none"
    
    # Strategy 1: Look for JSON array in markdown code block
    json_block_match = re.search(r'```(?:json)?\s*(\[\s*\{.*?\}\s*\](?:\s*\})?)```', event_text, re.DOTALL)
    if json_block_match:
        try:
            json_content = json_block_match.group(1)
            json_content = fix_incomplete_json(json_content)
            events = json.loads(json_content)
            parse_method = "code_block"
            logger.info(f"Successfully parsed JSON from code block, found {len(events)} events")
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse JSON from code block: {e}")
    
    # Strategy 2: Look for JSON array directly in text with more flexible pattern
    if not events:
        json_match = re.search(r'\[\s*\{.*?\}\s*(?:\,\s*\{.*?\}\s*)*\]', event_text, re.DOTALL)
        if json_match:
            try:
                json_content = json_match.group(0)
                json_content = fix_incomplete_json(json_content)
                events = json.loads(json_content)
                parse_method = "regex_match"
                logger.info(f"Successfully parsed JSON from text, found {len(events)} events")
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse JSON from text: {e}")
    
    # Strategy 3: Try to parse entire text
    if not events:
        try:
            json_content = fix_incomplete_json(event_text)
            events = json.loads(json_content)
            if isinstance(events, list) and len(events) > 0:
                parse_method = "entire_text"
                logger.info(f"Successfully parsed entire text as JSON, found {len(events)} events")
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse entire text as JSON: {e}")
    
    # Strategy 4: Extract substring between first '[' and last ']' and fix it
    if not events:
        start_index = event_text.find('[')
        end_index = event_text.rfind(']')
        if start_index != -1 and end_index != -1 and start_index < end_index:
            try:
                json_substring = event_text[start_index:end_index+1]
                json_substring = fix_incomplete_json(json_substring)
                events = json.loads(json_substring)
                parse_method = "substring"
                logger.info(f"Successfully parsed JSON using substring extraction, found {len(events)} events")
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse JSON substring: {e}")
    
    # Strategy 5: Very aggressive approach - extract any JSON-like content
    if not events:
        try:
            # Find all potential objects
            objects = re.findall(r'(\{[^{}]*"title":[^{}]*\})', event_text)
            if objects:
                # Reconstruct a JSON array with these objects
                reconstructed_json = "[" + ",".join(objects) + "]"
                reconstructed_json = fix_incomplete_json(reconstructed_json)
                events = json.loads(reconstructed_json)
                parse_method = "object_extraction"
                logger.info(f"Successfully parsed JSON by extracting individual objects, found {len(events)} events")
        except (json.JSONDecodeError, re.error) as e:
            logger.warning(f"Failed to parse JSON with object extraction: {e}")
    
    if not events:
        logger.warning("Could not extract events from text using any parsing strategy")
        logger.warning(f"Raw output (first 500 chars): {event_text[:500]}")
    
    # Save parsed events for debugging
    save_output(
        location, 
        f"parsed_events_{parse_method}", 
        {"method": parse_method, "count": len(events), "events": events}
    )
    
    return events

async def search_events(location: str, limit: int = 5) -> List[Dict[str, Any]]:
    """
    Search for events at the given location using the AI agent
    
    Args:
        location: Location to search for events (e.g., "Pensacola, Florida")
        limit: Maximum number of events to return (default is now 5)
        
    Returns:
        List of event dictionaries with structured information
    """
    # Query the agent - explicitly request exactly 5 events with full venue addresses
    query = f"Find exactly 5 upcoming events in {location}, including full venue addresses, and return only a JSON array with no other text"
    logger.info(f"Searching for 5 events in {location} (with addresses)")
    
    # Save search query for debugging
    save_output(location, "search_query", query)
    
    # Run the agent
    result = await Runner.run(event_search_agent, query)
    
    # Get the raw output
    text_output = result.final_output
    
    # Try to parse the result
    events_data = parse_event_data(text_output, location)
    
    if not events_data:
        logger.warning(f"No events found for {location}")
    else:
        logger.info(f"Found {len(events_data)} events for {location}")
    
    # Return the events (already limited to 5 by the agent)
    return events_data

# Example usage if run directly
async def main():
    location = "Pensacola, Florida"
    events = await search_events(location)
    
    if events:
        print(f"Found {len(events)} events in {location}")
        print("\nExample event:")
        print(json.dumps(events[0], indent=2))
    else:
        print(f"No events found in {location}")

if __name__ == "__main__":
    # For regular Python environments
    asyncio.run(main())
    
    # For Jupyter/Colab environments (uncomment if needed)
    # import nest_asyncio
    # nest_asyncio.apply()
    # await main()