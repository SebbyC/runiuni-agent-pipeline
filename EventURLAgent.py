# EventURLAgent.py

import asyncio
import os
import json
import logging
import requests
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, urljoin
import re
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('EventURLAgent')

class EventURLAgent:
    """
    Agent for scanning URLs to extract event data.
    Can use predefined URL sources from a JSON file or directly provided URLs.
    """

    def __init__(self, sources_file: Optional[str] = None, urls: Optional[List[str]] = None):
        """
        Initialize the EventURLAgent with URLs from a file or direct list.

        Args:
            sources_file: Path to a JSON file containing URL sources
            urls: Direct list of URLs to scan
        """
        self.sources_file = sources_file
        self.direct_urls = urls or []
        self.url_sources = []
        self.events = []

        # Load sources if file provided
        if sources_file:
            self._load_sources()

    def _load_sources(self) -> None:
        """Load URL sources from the specified JSON file."""
        try:
            if not os.path.exists(self.sources_file):
                logger.error(f"Sources file not found: {self.sources_file}")
                return

            with open(self.sources_file, 'r') as f:
                data = json.load(f)

            # Check for expected format
            if isinstance(data, list):
                self.url_sources = data
            elif isinstance(data, dict) and 'sources' in data:
                self.url_sources = data['sources']
            else:
                logger.error(f"Invalid format in sources file: {self.sources_file}")

            logger.info(f"Loaded {len(self.url_sources)} URL sources from {self.sources_file}")

        except json.JSONDecodeError:
            logger.error(f"Invalid JSON format in sources file: {self.sources_file}")
        except Exception as e:
            logger.error(f"Error loading sources file: {str(e)}")

    def get_all_urls(self) -> List[str]:
        """
        Get all URLs to scan from both file sources and direct URLs.

        Returns:
            List of URLs to scan
        """
        all_urls = []

        # Process URL sources from file
        for source in self.url_sources:
            if isinstance(source, str):
                all_urls.append(source)
            elif isinstance(source, dict) and 'url' in source:
                all_urls.append(source['url'])

        # Add directly provided URLs
        all_urls.extend(self.direct_urls)

        # Remove duplicates while preserving order
        unique_urls = []
        for url in all_urls:
            if url not in unique_urls:
                unique_urls.append(url)

        return unique_urls

    def extract_domain(self, url: str) -> str:
        """
        Extract the domain name from a URL.

        Args:
            url: URL to extract domain from

        Returns:
            Domain name
        """
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        return domain

    async def fetch_url(self, url: str) -> Optional[str]:
        """
        Fetch the content of a URL.

        Args:
            url: URL to fetch

        Returns:
            HTML content if successful, None otherwise
        """
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Cache-Control': 'max-age=0'
            }

            # Use asyncio to run requests.get in a separate thread to avoid blocking
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,  # Use default executor (ThreadPoolExecutor)
                lambda: requests.get(url, headers=headers, timeout=15) # Increased timeout
            )

            # Raise exception for bad status codes
            response.raise_for_status()

            # Check content type
            content_type = response.headers.get('content-type', '').lower()
            if 'html' not in content_type:
                logger.warning(f"Skipping non-HTML content at {url} (Content-Type: {content_type})")
                return None

            return response.text

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout fetching URL {url}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching URL {url}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching URL {url}: {str(e)}")
            return None

    def extract_event_data_from_html(self, html: str, url: str) -> List[Dict[str, Any]]:
        """
        Extract event data from HTML content.
        Different websites have different structures, so this uses heuristics.

        Args:
            html: HTML content to parse
            url: Original URL for reference

        Returns:
            List of extracted events
        """
        events = []
        domain = self.extract_domain(url)

        try:
            soup = BeautifulSoup(html, 'html.parser')

            # 1. LD+JSON (Schema.org) extraction - the best case
            ld_json_scripts = soup.find_all('script', type='application/ld+json')
            for script in ld_json_scripts:
                try:
                    # Check if script has content
                    if not script.string:
                        continue

                    # Clean script content (remove comments, etc.)
                    script_content = script.string.strip()
                    # Basic cleaning for common issues like trailing commas
                    script_content = re.sub(r',\s*([}\]])', r'\1', script_content)

                    data = json.loads(script_content)

                    # Handle arrays or single objects
                    if isinstance(data, list):
                        items = data
                    else:
                        items = [data]

                    for item in items:
                        # Look for Event schema type (or subtypes)
                        item_type = item.get('@type', '')
                        if isinstance(item_type, list): # Handle type arrays
                            is_event = any(t in ['Event', 'SocialEvent', 'Festival', 'ConcertEvent', 'TheaterEvent', 'VisualArtsEvent', 'MusicEvent', 'SportsEvent', 'EducationEvent', 'BusinessEvent'] for t in item_type)
                        else:
                             is_event = item_type in ['Event', 'SocialEvent', 'Festival', 'ConcertEvent', 'TheaterEvent', 'VisualArtsEvent', 'MusicEvent', 'SportsEvent', 'EducationEvent', 'BusinessEvent']

                        if isinstance(item, dict) and is_event:
                            event = self._parse_schema_event(item, url, domain)
                            if event:
                                events.append(event)
                except json.JSONDecodeError as e:
                     logger.warning(f"Invalid LD+JSON detected in {url}: {e}. Content: {script.string[:100]}...")
                except Exception as e:
                    logger.warning(f"Error parsing LD+JSON from {url}: {str(e)}")

            # 2. If no LD+JSON events found, try site-specific heuristics
            if not events:
                logger.info(f"No LD+JSON events found for {url}. Trying site-specific heuristics.")
                # More heuristics for common event sites
                if 'eventbrite' in domain:
                    events.extend(self._extract_eventbrite_events(soup, url))
                elif 'meetup' in domain:
                    events.extend(self._extract_meetup_events(soup, url))
                elif 'ticketmaster' in domain:
                    events.extend(self._extract_ticketmaster_events(soup, url))
                elif 'facebook.com/events' in url:
                    events.extend(self._extract_facebook_events(soup, url))
                # Add more site-specific extractors here
                # elif 'someotherdomain.com' in domain:
                #    events.extend(self._extract_someotherdomain_events(soup, url))

            # 3. If still no events, try generic heuristics
            if not events:
                 logger.info(f"No site-specific events found for {url}. Trying generic heuristics.")
                 events.extend(self._extract_generic_events(soup, url, domain))

        except Exception as e:
            logger.error(f"Error extracting events from {url}: {str(e)}", exc_info=True)

        # Basic deduplication based on title and start date
        unique_events = []
        seen_keys = set()
        for event in events:
            key = (event.get('title', '').strip().lower(), event.get('start_date', ''))
            if key[0] and key[1] and key not in seen_keys:
                unique_events.append(event)
                seen_keys.add(key)
            elif not key[1]: # If start_date is missing, just use title
                 key = (event.get('title', '').strip().lower(),)
                 if key[0] and key not in seen_keys:
                    unique_events.append(event)
                    seen_keys.add(key)


        logger.info(f"Extracted {len(unique_events)} unique events from {url}")
        return unique_events

    def _parse_schema_event(self, item: Dict[str, Any], url: str, domain: str) -> Optional[Dict[str, Any]]:
        """
        Parse an Event from Schema.org LD+JSON data.

        Args:
            item: LD+JSON item with @type: Event
            url: Original URL
            domain: Site domain

        Returns:
            Formatted event dict if successful, None otherwise
        """
        try:
            # Extract dates and times
            start_date, start_time = self._parse_datetime(item.get('startDate', ''))
            end_date, end_time = self._parse_datetime(item.get('endDate', ''))

            # Set end date to start date if not provided
            if start_date and not end_date:
                end_date = start_date
                end_time = "23:59:59" # Default end time if only start date is known

            # Extract location information
            location = item.get('location')
            venue = ''
            address = ''
            city = ''
            state = ''
            country = '' # Default to empty, try to infer
            latitude = None
            longitude = None


            if isinstance(location, list): # Handle multiple locations, take the first one
                location = location[0] if location else None

            if isinstance(location, dict):
                location_type = location.get('@type', '')
                venue = location.get('name', '')

                # Location can be Place or PostalAddress
                address_obj = location.get('address', {})
                if isinstance(address_obj, str): # If address is just a string
                     address = address_obj
                elif isinstance(address_obj, dict):
                    address_parts = []
                    street = address_obj.get('streetAddress', '')
                    if street: address_parts.append(street)
                    city = address_obj.get('addressLocality', '')
                    state = address_obj.get('addressRegion', '')
                    postal_code = address_obj.get('postalCode', '')
                    country = address_obj.get('addressCountry', '')

                    if city: address_parts.append(city)
                    if state: address_parts.append(state)
                    if postal_code: address_parts.append(postal_code)
                    address = ', '.join(filter(None, address_parts))
                elif not venue and location.get('name'): # If location name is present but no address obj
                    address = location.get('name') # Sometimes name contains the full address

                # GeoCoordinates
                geo = location.get('geo')
                if isinstance(geo, dict):
                    latitude = geo.get('latitude')
                    longitude = geo.get('longitude')

            elif isinstance(location, str): # Location is just a string
                address = location
                # Try to extract venue if it looks like "Venue Name, Address..."
                parts = address.split(',', 1)
                if len(parts) > 1 and len(parts[0]) < 50: # Heuristic for venue name length
                    venue = parts[0].strip()
                    # address = parts[1].strip() # Keep full string in address for now


            # Extract City/State/Country from address string if not found yet
            if address and not city and not state:
                 city, state = self._extract_city_state_from_text(address)

            if not country:
                country = "US" if state and len(state) == 2 else "" # Assume US if state looks like abbreviation


            # Extract image
            image = item.get('image')
            image_url = ''
            if isinstance(image, str):
                image_url = image
            elif isinstance(image, list) and image:
                image_url = image[0] # Take the first image
            elif isinstance(image, dict):
                 image_url = image.get('url', '')


            # Extract description
            description = item.get('description', '')
            if isinstance(description, str):
                # Clean up description: remove HTML tags
                desc_soup = BeautifulSoup(description, 'html.parser')
                description = desc_soup.get_text(separator=' ', strip=True)
            else:
                description = '' # Or handle other types if needed


            # Event URL - prefer specific event URL if different from source page
            event_url = item.get('url', url)

            # Organizer
            organizer = item.get('organizer')
            organizer_name = ''
            if isinstance(organizer, dict):
                organizer_name = organizer.get('name', '')
            elif isinstance(organizer, list) and organizer:
                 if isinstance(organizer[0], dict):
                    organizer_name = organizer[0].get('name', '')


            # Create event object
            event = {
                "title": item.get('name', ''),
                "start_date": start_date,
                "start_time": start_time or "00:00:00", # Default start time if missing
                "end_date": end_date,
                "end_time": end_time or "23:59:59", # Default end time if missing
                "venue": venue,
                "address": address,
                "city": city,
                "state": state,
                "country": country,
                "latitude": latitude,
                "longitude": longitude,
                "description": description[:1000],  # Limit description length
                "url": event_url,
                "image": image_url,
                "organizer": organizer_name,
                "source_url": url,
                "source_domain": domain,
                "source_format": "ld+json"
            }

            # Only return if we have the minimum required fields
            if event['title'] and event['start_date']:
                return event

        except Exception as e:
            logger.warning(f"Error parsing Schema.org event from {url}: {str(e)}")

        return None

    def _parse_datetime(self, datetime_str: Any) -> tuple:
        """
        Parse a datetime string (or dict) into date and time components.

        Args:
            datetime_str: ISO datetime string, date string, or object

        Returns:
            Tuple of (date_str, time_str) in YYYY-MM-DD and HH:MM:SS formats
        """
        if not datetime_str:
            return ('', '')

        # Handle potential objects (e.g., {'@type': 'DateTime', 'value': '...'})
        if isinstance(datetime_str, dict):
            datetime_str = datetime_str.get('value', '')
            if not datetime_str:
                return ('', '')

        if not isinstance(datetime_str, str):
             logger.warning(f"Unexpected datetime format: {type(datetime_str)}, value: {datetime_str}")
             return ('', '')

        datetime_str = datetime_str.strip()

        # Common formats to try
        formats = [
            '%Y-%m-%dT%H:%M:%S%z',  # ISO 8601 with timezone
            '%Y-%m-%dT%H:%M:%S',     # ISO 8601 without timezone
            '%Y-%m-%d %H:%M:%S%z',
            '%Y-%m-%d %H:%M:%S',
            '%Y/%m/%d %H:%M:%S',
            '%m/%d/%Y %I:%M:%S %p', # 01/20/2024 02:30:00 PM
            '%m/%d/%Y %H:%M:%S',
            '%a, %d %b %Y %H:%M:%S %Z', # RFC 5322 format (e.g., 'Wed, 21 Oct 2015 07:28:00 GMT')
            '%a, %d %b %Y %H:%M:%S',
            '%B %d, %Y %I:%M %p', # January 20, 2024 2:30 PM
            '%b %d, %Y %I:%M %p', # Jan 20, 2024 2:30 PM
            '%Y-%m-%d',              # Date only
            '%Y/%m/%d',
            '%m/%d/%Y',
            '%B %d, %Y',          # January 20, 2024
            '%b %d, %Y',           # Jan 20, 2024
        ]

        # Handle timezone offsets like -05:00 or +0100
        datetime_str = datetime_str.replace('Z', '+00:00') # Replace Z with UTC offset
        if re.search(r'[+-]\d{4}$', datetime_str): # Handle +0100 format
            datetime_str = datetime_str[:-2] + ':' + datetime_str[-2:]
        # Handle T separator without timezone
        if 'T' in datetime_str and '+' not in datetime_str and '-' not in datetime_str[-6:]:
             # Check if it looks like it has time component
             if ':' in datetime_str.split('T')[1]:
                 pass # Looks like ISO without timezone
             else:
                 datetime_str = datetime_str.split('T')[0] # Treat as date only


        dt = None
        for fmt in formats:
            try:
                dt = datetime.strptime(datetime_str, fmt)
                # If timezone is naive, assume local (less ideal, but fallback)
                # Note: Schema.org usually includes offset or Z
                break # Success
            except ValueError:
                continue

        if dt:
            has_time = any(c in fmt for c in ['H', 'I', 'p', 'S'])
            date_part = dt.strftime('%Y-%m-%d')
            time_part = dt.strftime('%H:%M:%S') if has_time else ''
            return (date_part, time_part)
        else:
             # If standard parsing fails, try relaxed regex
             date_match = re.search(r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})', datetime_str)
             if date_match:
                 year, month, day = map(int, date_match.groups())
                 try:
                     date_part = datetime(year, month, day).strftime('%Y-%m-%d')

                     # Try to find time part nearby
                     time_match = re.search(r'(\d{1,2}):(\d{2})(?::(\d{2}))?\s?([AP]M)?', datetime_str, re.IGNORECASE)
                     if time_match:
                         hour, minute, second, ampm = time_match.groups()
                         hour, minute = int(hour), int(minute)
                         second = int(second) if second else 0
                         if ampm and ampm.lower() == 'pm' and hour != 12:
                             hour += 12
                         if ampm and ampm.lower() == 'am' and hour == 12:
                              hour = 0 # Midnight case
                         time_part = f"{hour:02d}:{minute:02d}:{second:02d}"
                         return (date_part, time_part)
                     else:
                         return (date_part, '') # Found date, no time
                 except ValueError:
                     pass # Invalid date components

             logger.debug(f"Could not parse datetime string: {datetime_str}")
             return ('', '')


    def _extract_text(self, element: Optional[BeautifulSoup], default: str = '') -> str:
        """Safely extract text from a BeautifulSoup element."""
        if element:
            return element.get_text(strip=True)
        return default

    def _extract_attr(self, element: Optional[BeautifulSoup], attr: str, default: str = '') -> str:
         """Safely extract an attribute from a BeautifulSoup element."""
         if element and element.has_attr(attr):
             return element[attr]
         return default

    def _extract_meta_content(self, soup: BeautifulSoup, property_name: str, default: str = '') -> str:
        """Extract content from a meta tag by property."""
        tag = soup.find('meta', property=property_name)
        if tag and tag.has_attr('content'):
            return tag['content'].strip()
        # Fallback for name attribute
        tag = soup.find('meta', attrs={'name': property_name})
        if tag and tag.has_attr('content'):
            return tag['content'].strip()
        return default

    def _clean_text(self, text: str) -> str:
        """Remove excessive whitespace and potentially unwanted chars."""
        if not text: return ''
        text = re.sub(r'\s+', ' ', text).strip()
        # Add more cleaning rules if needed
        return text

    def _normalize_url(self, base_url: str, target_url: Optional[str]) -> str:
        """Make relative URLs absolute."""
        if not target_url:
            return ''
        return urljoin(base_url, target_url)

    def _extract_city_state_from_text(self, text: str) -> tuple:
        """Attempt to extract City, State from a string."""
        if not text:
            return ('', '')

        # Regex for City, ST (allowing for variations in spacing and punctuation)
        # Matches common US states, Canadian provinces, and some international patterns
        # Prioritize longer state names first if overlapping patterns exist
        patterns = [
            # US: City, State Name
            r'([A-Za-z\s\.\'-]+)\s*,\s*([A-Za-z]{3,})\b',
             # US: City, ST Zip
            r'([A-Za-z\s\.\'-]+)\s*,\s*([A-Z]{2})\s+\d{5}(-\d{4})?\b',
            # US: City, ST
            r'([A-Za-z\s\.\'-]+)\s*,\s*([A-Z]{2})\b',
            # Canada: City, Province AB
            r'([A-Za-z\s\.\'-]+)\s*,\s*([A-Z]{2})\b', # Covers provinces too
            # Just City State (less reliable)
            # r'\b([A-Za-z\s\.\'-]+)\s+([A-Z]{2})\b'
        ]

        for pattern in patterns:
             match = re.search(pattern, text)
             if match:
                 city = match.group(1).strip().rstrip(',').strip()
                 state = match.group(2).strip()
                 # Basic validation
                 if len(city) > 1 and len(state) >= 2:
                     return (city, state)

        return ('', '') # No match found

    # Site-specific extractors
    def _extract_eventbrite_events(self, soup: BeautifulSoup, url: str) -> List[Dict[str, Any]]:
        """Extract event information from Eventbrite pages."""
        # Eventbrite often uses LD+JSON, so this is a fallback
        logger.info(f"Running Eventbrite specific extractor for {url}")
        events = []
        domain = self.extract_domain(url)

        try:
            # Event data is often embedded in a script tag, but NOT ld+json
            # Look for script containing "eventbriteEvent" or similar structure
            scripts = soup.find_all('script')
            event_data = None
            for script in scripts:
                if script.string and '"event":{' in script.string and '"name":' in script.string:
                    # This is fragile - needs careful extraction
                    # Example: window.__SERVER_DATA__ = {"API_CACHE": ..., "event": {...}};
                    match = re.search(r'"event"\s*:\s*({.*?})\s*(?:,|})', script.string)
                    if match:
                        try:
                            event_json_str = match.group(1)
                            # Fix potential issues like trailing commas before parsing
                            event_json_str = re.sub(r',\s*([}\]])', r'\1', event_json_str)
                            event_data = json.loads(event_json_str)
                            logger.info(f"Found potential event data in script tag for {url}")
                            break
                        except json.JSONDecodeError as e:
                            logger.warning(f"Failed to parse embedded JSON from Eventbrite script: {e}")
                            continue

            if event_data:
                title = event_data.get('name', '')
                start_dt_str = event_data.get('start', {}).get('utc')
                end_dt_str = event_data.get('end', {}).get('utc')
                start_date, start_time = self._parse_datetime(start_dt_str)
                end_date, end_time = self._parse_datetime(end_dt_str)

                venue_data = event_data.get('venue')
                venue, address, city, state, country, lat, lon = '', '', '', '', '', None, None
                if venue_data:
                    venue = venue_data.get('name', '')
                    addr_data = venue_data.get('address', {})
                    if addr_data:
                        address_parts = [
                            addr_data.get('address_1'),
                            addr_data.get('address_2'),
                            addr_data.get('city'),
                            addr_data.get('region'),
                            addr_data.get('postal_code'),
                            addr_data.get('country')
                        ]
                        address = ', '.join(filter(None, address_parts))
                        city = addr_data.get('city', '')
                        state = addr_data.get('region', '')
                        country = addr_data.get('country', '')
                    lat = venue_data.get('latitude')
                    lon = venue_data.get('longitude')

                description = event_data.get('description', {}).get('text', '')
                if not description: # Fallback to summary
                    description = event_data.get('summary', '')
                desc_soup = BeautifulSoup(description or '', 'html.parser')
                description = desc_soup.get_text(separator=' ', strip=True)

                image_data = event_data.get('logo')
                image_url = ''
                if image_data:
                     image_url = image_data.get('original', {}).get('url') or image_data.get('url')


                event_url = event_data.get('url', url)
                organizer_data = event_data.get('organizer')
                organizer = organizer_data.get('name', '') if organizer_data else ''


                if title and start_date:
                    event = {
                        "title": title,
                        "start_date": start_date,
                        "start_time": start_time or "00:00:00",
                        "end_date": end_date or start_date,
                        "end_time": end_time or "23:59:59",
                        "venue": venue,
                        "address": address,
                        "city": city,
                        "state": state,
                        "country": country,
                        "latitude": lat,
                        "longitude": lon,
                        "description": description[:1000],
                        "url": event_url,
                        "image": image_url,
                         "organizer": organizer,
                        "source_url": url,
                        "source_domain": domain,
                        "source_format": "eventbrite-json"
                    }
                    events.append(event)
                    return events # Assume only one main event from this structure


            # --- Fallback HTML scraping if JSON wasn't found/parsed ---
            logger.info(f"Eventbrite JSON not found/parsed for {url}, falling back to HTML scraping.")

            title_elem = soup.select_one('[data-testid="event-title"]') # New Eventbrite structure
            if not title_elem:
                title_elem = soup.select_one('h1[data-automation="listing-event-title"]') # Older structure

            title = self._extract_text(title_elem)

            # Date and time are often complex, maybe combined
            # Look for structured date time elements first
            start_date, start_time, end_date, end_time = '', '', '', ''
            date_time_elem = soup.select_one('[data-testid="event-start-date"]')
            if date_time_elem:
                 date_time_text = self._extract_text(date_time_elem)
                 # Example: "Tue, Jul 16, 2024 7:00 PM CDT" or "July 16 · 7pm - July 17 · 10pm CDT"
                 # This requires more complex parsing logic
                 start_date, start_time = self._parse_datetime(date_time_text) # Basic attempt
                 # TODO: Add logic for date/time ranges if needed

            # Fallback date/time selectors
            if not start_date:
                 date_elem = soup.select_one('span[data-automation="event-details-time"] p') # Older selector
                 date_text = self._extract_text(date_elem)
                 start_date, start_time = self._parse_datetime(date_text) # Basic attempt

            # Location
            venue, address, city, state, country = '', '', '', '', ''
            location_link = soup.select_one('a[data-testid="event-venue-link"]')
            location_div = soup.select_one('div[data-testid="event-venue-map-link"]') # Parent div often has more details

            if location_link:
                venue = self._extract_text(location_link.find('p')) # Venue name often in first <p>
                if location_div:
                    address_elem = location_div.find('p', recursive=False, attrs={'class': None}) # Address often second <p> without specific class
                    address = self._extract_text(address_elem)
                    city, state = self._extract_city_state_from_text(address)

            # Fallback location
            if not venue and not address:
                location_elem = soup.select_one('[data-automation="event-details-location"]')
                location_text = self._extract_text(location_elem)
                # Simple split logic (can be inaccurate)
                lines = [line.strip() for line in location_text.split('\n') if line.strip()]
                if lines: venue = lines[0]
                if len(lines) > 1: address = ', '.join(lines[1:])
                city, state = self._extract_city_state_from_text(address or venue)

            # Description
            desc_elem = soup.select_one('[data-testid="event-description"]')
            description = self._extract_text(desc_elem)
            if not description: # Fallback
                desc_elem = soup.select_one('div[data-automation="listing-event-description"]')
                description = self._extract_text(desc_elem)


            # Image
            image_url = self._extract_meta_content(soup, 'og:image')
            if not image_url:
                 img_elem = soup.select_one('picture img[data-testid="hero-banner-image"]') # New selector
                 if not img_elem:
                      img_elem = soup.select_one('picture img') # Generic fallback
                 image_url = self._extract_attr(img_elem, 'src')

            # Organizer
            organizer_elem = soup.select_one('[data-testid="organizer-name"]')
            organizer = self._extract_text(organizer_elem)

            if title and start_date:
                event = {
                    "title": title,
                    "start_date": start_date,
                    "start_time": start_time or "00:00:00",
                    "end_date": end_date or start_date,
                    "end_time": end_time or "23:59:59",
                    "venue": venue,
                    "address": address,
                    "city": city,
                    "state": state,
                    "country": country or ("US" if state else ""),
                    "latitude": None, # HTML scraping less likely to get coords
                    "longitude": None,
                    "description": description[:1000],
                    "url": url,
                    "image": self._normalize_url(url, image_url),
                    "organizer": organizer,
                    "source_url": url,
                    "source_domain": domain,
                    "source_format": "eventbrite-html"
                }
                events.append(event)

        except Exception as e:
            logger.warning(f"Error extracting Eventbrite events from {url}: {str(e)}")

        return events

    def _extract_meetup_events(self, soup: BeautifulSoup, url: str) -> List[Dict[str, Any]]:
        """Extract event information from Meetup pages."""
         # Meetup often uses LD+JSON, so this is a fallback
        logger.info(f"Running Meetup specific extractor for {url}")
        events = []
        domain = self.extract_domain(url)

        try:
            # --- Look for embedded initial state data ---
            scripts = soup.find_all('script')
            event_data = None
            for script in scripts:
                if script.string and 'window.__INITIAL_STATE__' in script.string:
                    try:
                         state_str = script.string.split('=', 1)[1].strip().rstrip(';')
                         initial_state = json.loads(state_str)
                         # Navigate the complex state object - this path might change!
                         # Check potential paths based on observed structures
                         event_node = initial_state.get('event', {}).get('event') # Path 1
                         if not event_node: # Path 2 (nested under queries/apollo)
                             # This requires deeper inspection of typical __INITIAL_STATE__ structures
                             # For simplicity, we'll skip this complex case for now unless essential
                             pass
                         if event_node and isinstance(event_node, dict):
                              event_data = event_node
                              logger.info(f"Found potential event data in __INITIAL_STATE__ for {url}")
                              break
                    except (json.JSONDecodeError, IndexError, KeyError, TypeError) as e:
                         logger.warning(f"Failed to parse __INITIAL_STATE__ from Meetup script: {e}")
                         continue

            if event_data:
                title = event_data.get('title', '')
                start_dt_str = event_data.get('dateTime')
                end_dt_str = event_data.get('endTime')
                # Meetup often uses Unix timestamps (milliseconds)
                if isinstance(start_dt_str, (int, float)):
                    start_dt = datetime.utcfromtimestamp(start_dt_str / 1000)
                    start_date = start_dt.strftime('%Y-%m-%d')
                    start_time = start_dt.strftime('%H:%M:%S')
                else:
                    start_date, start_time = self._parse_datetime(start_dt_str)

                if isinstance(end_dt_str, (int, float)):
                    end_dt = datetime.utcfromtimestamp(end_dt_str / 1000)
                    end_date = end_dt.strftime('%Y-%m-%d')
                    end_time = end_dt.strftime('%H:%M:%S')
                else:
                     end_date, end_time = self._parse_datetime(end_dt_str)


                venue_data = event_data.get('venue')
                venue, address, city, state, country, lat, lon = '', '', '', '', '', None, None
                if venue_data:
                    venue = venue_data.get('name', '')
                    address = venue_data.get('address', '')
                    city = venue_data.get('city', '')
                    state = venue_data.get('state', '')
                    country = venue_data.get('country', '') # Often abbreviation e.g., 'us'
                    if country.lower() == 'us': country = 'US'
                    lat = venue_data.get('lat')
                    lon = venue_data.get('lon')
                    # Reconstruct address if parts are separate
                    if not address and city and state:
                        address = f"{city}, {state}"


                description = event_data.get('description', '')
                desc_soup = BeautifulSoup(description or '', 'html.parser')
                description = desc_soup.get_text(separator=' ', strip=True)

                image_data = event_data.get('image')
                image_url = ''
                if image_data:
                     image_url = image_data.get('baseUrl') + image_data.get('id') + '/highres.jpg' # Construct URL

                event_url = event_data.get('eventUrl', url)
                organizer_data = event_data.get('group') # Meetup calls it group
                organizer = organizer_data.get('name', '') if organizer_data else ''

                if title and start_date:
                    event = {
                        "title": title,
                        "start_date": start_date,
                        "start_time": start_time or "00:00:00",
                        "end_date": end_date or start_date,
                        "end_time": end_time or "23:59:59",
                        "venue": venue,
                        "address": address,
                        "city": city,
                        "state": state,
                        "country": country,
                        "latitude": lat,
                        "longitude": lon,
                        "description": description[:1000],
                        "url": event_url,
                        "image": image_url,
                        "organizer": organizer,
                        "source_url": url,
                        "source_domain": domain,
                        "source_format": "meetup-json"
                    }
                    events.append(event)
                    return events # Assume only one main event

            # --- Fallback HTML scraping ---
            logger.info(f"Meetup JSON not found/parsed for {url}, falling back to HTML scraping.")

            title_elem = soup.select_one('h1#event-title') # Specific ID if available
            if not title_elem: title_elem = soup.select_one('h1')
            title = self._extract_text(title_elem)

            start_date, start_time, end_date, end_time = '', '', '', ''
            # Meetup uses <time> tags but sometimes within complex structures
            time_elem = soup.select_one('time[datetime]')
            if time_elem:
                start_dt_str = self._extract_attr(time_elem, 'datetime')
                start_date, start_time = self._parse_datetime(start_dt_str)
                 # Look for end time, often relative or in text nearby
                parent_div = time_elem.find_parent('div')
                if parent_div:
                    time_text = self._extract_text(parent_div)
                    # Example: "Thursday, July 18, 2024 at 6:00 PM to 8:00 PM PDT"
                    time_match = re.search(r'to\s+(\d{1,2}:\d{2}\s*[AP]M)', time_text, re.IGNORECASE)
                    if time_match:
                        end_time_str = time_match.group(1)
                        # Need start_date to parse end time correctly relative to day
                        if start_date:
                            _, parsed_end_time = self._parse_datetime(f"{start_date} {end_time_str}")
                            if parsed_end_time:
                                end_time = parsed_end_time
                                end_date = start_date # Assume same day unless range specified

            # Location
            venue, address, city, state, country = '', '', '', '', ''
            venue_elem = soup.select_one('[data-testid="venue-name"]')
            venue = self._extract_text(venue_elem)

            address_elem = soup.select_one('[data-testid="venue-address"]')
            address = self._extract_text(address_elem)

            if address:
                city, state = self._extract_city_state_from_text(address)
            elif venue: # Sometimes city/state is in venue line
                 city, state = self._extract_city_state_from_text(venue)


            # Description
            desc_elem = soup.select_one('#event-details') # Find main details container
            if not desc_elem: desc_elem = soup.select_one('[data-testid="event-description"]')
            description = self._extract_text(desc_elem)


            # Image
            image_url = self._extract_meta_content(soup, 'og:image')


            # Organizer (Group Name)
            organizer_elem = soup.select_one('a[data-testid="group-link-in-event-header"]')
            if not organizer_elem: organizer_elem = soup.select_one('h3 ~ p a[href*="/groups/"]') # Fallback pattern
            organizer = self._extract_text(organizer_elem)


            if title and start_date:
                event = {
                    "title": title,
                    "start_date": start_date,
                    "start_time": start_time or "00:00:00",
                    "end_date": end_date or start_date,
                    "end_time": end_time or "23:59:59",
                    "venue": venue,
                    "address": address,
                    "city": city,
                    "state": state,
                    "country": country or ("US" if state else ""),
                    "latitude": None,
                    "longitude": None,
                    "description": description[:1000],
                    "url": url,
                    "image": self._normalize_url(url, image_url),
                    "organizer": organizer,
                    "source_url": url,
                    "source_domain": domain,
                    "source_format": "meetup-html"
                }
                events.append(event)

        except Exception as e:
            logger.warning(f"Error extracting Meetup events from {url}: {str(e)}")

        return events

    def _extract_ticketmaster_events(self, soup: BeautifulSoup, url: str) -> List[Dict[str, Any]]:
        """Extract event information from Ticketmaster pages."""
        # Ticketmaster often uses LD+JSON, so this is a fallback
        logger.info(f"Running Ticketmaster specific extractor for {url}")
        events = []
        domain = self.extract_domain(url)

        try:
             # --- Look for embedded JSON data ---
             # Ticketmaster often embeds data in window.__TMANALYSIS__ or similar
             scripts = soup.find_all('script')
             event_data = None
             for script in scripts:
                 if script.string and ('window.__TMANALYSIS__' in script.string or 'window.gon' in script.string):
                      # Extracting this requires careful regex or string manipulation
                      # Example pattern: window.__TMANALYSIS__.context = {...};
                      match = re.search(r'context\s*=\s*({.*?});', script.string, re.DOTALL)
                      if match:
                          try:
                              json_str = match.group(1)
                              # Clean potential issues (comments, functions, etc.) before parsing
                              json_str = re.sub(r'//.*?\n', '', json_str) # Remove JS comments
                              json_str = re.sub(r'/\*.*?\*/', '', json_str, flags=re.DOTALL) # Remove block comments
                              json_str = re.sub(r'\bundefined\b', 'null', json_str) # Replace undefined with null
                              json_str = re.sub(r',\s*([}\]])', r'\1', json_str) # Fix trailing commas

                              data = json.loads(json_str)
                              # Navigate structure (this will likely change)
                              if 'event' in data:
                                  event_data = data['event']
                              elif 'analytics' in data and 'event' in data['analytics']:
                                  event_data = data['analytics']['event']
                              # Add more potential paths based on observation
                              # ...

                              if event_data and isinstance(event_data, dict):
                                   logger.info(f"Found potential event data in embedded script for {url}")
                                   break
                          except (json.JSONDecodeError, KeyError, TypeError) as e:
                               logger.warning(f"Failed to parse embedded JSON from Ticketmaster script: {e}")
                               continue

             if event_data:
                 # Parse data from the extracted JSON object
                 # Note: Field names might differ significantly, adapt as needed
                 title = event_data.get('name') or event_data.get('eventName')

                 start_info = event_data.get('startDate') # Might be object or string
                 start_date, start_time = self._parse_datetime(start_info)

                 end_info = event_data.get('endDate')
                 end_date, end_time = self._parse_datetime(end_info)


                 venue_data = event_data.get('venue') # Might be nested
                 venue, address, city, state, country, lat, lon = '', '', '', '', '', None, None
                 if venue_data and isinstance(venue_data, dict):
                     venue = venue_data.get('name') or venue_data.get('venueName')
                     city = venue_data.get('city')
                     state = venue_data.get('stateCode') or venue_data.get('state')
                     country = venue_data.get('countryCode') or venue_data.get('country')
                     address_parts = [venue_data.get('address1'), venue_data.get('address2'), city, state, venue_data.get('postalCode'), country]
                     address = ', '.join(filter(None, address_parts))
                     loc = venue_data.get('location') # Coordinates might be here
                     if loc and isinstance(loc, dict):
                          lat = loc.get('latitude')
                          lon = loc.get('longitude')

                 description = event_data.get('description') or event_data.get('info')
                 # Description might need cleaning

                 image_data = event_data.get('images') # Often a list
                 image_url = ''
                 if isinstance(image_data, list) and image_data:
                      # Find a suitable image URL (e.g., based on ratio or size)
                      image_url = image_data[0].get('url') # Simplest case: take first

                 event_url = event_data.get('url') or url
                 organizer = event_data.get('promoter', {}).get('name', '')


                 if title and start_date:
                     event = {
                         "title": title,
                         "start_date": start_date,
                         "start_time": start_time or "00:00:00",
                         "end_date": end_date or start_date,
                         "end_time": end_time or "23:59:59",
                         "venue": venue,
                         "address": address,
                         "city": city,
                         "state": state,
                         "country": country,
                         "latitude": lat,
                         "longitude": lon,
                         "description": description[:1000] if description else '',
                         "url": event_url,
                         "image": image_url,
                         "organizer": organizer,
                         "source_url": url,
                         "source_domain": domain,
                         "source_format": "ticketmaster-json"
                     }
                     events.append(event)
                     return events # Assume only one main event


             # --- Fallback HTML scraping ---
             logger.info(f"Ticketmaster JSON not found/parsed for {url}, falling back to HTML scraping.")

             # Title - modern TM uses complex structures, h1 might not be specific enough
             title_elem = soup.select_one('h1.event-header__title') # Try specific class
             if not title_elem: title_elem = soup.select_one('h1') # Generic h1
             title = self._extract_text(title_elem)


             start_date, start_time, end_date, end_time = '', '', '', ''
             # Date and Time elements
             date_elem = soup.select_one('div.event-header__event-date')
             time_elem = soup.select_one('div.event-header__event-time')
             date_text = self._extract_text(date_elem)
             time_text = self._extract_text(time_elem)
             # Combine and parse
             datetime_text = f"{date_text} {time_text}"
             start_date, start_time = self._parse_datetime(datetime_text)

             # Venue and Location
             venue_elem = soup.select_one('a.event-header__venue-link > span') # Venue name in span inside link
             venue = self._extract_text(venue_elem)

             address_elem = soup.select_one('div.event-header__venue-address')
             address = self._extract_text(address_elem)

             city, state = self._extract_city_state_from_text(address)


             # Description - often hidden or loaded via JS
             desc_elem = soup.select_one('div[data-testid="event-details__description"]')
             description = self._extract_text(desc_elem)
             if not description: # Fallback
                 desc_elem = soup.select_one('#eventDetailsSection')
                 if desc_elem:
                      # Exclude unwanted sections like "Parking" if possible
                      for unwanted in desc_elem.select('.artist-spotify-player, #parkingModule'):
                          unwanted.decompose()
                      description = self._extract_text(desc_elem)


             # Image
             image_url = self._extract_meta_content(soup, 'og:image')
             if not image_url:
                  img_elem = soup.select_one('div.event-header__image img') # Old selector?
                  if not img_elem: img_elem = soup.select_one('img.event-header__background-image') # Try background
                  image_url = self._extract_attr(img_elem, 'src')


             # Organizer (Promoter) - Hard to find consistently in HTML
             organizer = ''


             if title and start_date:
                 event = {
                     "title": title,
                     "start_date": start_date,
                     "start_time": start_time or "00:00:00",
                     "end_date": end_date or start_date, # Default end date
                     "end_time": end_time or "23:59:59", # Default end time
                     "venue": venue,
                     "address": address,
                     "city": city,
                     "state": state,
                     "country": country or ("US" if state else ""),
                     "latitude": None,
                     "longitude": None,
                     "description": description[:1000],
                     "url": url,
                     "image": self._normalize_url(url, image_url),
                     "organizer": organizer,
                     "source_url": url,
                     "source_domain": domain,
                     "source_format": "ticketmaster-html"
                 }
                 events.append(event)

        except Exception as e:
            logger.warning(f"Error extracting Ticketmaster events from {url}: {str(e)}")

        return events

    def _extract_facebook_events(self, soup: BeautifulSoup, url: str) -> List[Dict[str, Any]]:
        """Extract event information from Facebook event pages."""
         # Facebook heavily relies on JS and obfuscated class names.
         # LD+JSON or reliable HTML structure is rare. Best bet is meta tags.
        logger.info(f"Running Facebook specific extractor for {url}")
        events = []
        domain = self.extract_domain(url)

        try:
            # Title
            title = self._extract_meta_content(soup, 'og:title')

            # Description
            description = self._extract_meta_content(soup, 'og:description')

            # Image
            image_url = self._extract_meta_content(soup, 'og:image')

            # Dates/Times - VERY unreliable from meta tags or standard HTML
            # Sometimes available in the description meta tag, try to parse
            start_date, start_time, end_date, end_time = '', '', '', ''
            if description:
                 # Look for patterns like "Date: Month Day, Year ⋅ Time: HH:MM PM"
                 # Or "Hosted by ... Event by ... on Month Day, Year at HH:MM PM"
                 # These patterns change frequently.
                 start_date, start_time = self._parse_datetime(description) # Very rough guess


            # Location - Also often in description meta tag
            venue, address, city, state, country = '', '', '', '', ''
            if description:
                 # Look for "at [Venue Name]" or address patterns
                 at_match = re.search(r'\bat\s+([A-Za-z0-9\s\.\'-]+?)(?:\s+\d+.*?,|\s+·|\s+Hosted by|\.$)', description)
                 if at_match:
                      venue = at_match.group(1).strip()

                 city, state = self._extract_city_state_from_text(description)
                 if venue and not city: # Try extracting from venue string if not found elsewhere
                    city_venue, state_venue = self._extract_city_state_from_text(venue)
                    if city_venue: city = city_venue
                    if state_venue: state = state_venue

            # Organizer - Often in description too
            organizer = ''
            org_match = re.search(r'(?:Hosted by|Event by)\s+(.+?)(?:\s+on\s+|\s+·|\.$)', description)
            if org_match:
                organizer = org_match.group(1).strip()


            # Create event - Be lenient as data quality is low
            if title:
                event = {
                    "title": title,
                    "start_date": start_date, # Often missing/inaccurate
                    "start_time": start_time, # Often missing/inaccurate
                    "end_date": end_date or start_date,
                    "end_time": end_time or "23:59:59",
                    "venue": venue,
                    "address": address, # Usually missing
                    "city": city,
                    "state": state,
                    "country": country or ("US" if state else ""),
                    "latitude": None,
                    "longitude": None,
                    "description": description[:1000],
                    "url": url,
                    "image": image_url,
                    "organizer": organizer,
                    "source_url": url,
                    "source_domain": domain,
                    "source_format": "facebook-meta"
                }
                events.append(event)

        except Exception as e:
            logger.warning(f"Error extracting Facebook events from {url}: {str(e)}")

        return events

    def _extract_generic_events(self, soup: BeautifulSoup, url: str, domain: str) -> List[Dict[str, Any]]:
        """
        Generic event extraction for unsupported sites.
        Uses heuristics to find events based on common patterns and meta tags.

        Args:
            soup: BeautifulSoup object of the page
            url: Original URL
            domain: Site domain

        Returns:
            List of extracted events
        """
        logger.info(f"Running generic extractor for {url}")
        events = []

        try:
            # 1. Check Meta Tags (Open Graph, Dublin Core, basic meta)
            title = self._extract_meta_content(soup, 'og:title')
            if not title: title = self._extract_text(soup.find('title')) # Basic title tag

            description = self._extract_meta_content(soup, 'og:description')
            if not description: description = self._extract_meta_content(soup, 'description')

            image_url = self._extract_meta_content(soup, 'og:image')

            # Try specific date meta tags
            start_date_str = self._extract_meta_content(soup, 'event:start_time') \
                        or self._extract_meta_content(soup, 'og:start_date') \
                        or self._extract_meta_content(soup, 'article:published_time') # Less ideal fallback
            end_date_str = self._extract_meta_content(soup, 'event:end_time') \
                        or self._extract_meta_content(soup, 'og:end_date') \
                        or self._extract_meta_content(soup, 'article:expiration_time') # Less ideal fallback

            start_date, start_time = self._parse_datetime(start_date_str)
            end_date, end_time = self._parse_datetime(end_date_str)

            # Try location meta tags
            city = self._extract_meta_content(soup, 'og:locality')
            state = self._extract_meta_content(soup, 'og:region')
            country = self._extract_meta_content(soup, 'og:country-name')
            address = self._extract_meta_content(soup, 'og:street-address')
            # Combine address parts if found separately
            if not address and city and state:
                address = f"{city}, {state}"
            venue = self._extract_meta_content(soup, 'og:venue') # Less common OG tag


            # 2. If key info missing, search HTML content using heuristics
            if not title:
                # Look for H1 or elements with "title" in class/id
                title_elem = soup.find(['h1', 'h2'], class_=re.compile(r'title|headline|heading', re.I))
                if not title_elem: title_elem = soup.find('h1')
                title = self._extract_text(title_elem)


            # Search for date/time text patterns if not found in meta
            if not start_date:
                date_selectors = [
                    'time[datetime]', '.event-date', '.entry-date', '.published', '.post-date',
                    '[class*="date"]', '[class*="time"]', '[itemprop*="Date"]'
                ]
                found_date_text = ''
                for selector in date_selectors:
                    elements = soup.select(selector)
                    for element in elements:
                        dt_attr = self._extract_attr(element, 'datetime')
                        text = dt_attr or self._extract_text(element)
                        parsed_date, _ = self._parse_datetime(text)
                        if parsed_date: # Found a valid date
                            found_date_text = text
                            break # Use the first likely candidate
                    if found_date_text: break

                if found_date_text:
                    start_date, start_time = self._parse_datetime(found_date_text)
                    # Try to find end date/time nearby or with range indicators
                    # (Simplified: assumes no end date found this way)


            # Search for location text patterns if not found in meta
            if not city and not address and not venue:
                location_selectors = [
                    '.location', '.venue', '.address', '[class*="location"]', '[class*="venue"]',
                    '[class*="address"]', '[itemprop="location"]', '[itemprop="address"]'
                ]
                found_location_text = ''
                for selector in location_selectors:
                    elements = soup.select(selector)
                    for element in elements:
                        text = self._extract_text(element)
                        # Check if text looks like a location
                        if re.search(r'\d+\s+[A-Za-z]+|\b([A-Z]{2})\b|,', text):
                            found_location_text = text
                            break
                    if found_location_text: break

                if found_location_text:
                    address = found_location_text # Use the whole block as address initially
                    city, state = self._extract_city_state_from_text(address)
                    # Try to extract venue name (e.g., first line)
                    lines = [line.strip() for line in address.split('\n') if line.strip()]
                    if lines and len(lines) > 1 and not city: # If multiple lines and city not parsed yet
                        maybe_venue = lines[0]
                        # Check if first line looks more like venue than street address
                        if not re.match(r'\d+', maybe_venue):
                            venue = maybe_venue


            # Search for description if not found in meta
            if not description:
                 desc_selectors = [
                     '.event-description', '.entry-content', '.post-content', 'article',
                     '[itemprop="description"]', 'div[class*="content"]', 'div[class*="details"]'
                 ]
                 desc_text = ''
                 for selector in desc_selectors:
                     element = soup.select_one(selector)
                     if element:
                         # Clean up common unwanted sub-elements
                         for unwanted in element.select('nav, .social-share, .comments, footer'):
                             unwanted.decompose()
                         desc_text = element.get_text(separator=' ', strip=True)
                         if len(desc_text) > 100: # Prefer longer content
                              break
                 description = desc_text


            # Organizer: Look for common patterns
            organizer = ''
            organizer_selectors = ['.organizer', '[class*="organizer"]', '.host', '[class*="host"]']
            for selector in organizer_selectors:
                 element = soup.select_one(selector)
                 text = self._extract_text(element)
                 if text:
                     organizer = text
                     break


            # Final checks and assembly
            if title and start_date:
                # Refine address if only city/state found
                if not address and city and state:
                    address = f"{city}, {state}"
                if not country and state:
                     country = "US" # Assumption

                event = {
                    "title": self._clean_text(title),
                    "start_date": start_date,
                    "start_time": start_time or "00:00:00",
                    "end_date": end_date or start_date,
                    "end_time": end_time or "23:59:59",
                    "venue": self._clean_text(venue),
                    "address": self._clean_text(address),
                    "city": self._clean_text(city),
                    "state": self._clean_text(state),
                    "country": self._clean_text(country),
                    "latitude": None, # Generic usually doesn't get coords
                    "longitude": None,
                    "description": self._clean_text(description)[:1000],
                    "url": url,
                    "image": self._normalize_url(url, image_url),
                    "organizer": self._clean_text(organizer),
                    "source_url": url,
                    "source_domain": domain,
                    "source_format": "generic-html"
                }
                events.append(event)

        except Exception as e:
            logger.warning(f"Error extracting generic events from {url}: {str(e)}")

        return events


    async def scan_url(self, url: str) -> List[Dict[str, Any]]:
        """
        Scan a single URL, fetch content, and extract events.

        Args:
            url: The URL to scan

        Returns:
            List of extracted event dictionaries
        """
        logger.info(f"Scanning URL: {url}")
        html_content = await self.fetch_url(url)

        if html_content:
            extracted_events = self.extract_event_data_from_html(html_content, url)
            return extracted_events
        else:
            return []

    async def run(self) -> List[Dict[str, Any]]:
        """
        Run the agent to scan all configured URLs and return extracted events.

        Returns:
            List of all unique events found across all URLs.
        """
        all_urls = self.get_all_urls()
        logger.info(f"Starting scan for {len(all_urls)} URLs...")

        # Use asyncio.gather to run scans concurrently
        tasks = [self.scan_url(url) for url in all_urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_events = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"An error occurred during URL scanning: {result}")
            elif isinstance(result, list):
                all_events.extend(result)

        # Deduplicate events across all sources (more robust deduplication)
        final_unique_events = []
        seen_event_keys = set()

        for event in all_events:
            # Create a unique key based on title, date, and potentially location/url
            title_key = event.get('title', '').strip().lower()
            date_key = event.get('start_date', '')
            # Use a slightly fuzzy key to catch minor variations
            location_key = (event.get('city', '') or event.get('venue', '') or '').strip().lower()[:15] # First 15 chars of city/venue

            key = (title_key, date_key, location_key)

            if all(k for k in key) and key not in seen_event_keys:
                final_unique_events.append(event)
                seen_event_keys.add(key)
            elif title_key and date_key and not location_key and (title_key, date_key) not in seen_event_keys:
                 # Fallback key if location is missing
                 key_no_loc = (title_key, date_key)
                 final_unique_events.append(event)
                 seen_event_keys.add(key) # Add the full key anyway
                 seen_event_keys.add(key_no_loc)


        self.events = final_unique_events
        logger.info(f"Scan complete. Found {len(self.events)} unique events in total.")
        return self.events

    def save_events(self, output_file: str = 'events_output.json') -> None:
        """
        Save the extracted events to a JSON file.

        Args:
            output_file: Path to the output JSON file.
        """
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True) # Ensure directory exists

        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(self.events, f, indent=4, ensure_ascii=False)
            logger.info(f"Successfully saved {len(self.events)} events to {output_file}")
        except IOError as e:
            logger.error(f"Error saving events to {output_file}: {str(e)}")
        except TypeError as e:
             logger.error(f"Error serializing events to JSON: {str(e)}. Check event data types.")


# Example Usage
async def main():
    # Option 1: Load from file
    # sources_file = 'url_sources.json' # Create this file first
    # agent = EventURLAgent(sources_file=sources_file)

    # Create a dummy sources file for testing
    sources_data = {
        "sources": [
            "https://www.eventbrite.com/d/ca--san-francisco/events/", # List page - might not yield events directly
            "https://www.eventbrite.com/e/example-event-tickets-1234567890", # Example specific event (replace with real one)
            "https://www.meetup.com/find/events/?location=us--ca--san%20francisco", # List page
            "https://www.meetup.com/example-group/events/123456789/", # Example specific event (replace with real one)
             "https://example.com/events/some-generic-event", # Generic site example
             "https://www.facebook.com/events/1234567890/" # Example Facebook event
        ]
    }
    sources_file = 'temp_url_sources.json'
    with open(sources_file, 'w') as f:
         json.dump(sources_data, f)

    # Option 2: Provide URLs directly
    direct_urls = [
        # Add real, accessible event URLs here for better testing
        "https://www.eventbrite.com/e/free-san-francisco-tech-career-fair-exclusive-tech-hiring-event-tickets-867870816007",
        "https://www.meetup.com/sf-ai-meetup/events/301696871/",
        "https://schema.org/Event", # Test schema.org example page
        # "https://www.ticketmaster.com/discover/concerts/san-francisco", # List page
        # Add a URL known to have LD+JSON
        # Add a URL for a site NOT specifically handled (to test generic)
    ]
    agent = EventURLAgent(sources_file=sources_file, urls=direct_urls)

    # Run the agent
    extracted_events = await agent.run()

    # Print extracted events (optional)
    # print(json.dumps(extracted_events, indent=2))

    # Save the results
    agent.save_events('found_events.json')

    # Clean up dummy file
    os.remove(sources_file)


if __name__ == '__main__':
    # To run the async main function
    asyncio.run(main())