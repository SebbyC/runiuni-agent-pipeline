# RuniUniJWTClient.py

import aiohttp
import asyncio
import time
import logging
import json
import os
from typing import Dict, List, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('RuniUniJWTClient')

class RuniUniJWTClient:
    """Client for the RuniUni Event API using JWT authentication."""
    
    def __init__(self, username: str = None, password: str = None, base_url: str = None):
        """
        Initialize the RuniUni API client with JWT authentication.
        
        Args:
            username: Username for login (will be converted to lowercase)
            password: Password for login (case-sensitive)
            base_url: The base URL of the RuniUni API
        """
        # Get values from environment variables if not provided
        self.username = (username or os.environ.get("RUNIUNI_USERNAME", "")).lower()
        self.password = password or os.environ.get("RUNIUNI_PASSWORD", "")
        self.base_url = (base_url or os.environ.get("RUNIUNI_BASE_URL", "")).rstrip('/')
        self.jwt_token = None
        self.token_expiry = 0  # Unix timestamp when token expires
        
        logger.info(f"Initialized RuniUni JWT API client for {self.username} with base URL: {self.base_url}")
    
    async def login(self) -> bool:
        """
        Log in to the RuniUni API and get a JWT token.
        
        Returns:
            True if login was successful, False otherwise
        """
        url = f"{self.base_url}/user/login"
        
        login_data = {
            "username": self.username,
            "password": self.password
        }
        
        try:
            logger.info(f"Attempting to log in as {self.username} to {url}")
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=login_data) as response:
                    if response.status != 200:
                        response_text = await response.text()
                        logger.error(f"Login failed with status {response.status}: {response_text}")
                        return False
                    
                    data = await response.json()
                    logger.info(f"Login response keys: {list(data.keys())}")
                    
                    # Check if token is in the response
                    if "token" in data:
                        self.jwt_token = data["token"]
                    elif "access" in data:
                        self.jwt_token = data["access"]
                    else:
                        # Some JWT implementations use different key names
                        # Let's check for common alternatives
                        possible_token_keys = ["jwt", "id_token", "auth_token", "token_access"]
                        for key in possible_token_keys:
                            if key in data:
                                self.jwt_token = data[key]
                                break
                                
                        if not self.jwt_token:
                            logger.error(f"No token found in login response. Available keys: {list(data.keys())}")
                            return False
                    
                    # Set token expiry (typically 24 hours from now)
                    self.token_expiry = int(time.time()) + 86400  # 24 hours
                    
                    logger.info("Login successful, JWT token obtained")
                    return True
                    
        except Exception as e:
            logger.error(f"Login error: {str(e)}")
            logger.exception("Full exception details:")
            return False
    
    async def ensure_authenticated(self) -> bool:
        """
        Ensure the client is authenticated with a valid JWT token.
        Logs in again if the token is missing or expired.
        
        Returns:
            True if authenticated, False otherwise
        """
        current_time = int(time.time())
        
        # If token is missing or expired (with 5-minute buffer), log in again
        if not self.jwt_token or current_time > (self.token_expiry - 300):
            return await self.login()
        
        return True
    
    async def post_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Post a single event to the RuniUni API.
        
        Args:
            event: Event dictionary with all required fields
            
        Returns:
            Response dictionary with success status and message
        """
        # Ensure we have a valid token
        if not await self.ensure_authenticated():
            return {
                "success": False,
                "message": "Failed to authenticate",
                "event": event.get("name", "Unknown event")
            }
        
        url = f"{self.base_url}/events/music/createEvent"
        
        headers = {
            "Authorization": f"Bearer {self.jwt_token}",
            "Content-Type": "application/json"
        }
        
        try:
            logger.info(f"Posting event: {event.get('name', 'Unknown event')}")
            logger.info(f"To URL: {url}")
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=event, headers=headers) as response:
                    # Get response content
                    try:
                        response_data = await response.json()
                    except:
                        response_data = await response.text()
                    
                    # Check response status
                    if response.status in [200, 201]:
                        logger.info(f"Successfully created event: {event.get('name')}")
                        return {
                            "success": True,
                            "message": "Event created successfully",
                            "response": response_data,
                            "event": event.get("name")
                        }
                    elif response.status == 401:
                        # Token expired, try to login again
                        logger.info("Token expired, logging in again")
                        if await self.login():
                            # Try again with new token
                            return await self.post_event(event)
                        else:
                            return {
                                "success": False,
                                "message": "Authentication failed after token expired",
                                "event": event.get("name")
                            }
                    else:
                        logger.error(f"Failed to create event: {response.status}")
                        return {
                            "success": False,
                            "message": f"API error: {response.status}",
                            "error": response_data,
                            "event": event.get("name")
                        }
                    
        except Exception as e:
            logger.error(f"Error posting event: {str(e)}")
            logger.exception("Full exception details:")
            return {
                "success": False,
                "message": f"Error posting event: {str(e)}",
                "event": event.get("name", "Unknown event")
            }
    
    async def post_multiple_events(self, events: List[Dict[str, Any]], 
                                  delay_between_requests: float = 1.0) -> Dict[str, Any]:
        """
        Post multiple events to the RuniUni API.
        
        Args:
            events: List of event dictionaries to post
            delay_between_requests: Time in seconds to wait between requests to avoid rate limiting
            
        Returns:
            Summary of results
        """
        if not events:
            logger.warning("No events to post")
            return {
                "success": True,
                "message": "No events to post",
                "posted": 0,
                "failed": 0
            }
        
        logger.info(f"Posting {len(events)} events to RuniUni API")
        
        # Results tracking
        results = {
            "success": True,
            "posted": 0,
            "failed": 0,
            "total": len(events),
            "successful_events": [],
            "failed_events": []
        }
        
        # Ensure we're logged in
        if not await self.ensure_authenticated():
            logger.error("Failed to authenticate")
            results["success"] = False
            results["message"] = "Failed to authenticate"
            results["failed"] = len(events)
            return results
        
        # Post each event
        for i, event in enumerate(events):
            event_name = event.get("name", f"Unknown event {i+1}")
            logger.info(f"Posting event {i+1}/{len(events)}: {event_name}")
            
            # Post the event
            result = await self.post_event(event)
            
            # Add delay to avoid rate limiting
            if i < len(events) - 1:  # Don't delay after the last event
                await asyncio.sleep(delay_between_requests)
            
            # Track results
            if result["success"]:
                results["posted"] += 1
                results["successful_events"].append({
                    "name": event_name,
                    "response": result.get("response", {})
                })
            else:
                results["failed"] += 1
                results["failed_events"].append({
                    "name": event_name,
                    "error": result.get("message", "Unknown error")
                })
        
        # Set overall success based on results
        results["success"] = results["failed"] == 0
        results["message"] = f"Posted {results['posted']}/{len(events)} events successfully"
        
        logger.info(f"Finished posting events: {results['posted']} posted, {results['failed']} failed")
        return results

# Example usage if run directly
async def test_client():
    # Create client using environment variables
    client = RuniUniJWTClient()
    
    # Login test
    if await client.login():
        print("Login successful!")
    else:
        print("Login failed!")
        return
    
    # Sample event
    sample_event = {
        "name": "Test Event",
        "description": "This is a test event created by the RuniUni JWT Client",
        "url": "https://example.com",
        "imageURL": "https://picsum.photos/800/600",
        "start_date": "2025-04-01",
        "start_time": "18:00:00",
        "end_date": "2025-04-01",
        "end_time": "22:00:00",
        "city": "Pensacola",
        "state": "Florida",
        "country": "United States",
        "district": "Escambia County",
        "lat": 30.421309,
        "lng": -87.216915,
        "tag_ids": [1, 3]
    }
    
    # Post the sample event
    result = await client.post_event(sample_event)
    
    # Print result
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    asyncio.run(test_client())