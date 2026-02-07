"""
Bluesky Automation API
FastAPI service for scraping and following on Bluesky
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from atproto import Client
import time
from typing import List, Optional, Dict, Set
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Bluesky Automation API",
    description="API for scraping and following on Bluesky",
    version="1.0.0"
)

# ============================================================================
# SCRAPER
# ============================================================================

class BlueskyScraper:
    def __init__(self, max_pages_per_keyword: int = 5, delay_seconds: int = 2):
        self.client = Client()
        self.max_pages = max_pages_per_keyword
        self.delay = delay_seconds
        self.seen_dids: Set[str] = set()
        
    def scrape_keyword(self, keyword: str) -> List[Dict]:
        all_accounts = []
        cursor = None
        page = 0
        
        logger.info(f"Scraping keyword: '{keyword}'")
        
        while page < self.max_pages:
            try:
                params = {
                    'q': keyword,
                    'limit': 100
                }
                if cursor:
                    params['cursor'] = cursor
                
                response = self.client.app.bsky.actor.search_actors(params)
                actors = response.actors if hasattr(response, 'actors') else []
                
                if not actors:
                    logger.info(f"No more results at page {page + 1}")
                    break
                
                for actor in actors:
                    account = {
                        'did': actor.did,
                        'handle': actor.handle,
                        'displayName': getattr(actor, 'display_name', 'N/A'),
                        'description': getattr(actor, 'description', 'N/A'),
                        'avatar': getattr(actor, 'avatar', ''),
                        'followersCount': getattr(actor, 'followers_count', 0),
                        'profileUrl': f"https://bsky.app/profile/{actor.handle}",
                        'keyword': keyword,
                        'scrapedAt': datetime.now().isoformat()
                    }
                    all_accounts.append(account)
                
                page += 1
                logger.info(f"Page {page}: Found {len(actors)} accounts (Total: {len(all_accounts)})")
                
                cursor = response.cursor if hasattr(response, 'cursor') else None
                if not cursor:
                    logger.info(f"Reached end of results at page {page}")
                    break
                
                if page < self.max_pages and cursor:
                    time.sleep(self.delay)
                    
            except Exception as e:
                logger.error(f"Error on page {page + 1}: {str(e)}")
                break
        
        logger.info(f"Completed '{keyword}': {len(all_accounts)} accounts across {page} pages")
        return all_accounts
    
    def scrape_multiple_keywords(self, keywords: List[str]) -> List[Dict]:
        all_accounts = []
        
        logger.info(f"Starting scrape for {len(keywords)} keywords")
        logger.info(f"Settings: Max {self.max_pages} pages/keyword, {self.delay}s delay")
        
        for i, keyword in enumerate(keywords, 1):
            logger.info(f"[{i}/{len(keywords)}] Processing '{keyword}'")
            accounts = self.scrape_keyword(keyword)
            all_accounts.extend(accounts)
            
            if i < len(keywords):
                time.sleep(self.delay)
        
        logger.info(f"Total accounts scraped: {len(all_accounts)}")
        return all_accounts
    
    def deduplicate(self, accounts: List[Dict], seen_dids: List[str] = None) -> List[Dict]:
        if seen_dids:
            self.seen_dids.update(seen_dids)
        
        unique_accounts = []
        for account in accounts:
            did = account['did']
            if did not in self.seen_dids:
                unique_accounts.append(account)
                self.seen_dids.add(did)
        
        duplicates = len(accounts) - len(unique_accounts)
        logger.info(f"Deduplication: {len(unique_accounts)} unique, {duplicates} duplicates removed")
        
        return unique_accounts


# ============================================================================
# FOLLOWER
# ============================================================================

class BlueskyFollower:
    def __init__(self, handle: str, app_password: str, delay_seconds: int = 5):
        self.client = Client()
        self.handle = handle
        self.delay = delay_seconds
        self.login(app_password)
        
    def login(self, app_password: str):
        try:
            logger.info(f"Logging in as {self.handle}")
            self.client.login(self.handle, app_password)
            logger.info("Login successful")
        except Exception as e:
            logger.error(f"Login failed: {str(e)}")
            raise
    
    def follow_user(self, did: str, handle: str = None) -> Dict:
        try:
            result = self.client.follow(did)
            return {
                'did': did,
                'handle': handle,
                'success': True,
                'uri': result.uri if hasattr(result, 'uri') else None,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            error_msg = str(e)
            if 'already following' in error_msg.lower():
                return {
                    'did': did,
                    'handle': handle,
                    'success': False,
                    'error': 'Already following',
                    'timestamp': datetime.now().isoformat()
                }
            elif 'rate limit' in error_msg.lower():
                return {
                    'did': did,
                    'handle': handle,
                    'success': False,
                    'error': 'Rate limited',
                    'timestamp': datetime.now().isoformat()
                }
            else:
                return {
                    'did': did,
                    'handle': handle,
                    'success': False,
                    'error': error_msg,
                    'timestamp': datetime.now().isoformat()
                }
    
    def follow_bulk(self, accounts: List[Dict], max_follows: int = None) -> Dict:
        results = []
        successful = 0
        failed = 0
        already_following = 0
        rate_limited = 0
        
        accounts_to_process = accounts[:max_follows] if max_follows else accounts
        total = len(accounts_to_process)
        
        logger.info(f"Starting bulk follow: {total} accounts")
        logger.info(f"Rate limit: {self.delay}s between requests")
        
        for i, account in enumerate(accounts_to_process, 1):
            did = account.get('did') or account.get('DID')
            handle = account.get('handle') or account.get('Handle')
            
            if not did:
                logger.warning(f"[{i}/{total}] Skipping - no DID found")
                results.append({
                    'did': None,
                    'handle': handle,
                    'success': False,
                    'error': 'No DID provided'
                })
                failed += 1
                continue
            
            logger.info(f"[{i}/{total}] Following {handle or did}")
            
            result = self.follow_user(did, handle)
            results.append(result)
            
            if result['success']:
                successful += 1
                logger.info(f"[{i}/{total}] ✓ Success")
            else:
                error = result.get('error', 'Unknown error')
                if 'Already following' in error:
                    already_following += 1
                    logger.info(f"[{i}/{total}] → Already following")
                elif 'Rate limited' in error:
                    rate_limited += 1
                    logger.warning(f"[{i}/{total}] ✗ RATE LIMITED - stopping")
                    break
                else:
                    failed += 1
                    logger.error(f"[{i}/{total}] ✗ Failed: {error}")
            
            if i < total and result['success']:
                time.sleep(self.delay)
        
        logger.info(f"Follow summary: {successful} successful, {already_following} already following, {failed} failed, {rate_limited} rate limited")
        
        return {
            'success': True,
            'results': results,
            'summary': {
                'total_attempted': len(results),
                'successful': successful,
                'already_following': already_following,
                'failed': failed,
                'rate_limited': rate_limited,
                'rate_limited_stopped': rate_limited > 0
            }
        }


# ============================================================================
# API MODELS
# ============================================================================

class ScrapeRequest(BaseModel):
    keywords: List[str]
    max_pages: int = 5
    delay: int = 2
    seen_dids: List[str] = []

class FollowRequest(BaseModel):
    handle: str
    app_password: str
    accounts: List[Dict]
    delay: int = 5
    max_follows: Optional[int] = None


# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.get("/")
async def root():
    return {
        "service": "Bluesky Automation API",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "scrape": "/scrape (POST)",
            "follow": "/follow (POST)"
        }
    }

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat()
    }

@app.post("/scrape")
async def scrape(request: ScrapeRequest):
    """
    Scrape Bluesky accounts by keywords with pagination
    
    Example:
    {
        "keywords": ["AI", "tech"],
        "max_pages": 5,
        "delay": 2,
        "seen_dids": []
    }
    """
    try:
        logger.info(f"Received scrape request for {len(request.keywords)} keywords")
        
        scraper = BlueskyScraper(
            max_pages_per_keyword=request.max_pages,
            delay_seconds=request.delay
        )
        
        accounts = scraper.scrape_multiple_keywords(request.keywords)
        unique_accounts = scraper.deduplicate(accounts, request.seen_dids)
        
        return {
            "success": True,
            "total_scraped": len(accounts),
            "unique_accounts": len(unique_accounts),
            "duplicates_removed": len(accounts) - len(unique_accounts),
            "keywords_processed": len(request.keywords),
            "accounts": unique_accounts
        }
        
    except Exception as e:
        logger.error(f"Scrape error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/follow")
async def follow(request: FollowRequest):
    """
    Follow multiple Bluesky accounts with rate limiting
    
    Example:
    {
        "handle": "yourhandle.bsky.social",
        "app_password": "xxxx-xxxx-xxxx-xxxx",
        "accounts": [
            {"did": "did:plc:abc", "handle": "user.bsky.social"}
        ],
        "delay": 5,
        "max_follows": 50
    }
    """
    try:
        logger.info(f"Received follow request for {len(request.accounts)} accounts")
        
        follower = BlueskyFollower(
            handle=request.handle,
            app_password=request.app_password,
            delay_seconds=request.delay
        )
        
        result = follower.follow_bulk(
            accounts=request.accounts,
            max_follows=request.max_follows
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Follow error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
