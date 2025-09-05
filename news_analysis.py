#!/usr/bin/env python3
"""
News Analysis Script

This script:
1. Fetches last 24 hours news from MongoDB news database
2. Fetches all portfolios from portfolios collection
3. Sends portfolio and news data to Lyzr agent for analysis
"""

import os
import asyncio
import httpx
import json
import logging
from datetime import datetime, timezone, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
from uuid import uuid4

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('news_analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "citibank")
NEWS_COLLECTION = "news"
PORTFOLIOS_COLLECTION = "portfolios"
NEWS_ANALYSIS_COLLECTION = "news_analysis"

# Lyzr API Configuration
LYZR_API_URL = "https://agent-prod.studio.lyzr.ai/v3/inference/chat/"
LYZR_API_KEY = "sk-default-PnO8PLVxE8ukLHaAVFQPbnlUYmkkEfXs"
LYZR_AGENT_ID = "68b7f556e235001e893b0410"
LYZR_USER_ID = "harshit@lyzr.ai"

class NewsAnalyzer:
    def __init__(self):
        self.client = None
        self.db = None
        self.news_collection = None
        self.portfolios_collection = None
        self.news_analysis_collection = None
    
    async def connect(self):
        """Connect to MongoDB"""
        try:
            self.client = AsyncIOMotorClient(MONGO_URI)
            self.db = self.client[DB_NAME]
            self.news_collection = self.db[NEWS_COLLECTION]
            self.portfolios_collection = self.db[PORTFOLIOS_COLLECTION]
            self.news_analysis_collection = self.db[NEWS_ANALYSIS_COLLECTION]
            logger.info("Connected to MongoDB successfully")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    async def disconnect(self):
        """Disconnect from MongoDB"""
        if self.client:
            self.client.close()
            logger.info("Disconnected from MongoDB")
    
    async def fetch_last_24h_news(self):
        """Fetch news from last 24 hours"""
        try:
            # Calculate 24 hours ago
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=24)
            
            # Query news from last 24 hours
            cursor = self.news_collection.find({
                "created_at": {"$gte": cutoff_time}
            }).sort("created_at", -1)  # Most recent first
            
            news_list = await cursor.to_list(length=None)
            logger.info(f"Found {len(news_list)} news articles from last 24 hours")
            
            # Format news for API
            formatted_news = []
            for news in news_list:
                formatted_news.append({
                    "ticker": news.get("ticker", "N/A"),
                    "title": news.get("title", "N/A"),
                    "content": news.get("content", "N/A"),
                    "publisher": news.get("publisher", "Unknown"),
                    "created_at": news.get("created_at").isoformat() if news.get("created_at") else "N/A"
                })
            
            return formatted_news
            
        except Exception as e:
            logger.error(f"Error fetching last 24h news: {e}")
            return []
    
    async def fetch_all_portfolios(self):
        """Fetch all portfolios from database"""
        try:
            cursor = self.portfolios_collection.find({})
            portfolios = await cursor.to_list(length=None)
            logger.info(f"Found {len(portfolios)} portfolios")
            
            # Clean up portfolios (remove MongoDB _id)
            cleaned_portfolios = []
            for portfolio in portfolios:
                if "_id" in portfolio:
                    del portfolio["_id"]
                cleaned_portfolios.append(portfolio)
            
            return cleaned_portfolios
            
        except Exception as e:
            logger.error(f"Error fetching portfolios: {e}")
            return []
    
    async def send_to_lyzr_agent(self, portfolios_data, news_data):
        """Send all portfolios and news data to Lyzr AI agent in single call"""
        try:
            # Create session ID
            session_id = f"{LYZR_AGENT_ID}-news-analysis-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            # Prepare message with all portfolios
            message = f"news: {json.dumps(news_data, default=str)}"
            
            # API payload
            payload = {
                "user_id": LYZR_USER_ID,
                "agent_id": LYZR_AGENT_ID,
                "session_id": session_id,
                "message": message
            }
            
            # Make API call with retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    logger.info(f"🔄 Attempt {attempt + 1}/{max_retries} for portfolio analysis")
                    
                    async with httpx.AsyncClient(timeout=300.0) as client:  # Increased to 5 minutes
                        response = await client.post(
                            LYZR_API_URL,
                            headers={
                                'Content-Type': 'application/json',
                                'x-api-key': LYZR_API_KEY
                            },
                            json=payload
                        )
                        
                        if response.status_code == 200:
                            result = response.json()
                            ai_response = result.get('response', '')
                            logger.info(f"✅ Lyzr news analysis completed for all portfolios")
                            
                            # Parse the JSON response to dictionary
                            try:
                                if isinstance(ai_response, str):
                                    # Try to parse JSON string to dictionary
                                    parsed_response = json.loads(ai_response)
                                    logger.info(f"📊 Successfully parsed JSON response for news analysis")
                                    return parsed_response
                                elif isinstance(ai_response, dict):
                                    # Already a dictionary
                                    logger.info(f"📊 Response already in dictionary format for news analysis")
                                    return ai_response
                                else:
                                    logger.warning(f"⚠️ Unexpected response format for news analysis, storing as-is")
                                    return ai_response
                            except json.JSONDecodeError as e:
                                logger.error(f"❌ Failed to parse JSON response for news analysis: {e}")
                                logger.info(f"📝 Storing raw response for news analysis")
                                return {"raw_response": ai_response, "parse_error": str(e)}
                        else:
                            logger.error(f"❌ Lyzr API error {response.status_code}: {response.text}")
                            if attempt < max_retries - 1:
                                await asyncio.sleep(5)
                                continue
                            return None
                            
                except httpx.TimeoutException:
                    logger.warning(f"⏰ Timeout on attempt {attempt + 1}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(10)
                        continue
                    raise
                except httpx.ConnectError as e:
                    logger.warning(f"🔌 Connection error on attempt {attempt + 1}: {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(5)
                        continue
                    raise
                    
        except Exception as e:
            logger.error(f"❌ Error calling Lyzr API: {str(e)}")
            return None
    
    async def save_analysis_to_db(self, analysis_data, portfolios_data, news_data, analysis_id=None):
        """Save news analysis results to MongoDB with structured data"""
        try:
            today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Ensure analysis_data is properly structured
            structured_analysis = analysis_data
            if isinstance(analysis_data, dict):
                # Extract key components from the structured response
                structured_analysis = {
                    "key_insights": analysis_data.get("key_insights", []),
                    "raw_response": analysis_data.get("raw_response"),  # In case of parse errors
                    "parse_error": analysis_data.get("parse_error")     # In case of parse errors
                }
                logger.info(f"📊 Structured news analysis data with {len(analysis_data.get('key_insights', []))} insights")
            else:
                # Fallback for non-dict responses
                structured_analysis = {"raw_response": analysis_data}
                logger.warning(f"⚠️ Non-structured news analysis data")
            
            analysis_doc = {
                "analysis_id": analysis_id,
                "date": today,
                "analysis_type": "news_analysis_all_portfolios",
                "portfolios": portfolios_data,
                "news_data": news_data,
                "analysis_result": structured_analysis,
                "created_at": datetime.now(timezone.utc),
                "status": "completed"
            }
            
            result = await self.news_analysis_collection.insert_one(analysis_doc)
            logger.info(f"✅ Saved news analysis for all portfolios to database with ID: {result.inserted_id}")
            
            # Log the structure for debugging
            if isinstance(structured_analysis, dict) and "key_insights" in structured_analysis:
                insights_count = len(structured_analysis.get("key_insights", []))
                logger.info(f"📈 News analysis saved with {insights_count} key insights")
            
            return result.inserted_id
            
        except Exception as e:
            logger.error(f"❌ Error saving news analysis to database: {e}")
            return None
    
    async def run_news_analysis(self):
        """Main function to run news analysis"""
        try:
            logger.info("🚀 Starting news analysis...")
            
            # Generate unique analysis ID for this run
            analysis_id = f"news-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{str(uuid4())[:8]}"
            logger.info(f"📋 Analysis ID: {analysis_id}")
            
            # Fetch data
            news_data = await self.fetch_last_24h_news()
            portfolios_data = await self.fetch_all_portfolios()
            
            if not news_data:
                logger.warning("⚠️ No news data found for last 24 hours")
            
            if not portfolios_data:
                logger.error("❌ No portfolios found - cannot proceed")
                return
            
            logger.info(f"📊 Processing {len(portfolios_data)} portfolios with {len(news_data)} news articles in single analysis")
            
            # Analyze all portfolios together with news data
            logger.info("📈 Analyzing all portfolios together...")
            
            result = await self.send_to_lyzr_agent(
                portfolios_data=portfolios_data,
                news_data=news_data
            )
            
            if result:
                # Save to database
                await self.save_analysis_to_db(
                    analysis_data=result,
                    portfolios_data=portfolios_data,
                    news_data=news_data,
                    analysis_id=analysis_id
                )
                
                analysis_result = {
                    "portfolios": portfolios_data,
                    "analysis": result,
                    "analysis_id": analysis_id
                }
            else:
                analysis_result = None
            
            # Summary
            logger.info("🏁 News analysis completed:")
            logger.info(f"   📈 All portfolios analysis: {'✅' if analysis_result else '❌'}")
            logger.info(f"   📰 News articles processed: {len(news_data)}")
            
            return {
                "result": analysis_result,
                "news_count": len(news_data),
                "portfolio_count": len(portfolios_data)
            }
            
        except Exception as e:
            logger.error(f"❌ Error in news analysis: {e}")
            raise

async def main():
    """Main function"""
    analyzer = NewsAnalyzer()
    
    try:
        # Connect to database
        await analyzer.connect()
        
        # Run news analysis
        results = await analyzer.run_news_analysis()
        
        logger.info("✅ News analysis script completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Error in main: {e}")
    finally:
        await analyzer.disconnect()

if __name__ == "__main__":
    # Run the news analysis
    asyncio.run(main())
