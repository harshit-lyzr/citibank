#!/usr/bin/env python3
"""
Daily Portfolio Analysis Script

This script runs as a cron job every 24 hours to:
1. Fetch last 24 hours news from MongoDB
2. Fetch all portfolios from portfolios database
3. Send 4 API calls to Lyzr agent:
   - 3 individual portfolio analyses
   - 1 all-portfolio analysis
"""

import os
import asyncio
import os
import logging
import httpx
import json
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
        logging.FileHandler('daily_analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "citibank")
NEWS_COLLECTION = "news"
PORTFOLIOS_COLLECTION = "portfolios"
ANALYSIS_COLLECTION = "daily_analysis"
STOCKS_COLLECTION = "stocks"

# Lyzr API Configuration
LYZR_API_URL = "https://agent-prod.studio.lyzr.ai/v3/inference/chat/"
LYZR_API_KEY = "sk-default-PnO8PLVxE8ukLHaAVFQPbnlUYmkkEfXs"
LYZR_AGENT_ID = "68b6c68de2e877db76241f13"
LYZR_USER_ID = "harshit@lyzr.ai"

class DailyAnalyzer:
    def __init__(self):
        self.client = None
        self.db = None
        self.news_collection = None
        self.portfolios_collection = None
        self.analysis_collection = None
        self.stocks_collection = None
    
    async def connect(self):
        """Connect to MongoDB"""
        try:
            self.client = AsyncIOMotorClient(MONGO_URI)
            self.db = self.client[DB_NAME]
            self.news_collection = self.db[NEWS_COLLECTION]
            self.portfolios_collection = self.db[PORTFOLIOS_COLLECTION]
            self.analysis_collection = self.db[ANALYSIS_COLLECTION]
            self.stocks_collection = self.db[STOCKS_COLLECTION]
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
    
    async def fetch_stock_8k_filings(self):
        """Fetch 8-K filings for AAPL, MSFT, NTFL, AMZN from stocks collection"""
        try:
            target_stocks = ["AAPL", "MSFT", "NFLX", "AMZN"]
            cursor = self.stocks_collection.find({"stock": {"$in": target_stocks}})
            stocks_data = await cursor.to_list(length=None)
            
            # Format as dictionary {stock: 8_k}
            filings_dict = {}
            for stock_doc in stocks_data:
                stock_symbol = stock_doc.get("stock")
                filing_8k = stock_doc.get("8_k", "No 8-K filing available")
                if stock_symbol:
                    filings_dict[stock_symbol] = filing_8k
            
            # Ensure all target stocks are included
            for stock in target_stocks:
                if stock not in filings_dict:
                    filings_dict[stock] = "No 8-K filing available"
            
            logger.info(f"Found 8-K filings for {len(filings_dict)} stocks")
            return filings_dict
            
        except Exception as e:
            logger.error(f"Error fetching 8-K filings: {e}")
            return {stock: "Error fetching 8-K filing" for stock in ["AAPL", "MSFT", "NTFL", "AMZN"]}
    
    async def send_to_lyzr_agent(self, portfolio_data, news_data, filings_data, analysis_type="individual"):
        """Send data to Lyzr AI agent"""
        try:
            # Create session ID
            session_id = f"{LYZR_AGENT_ID}-{analysis_type}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            # Prepare message with 8-K filings
            message = f"Portfolio: {json.dumps(portfolio_data, default=str)} News: {json.dumps(news_data, default=str)} 8-K Filings: {json.dumps(filings_data, default=str)}"
            
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
                    logger.info(f"🔄 Attempt {attempt + 1}/{max_retries} for {analysis_type}")
                    
                    async with httpx.AsyncClient(timeout=120.0) as client:  # Increased timeout
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
                            logger.info(f"✅ Lyzr analysis completed for {analysis_type}")
                            return ai_response
                        else:
                            logger.error(f"❌ Lyzr API error {response.status_code}: {response.text}")
                            if attempt < max_retries - 1:
                                await asyncio.sleep(5)  # Wait before retry
                                continue
                            return None
                            
                except httpx.TimeoutException:
                    logger.warning(f"⏰ Timeout on attempt {attempt + 1} for {analysis_type}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(10)  # Wait longer after timeout
                        continue
                    raise
                except httpx.ConnectError as e:
                    logger.warning(f"🔌 Connection error on attempt {attempt + 1} for {analysis_type}: {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(5)
                        continue
                    raise
                    
        except Exception as e:
            logger.error(f"❌ Error calling Lyzr API for {analysis_type}: {str(e)}")
            logger.error(f"❌ Error type: {type(e).__name__}")
            if hasattr(e, '__cause__') and e.__cause__:
                logger.error(f"❌ Caused by: {e.__cause__}")
            return None
    
    async def analyze_and_save_portfolio(self, portfolio, news_data, filings_data, portfolio_index, analysis_id):
        """Analyze individual portfolio and save to database"""
        try:
            logger.info(f"📈 Analyzing portfolio {portfolio_index}")
            
            result = await self.send_to_lyzr_agent(
                portfolio_data=portfolio,
                news_data=news_data,
                filings_data=filings_data,
                analysis_type=f"portfolio_{portfolio_index}"
            )
            
            if result:
                # Save to database
                await self.save_analysis_to_db(
                    analysis_data=result,
                    analysis_type=f"portfolio_{portfolio_index}",
                    portfolio_data=portfolio,
                    analysis_id=analysis_id
                )
                
                return {
                    "portfolio_index": portfolio_index,
                    "portfolio": portfolio,
                    "analysis": result
                }
            return None
            
        except Exception as e:
            logger.error(f"❌ Error analyzing portfolio {portfolio_index}: {e}")
            return None
    
    async def analyze_and_save_all_portfolios(self, portfolios_data, news_data, filings_data, analysis_id):
        """Analyze all portfolios together and save to database"""
        try:
            logger.info("📊 Running all-portfolio analysis...")
            
            result = await self.send_to_lyzr_agent(
                portfolio_data=portfolios_data,
                news_data=news_data,
                filings_data=filings_data,
                analysis_type="all_portfolios"
            )
            
            if result:
                # Save to database
                await self.save_analysis_to_db(
                    analysis_data=result,
                    analysis_type="all_portfolios",
                    portfolio_data=portfolios_data,
                    analysis_id=analysis_id
                )
                
            return result
            
        except Exception as e:
            logger.error(f"❌ Error in all-portfolio analysis: {e}")
            return None
    
    async def save_analysis_to_db(self, analysis_data, analysis_type, portfolio_data=None, analysis_id=None):
        """Save analysis results to MongoDB"""
        try:
            today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            
            analysis_doc = {
                "analysis_id": analysis_id,
                "date": today,
                "analysis_type": analysis_type,
                "portfolio": portfolio_data,
                "analysis_result": analysis_data,
                "created_at": datetime.now(timezone.utc),
                "status": "completed"
            }
            
            result = await self.analysis_collection.insert_one(analysis_doc)
            logger.info(f"✅ Saved {analysis_type} analysis to database with ID: {result.inserted_id}")
            return result.inserted_id
            
        except Exception as e:
            logger.error(f"❌ Error saving {analysis_type} analysis to database: {e}")
            return None
    
    async def run_daily_analysis(self):
        """Main function to run daily analysis"""
        try:
            logger.info("🚀 Starting daily portfolio analysis...")
            
            # Generate unique analysis ID for this run
            analysis_id = f"daily-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{str(uuid4())[:8]}"
            logger.info(f"📋 Analysis ID: {analysis_id}")
            
            # Check if analysis already exists for today
            today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            
            
            # Fetch data
            news_data = await self.fetch_last_24h_news()
            portfolios_data = await self.fetch_all_portfolios()
            filings_data = await self.fetch_stock_8k_filings()
            
            if not news_data:
                logger.warning("⚠️ No news data found for last 24 hours")
            
            if not portfolios_data:
                logger.error("❌ No portfolios found - cannot proceed")
                return
            
            logger.info(f"📊 Processing {len(portfolios_data)} portfolios with {len(news_data)} news articles and {len(filings_data)} 8-K filings")
            
            # Create all analysis tasks to run in parallel
            logger.info("📊 Starting all analyses in parallel...")
            
            # Individual portfolio analysis tasks
            individual_tasks = []
            for i, portfolio in enumerate(portfolios_data, 1):
                task = self.analyze_and_save_portfolio(
                    portfolio=portfolio,
                    news_data=news_data,
                    filings_data=filings_data,
                    portfolio_index=i,
                    analysis_id=analysis_id
                )
                individual_tasks.append(task)
            
            # All-portfolio analysis task
            all_portfolio_task = self.analyze_and_save_all_portfolios(
                portfolios_data=portfolios_data,
                news_data=news_data,
                filings_data=filings_data,
                analysis_id=analysis_id
            )
            
            # Run all tasks in parallel
            all_tasks = individual_tasks + [all_portfolio_task]
            results = await asyncio.gather(*all_tasks, return_exceptions=True)
            
            # Process results
            individual_results = []
            all_portfolio_result = None
            
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"❌ Task {i+1} failed: {result}")
                elif i < len(individual_tasks):
                    # Individual portfolio result
                    if result:
                        individual_results.append(result)
                else:
                    # All-portfolio result
                    all_portfolio_result = result
            
            # Summary
            logger.info("🏁 Daily analysis completed:")
            logger.info(f"   📈 Individual analyses: {len(individual_results)}")
            logger.info(f"   📊 All-portfolio analysis: {'✅' if all_portfolio_result else '❌'}")
            logger.info(f"   📰 News articles processed: {len(news_data)}")
            
            return {
                "individual_results": individual_results,
                "all_portfolio_result": all_portfolio_result,
                "news_count": len(news_data),
                "portfolio_count": len(portfolios_data)
            }
            
        except Exception as e:
            logger.error(f"❌ Error in daily analysis: {e}")
            raise

async def main():
    """Main function"""
    analyzer = DailyAnalyzer()
    
    try:
        # Connect to database
        await analyzer.connect()
        
        # Run daily analysis
        results = await analyzer.run_daily_analysis()
        
        logger.info("✅ Daily analysis script completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Error in main: {e}")
    finally:
        await analyzer.disconnect()

if __name__ == "__main__":
    # Run the daily analysis
    asyncio.run(main())
