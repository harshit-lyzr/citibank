from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from uuid import uuid4
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
import os
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
import asyncio
from daily_analysis import DailyAnalyzer
from news_analysis import NewsAnalyzer

load_dotenv()

# ----- Config -----
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")

app = FastAPI(title="News API with MongoDB", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],  # Allows all common HTTP methods (GET, POST, PUT, DELETE, OPTIONS, etc.)
    allow_headers=["*"],  # Allows all headers
)

# ----- Mongo Client -----
client = AsyncIOMotorClient(MONGO_URI)
db = client[DB_NAME]
news_collection = db["news"]

# ----- Models -----
class NewsCreate(BaseModel):
    title: str
    content: str

class NewsItem(NewsCreate):
    id: str = Field(default_factory=lambda: str(uuid4()))
    created_at: datetime = Field(default_factory=datetime.utcnow)

# Utility to clean Mongo docs
def clean_doc(doc):
    if "_id" in doc:
        doc["_id"] = str(doc["_id"])
    return doc

# ----- POST endpoint -----
@app.post("/news", response_model=NewsItem)
async def create_news(news: NewsCreate):
    item = NewsItem(**news.dict())
    await news_collection.insert_one(item.dict())
    return item

# ----- GET all news with pagination -----
@app.get("/news")
async def get_news(page: int = 1, page_size: int = 100):
    """Get paginated news items"""
    if page < 1:
        raise HTTPException(status_code=400, detail="Page must be >= 1")
    if page_size < 1 or page_size > 500:
        raise HTTPException(status_code=400, detail="Page size must be between 1 and 500")
    
    skip = (page - 1) * page_size
    
    # Get total count for pagination info
    total_count = await news_collection.count_documents({})
    
    # Get paginated results sorted by created_at descending (newest first)
    news_list = await news_collection.find().sort("created_at", -1).skip(skip).limit(page_size).to_list(length=page_size)
    
    if not news_list and page > 1:
        raise HTTPException(status_code=404, detail="Page not found")
    
    total_pages = (total_count + page_size - 1) // page_size
    
    return {
        "news": [clean_doc(n) for n in news_list],
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total_items": total_count,
            "total_pages": total_pages,
            "has_next": page < total_pages,
            "has_prev": page > 1
        }
    }

# ----- GET news by ID -----
@app.get("/news/{news_id}", response_model=NewsItem)
async def get_news_by_id(news_id: str):
    news = await news_collection.find_one({"id": news_id})
    if not news:
        raise HTTPException(status_code=404, detail="News not found")
    return NewsItem(**{k: v for k, v in news.items() if k != "_id"})

# ----- Daily Analysis Endpoint -----
@app.post("/trigger-daily-analysis")
async def trigger_daily_analysis(background_tasks: BackgroundTasks):
    """Trigger daily portfolio analysis manually"""
    try:
        analysis_id = f"daily-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}-{str(uuid4())[:8]}"
        # Run analysis in background
        background_tasks.add_task(run_daily_analysis_task, analysis_id)
        
        return {
            "status": "success",
            "message": "Daily analysis started in background",
            "timestamp": datetime.utcnow().isoformat(),
            "analysis_id": analysis_id
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start daily analysis: {str(e)}")

@app.get("/daily-analysis/status")
async def get_daily_analysis_status():
    """Get the latest daily analysis results"""
    try:
        # Connect to analysis collection
        analysis_collection = db["daily_analysis"]
        
        # Get latest analysis results
        cursor = analysis_collection.find().sort("created_at", -1).limit(10)
        results = await cursor.to_list(length=10)
        
        if not results:
            return {"status": "no_data", "message": "No analysis results found"}
        
        # Clean results
        cleaned_results = [clean_doc(result) for result in results]
        
        return {
            "status": "success",
            "latest_analyses": cleaned_results,
            "count": len(cleaned_results)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get analysis status: {str(e)}")

async def run_daily_analysis_task(analysis_id: str):
    """Background task to run daily analysis"""
    analyzer = DailyAnalyzer()
    try:
        await analyzer.connect()
        await analyzer.run_daily_analysis()
    except Exception as e:
        print(f"Error in daily analysis task: {e}")
    finally:
        await analyzer.disconnect()

# ----- News Analysis Endpoints -----
@app.post("/trigger-news-analysis")
async def trigger_news_analysis(background_tasks: BackgroundTasks):
    """Trigger news analysis manually"""
    try:
        analysis_id = f"news-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}-{str(uuid4())[:8]}"
        # Run analysis in background
        background_tasks.add_task(run_news_analysis_task, analysis_id)
        
        return {
            "status": "success",
            "message": "News analysis started in background",
            "timestamp": datetime.utcnow().isoformat(),
            "analysis_id": analysis_id
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start news analysis: {str(e)}")

@app.get("/news-analysis/status")
async def get_news_analysis_status():
    """Get the latest news analysis results"""
    try:
        # Connect to news analysis collection
        news_analysis_collection = db["news_analysis"]
        
        # Get latest analysis results
        cursor = news_analysis_collection.find().sort("created_at", -1).limit(10)
        results = await cursor.to_list(length=10)
        
        if not results:
            return {"status": "no_data", "message": "No news analysis results found"}
        
        # Clean results
        cleaned_results = [clean_doc(result) for result in results]
        
        return {
            "status": "success",
            "latest_analyses": cleaned_results,
            "count": len(cleaned_results)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get news analysis status: {str(e)}")

async def run_news_analysis_task(analysis_id: str):
    """Background task to run news analysis"""
    analyzer = NewsAnalyzer()
    try:
        await analyzer.connect()
        await analyzer.run_news_analysis()
    except Exception as e:
        print(f"Error in news analysis task: {e}")
    finally:
        await analyzer.disconnect()
