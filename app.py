from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from uuid import uuid4
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
import os
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware

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

# ----- GET all news -----
@app.get("/news")
async def get_news():
    news_list = await news_collection.find().to_list(length=100)  # fetch max 100
    if not news_list:
        raise HTTPException(status_code=404, detail="No news found")
    return [clean_doc(n) for n in news_list]

# ----- GET news by ID -----
@app.get("/news/{news_id}", response_model=NewsItem)
async def get_news_by_id(news_id: str):
    news = await news_collection.find_one({"id": news_id})
    if not news:
        raise HTTPException(status_code=404, detail="News not found")
    return NewsItem(**{k: v for k, v in news.items() if k != "_id"})
