from dotenv import load_dotenv

load_dotenv()

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from db.database import load_data
from routers.towers import router as towers_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_data()
    yield


app = FastAPI(title="Steel Tower Risk API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(towers_router)


@app.get("/")
def root() -> dict[str, str]:
    return {"status": "healthy", "service": "steel-tower-risk"}


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "healthy"}
