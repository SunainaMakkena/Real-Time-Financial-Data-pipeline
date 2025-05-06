from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware  # Import CORSMiddleware
from fastapi.templating import Jinja2Templates
from starlette.responses import HTMLResponse
from api.routes import router as api_router
from sources.config import API_HOST, API_PORT, API_WORKERS
import uvicorn

app = FastAPI(title="Financial Data Pipeline API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(api_router, prefix="/api")
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=API_HOST,
        port=API_PORT,
        workers=API_WORKERS,
        reload=True,  
    )