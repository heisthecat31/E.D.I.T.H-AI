"""E.D.I.T.H's server: her brain and her relay.

    uvicorn edith_app:app --host 0.0.0.0 --port 8000

Same routes as when she ran inside SparkAPI on Railway (/edith/ws, /edith/brain, /edith/brain/...), so
EDITH.dll only needs the new host. Deployed with Docker: README.md.
"""
from fastapi import FastAPI

from edith_brain import router as edith_brain_router
from edith_relay import router as edith_relay_router

app = FastAPI(title="E.D.I.T.H")
app.include_router(edith_relay_router)
app.include_router(edith_brain_router)


@app.get("/")
async def root():
    return {"name": "E.D.I.T.H", "routes": ["/edith/ws", "/edith/brain", "/edith/brain/stats"]}
