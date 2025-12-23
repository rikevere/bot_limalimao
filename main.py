# Ponto de entrada do app web (FastAPI)
# Rode com:  E:\Python\Codigos\CooperVere> uvicorn codigos.coopervere.main:app --host 0.0.0.0 --port 8000 --reload
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .webhooks.router import router as webhooks_router

app = FastAPI(title="Coopervere - Evolution Webhook")

# CORS básico para desenvolvimento local
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost", "http://127.0.0.1", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Rotas
app.include_router(webhooks_router)

@app.get("/")
def root():
    return {"ok": True, "service": "coopervere", "webhooks": ["/webhook", "/webhook/{event}"]}

@app.get("/healthz")
def health():
    return {"status": "healthy"}
