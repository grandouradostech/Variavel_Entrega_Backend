import os
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.responses import Response
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from loguru import logger
from fastapi.openapi.utils import get_openapi

# --- CARREGAMENTO ROBUSTO DO .ENV ---
# Isso garante que o arquivo seja encontrado mesmo se rodar o terminal de outra pasta
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

# Logs para confirmar o carregamento (pode remover em produção)
logger.info(f"Carregando .env de: {env_path}")
logger.info(f"SUPABASE_URL detectada? {'SIM' if os.environ.get('SUPABASE_URL') else 'NÃO'}")

from supabase import create_client, Client
from routers import auth, xadrez, incentivo, metas, caixas, pagamento
from core.database import clear_cache

app = FastAPI()

# Configuração de CORS
origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "https://variavel-entrega-frontend.vercel.app",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- CONFIGURAÇÃO DO CLIENTE SUPABASE ---
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")

# Inicializa como None para evitar 'NameError'
supabase: Client = None 

if not url or not key:
    logger.error("CRÍTICO: SUPABASE_URL ou SUPABASE_KEY não encontradas no arquivo .env")
else:
    try:
        supabase = create_client(url, key)
        logger.info("Supabase conectado com sucesso")
    except Exception as e:
        logger.error(f"Erro ao conectar Supabase: {e}")

# --- MIDDLEWARE DB SESSION ---
@app.middleware("http")
async def db_session_middleware(request: Request, call_next):
    # Proteção: Verifica se o supabase foi iniciado corretamente
    if supabase is None:
        logger.error("Falha na requisição: Banco de dados não configurado.")
        return Response(
            content="Erro interno: Banco de dados não configurado. Verifique as chaves no .env.", 
            status_code=500
        )

    request.state.supabase = supabase
    response = await call_next(request)
    return response

# Middleware para logs de requisições
@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"Request: {request.method} {request.url}")
    response = await call_next(request)
    logger.info(f"Response status: {response.status_code}")
    return response

# Incluir Rotas
app.include_router(auth.router)
app.include_router(xadrez.router)
app.include_router(incentivo.router)
app.include_router(metas.router)
app.include_router(caixas.router)
app.include_router(pagamento.router)

@app.get("/favicon.ico", include_in_schema=False)
async def favicon_route():
    return Response(status_code=204)

@app.get("/")
def root():
    return {"message": "API Variável Entrega Online"}

@app.post("/refresh")
def refresh_data():
    """
    Limpa o cache do servidor para forçar o recálculo dos dados na próxima requisição.
    """
    clear_cache()
    return {"message": "Cache limpo com sucesso. Dados serão recalculados."}

# Customização do OpenAPI
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="Variável Distribuição API",
        version="1.0.0",
        description="API para gerenciar incentivos, metas, caixas e pagamentos.",
        routes=app.routes,
    )
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi