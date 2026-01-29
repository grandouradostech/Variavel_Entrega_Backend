import os
from pathlib import Path
from typing import List, Optional
import pandas as pd
from fastapi import FastAPI, Request, Query
from fastapi.responses import Response
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from loguru import logger
from fastapi.openapi.utils import get_openapi
from supabase import create_client, Client

# Importações internas do projeto
from routers import auth, xadrez, incentivo, metas, caixas, pagamento
from core.database import clear_cache

# --- CARREGAMENTO DO AMBIENTE ---
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

app = FastAPI()

# --- CONFIGURAÇÃO DE CORS ---
origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:3000",
    "https://variavel-entrega-frontend.vercel.app",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- CLIENTE SUPABASE ---
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = None 

if not url or not key:
    logger.error("ERRO: SUPABASE_URL ou SUPABASE_KEY não configuradas.")
else:
    try:
        supabase = create_client(url, key)
        logger.info("Supabase conectado com sucesso")
    except Exception as e:
        logger.error(f"Erro ao conectar Supabase: {e}")

# --- MIDDLEWARES ---
@app.middleware("http")
async def db_session_middleware(request: Request, call_next):
    if supabase is None:
        return Response(content="Erro: Banco de dados não configurado.", status_code=500)
    request.state.supabase = supabase
    return await call_next(request)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"Request: {request.method} {request.url}")
    response = await call_next(request)
    return response

# --- ROTAS ---
app.include_router(auth.router)
app.include_router(xadrez.router)
app.include_router(incentivo.router)
app.include_router(metas.router)
app.include_router(caixas.router)
app.include_router(pagamento.router)

@app.get("/")
def root():
    return {"message": "API Variável Entrega Online"}

@app.post("/refresh")
def refresh_data():
    clear_cache()
    return {"message": "Cache limpo com sucesso."}

# --- ROTA CRÍTICA: XADREZ DETALHADO COM SOMA DE CAIXAS ---
@app.get("/xadrez/detalhado")
async def get_xadrez_detalhado(
    request: Request,
    data_inicio: str = Query(..., description="Data inicial YYYY-MM-DD"),
    data_fim: str = Query(..., description="Data final YYYY-MM-DD")
):
    """
    Retorna os dados da Distribuição cruzados com a soma correta de Caixas por mapa.
    """
    try:
        client = request.state.supabase
        
        # 1. Busca dados de Distribuição (Viagens)
        resp_viagens = client.table("Distribuição")\
            .select("*")\
            .gte("DATA", data_inicio)\
            .lte("DATA", data_fim)\
            .order("DATA", desc=False)\
            .execute()
        
        if not resp_viagens.data:
            return []

        df_v = pd.DataFrame(resp_viagens.data)

        # 2. Busca dados de Caixas para o período
        resp_caixas = client.table("Caixas")\
            .select("mapa, caixas")\
            .gte("data", data_inicio)\
            .lte("data", data_fim)\
            .execute()
        
        # 3. Processamento de Somas (Correção do erro de duplicidade)
        if resp_caixas.data:
            df_c = pd.DataFrame(resp_caixas.data)
            df_c['caixas'] = pd.to_numeric(df_c['caixas'], errors='coerce').fillna(0)
            
            # Agrupa e soma por mapa para garantir que nenhuma caixa seja perdida
            df_c_agrupado = df_c.groupby('mapa')['caixas'].sum().reset_index()
            
            # Merge com a tabela de viagens
            df_v = df_v.merge(df_c_agrupado, left_on='MAPA', right_on='mapa', how='left')
            df_v.drop(columns=['mapa'], inplace=True, errors='ignore')
        else:
            df_v['caixas'] = 0

        # 4. Organização Final das Colunas (Conforme solicitado)
        colunas_final = [
            'DATA', 'MAPA', 'caixas', 
            'COD', 'MOTORISTA', 'COD_2', 'MOTORISTA_2',
            'CODJ_1', 'AJUDANTE_1', 'CODJ_2', 'AJUDANTE_2', 'CODJ_3', 'AJUDANTE_3'
        ]
        
        # Garante que apenas colunas existentes sejam filtradas
        colunas_existentes = [c for c in colunas_final if c in df_v.columns]
        df_v = df_v[colunas_existentes].fillna(0)

        return df_v.to_dict('records')

    except Exception as e:
        logger.error(f"Erro no xadrez detalhado: {e}")
        return {"error": str(e)}

# --- DOCUMENTAÇÃO OPENAPI ---
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="Variável Distribuição API",
        version="1.1.0",
        description="API de Gestão de Incentivos com soma de caixas corrigida.",
        routes=app.routes,
    )
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi