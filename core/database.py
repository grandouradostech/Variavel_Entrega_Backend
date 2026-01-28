import os
import pandas as pd
from supabase import Client
from typing import Optional, Tuple
from .analysis import limpar_texto 
import functools
from fastapi import Request

NOME_DA_TABELA = "Distribuição"
NOME_COLUNA_DATA = "DATA"

def get_supabase(request: Request) -> Client:
    """Função centralizada para recuperar o cliente Supabase do estado da requisição."""
    return request.state.supabase

def validar_colunas(df: pd.DataFrame, colunas_obrigatorias: list):
    """Verifica se todas as colunas necessárias estão presentes no DataFrame para evitar KeyError."""
    colunas_faltantes = [col for col in colunas_obrigatorias if col not in df.columns]
    if colunas_faltantes:
        raise KeyError(f"Colunas obrigatórias ausentes na tabela: {', '.join(colunas_faltantes)}")

# --- FUNÇÃO 1: DADOS APURADOS (XADREZ) ---
def get_dados_apurados(
    supabase: Client, 
    data_inicio_str: str, 
    data_fim_str: str, 
    search_str: str
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Busca dados do Supabase (Distribuição), limpa e filtra.
    Retorna o DataFrame ou (None, error_message).
    """
    try:
        dados_completos = []
        page_size = 1000
        page = 0
        
        while True:
            # Paginação robusta para lidar com o limite de 1000 linhas do Supabase
            query = (
                supabase.table(NOME_DA_TABELA)
                .select("*")
                .gte(NOME_COLUNA_DATA, data_inicio_str)
                .lte(NOME_COLUNA_DATA, data_fim_str)
                .range(page * page_size, (page + 1) * page_size - 1)
            )
            response = query.execute() 
            
            if not response.data: 
                break
            dados_completos.extend(response.data)
            page += 1
            if len(response.data) < page_size: 
                break
        
        if not dados_completos:
            return None, "Nenhum dado encontrado para o período selecionado."
        
        df = pd.DataFrame(dados_completos)

        # Limpeza de Texto
        for col in df.select_dtypes(include=['object']):
            df[col] = df[col].apply(limpar_texto)
        
        # Validação da coluna COD principal
        if 'COD' in df.columns:
            df['COD'] = pd.to_numeric(df['COD'], errors='coerce')
            df.dropna(subset=['COD'], inplace=True)
            df['COD'] = df['COD'].astype(int)
        else:
             return None, "A coluna 'COD' principal não foi encontrada."

        # Filtro de Pesquisa
        if search_str:
            search_clean = limpar_texto(search_str)
            colunas_busca = ['MOTORISTA', 'MOTORISTA_2', 'AJUDANTE_1', 'AJUDANTE_2', 'AJUDANTE_3']
            colunas_existentes_busca = [col for col in colunas_busca if col in df.columns]
            mask = pd.Series(False, index=df.index)
            for col in colunas_existentes_busca:
                mask = mask | df[col].str.contains(search_clean, na=False)
            df = df[mask]
            if df.empty:
                return None, f"Nenhum dado encontrado para o termo de busca: '{search_str}'"

        return df, None

    except Exception as e:
        print(f"Erro ao buscar dados do Supabase (Distribuição): {e}")
        if "permission denied" in str(e):
             return None, "Erro de permissão no Supabase. Execute o comando GRANT para a tabela Distribuição."
        return None, "Erro ao conectar à tabela 'Distribuição'."

# --- FUNÇÃO 2: CADASTRO ---
@functools.lru_cache(maxsize=128)
def get_cadastro_sincrono(supabase: Client) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Busca todos os dados da tabela de cadastro (public.Cadastro).
    NOTA: O lru_cache pode causar erros se o objeto Client mudar, 
    mas é mantido aqui apenas para compatibilidade de performance se o cliente for persistente.
    """
    try:
        response = supabase.table("Cadastro").select("*").execute()
        
        if not response.data:
            return None, "Tabela 'Cadastro' está vazia ou não foi encontrada."
        
        df_cadastro = pd.DataFrame(response.data)
        df_cadastro.columns = df_cadastro.columns.str.strip()

        # Limpeza e padronização de CPFs para evitar erros no merge de pagamento
        if 'CPF_M' in df_cadastro.columns:
            df_cadastro['CPF_M'] = df_cadastro['CPF_M'].astype(str).str.replace(r'[.-]', '', regex=True).fillna('')
        if 'CPF_J' in df_cadastro.columns:
            df_cadastro['CPF_J'] = df_cadastro['CPF_J'].astype(str).str.replace(r'[.-]', '', regex=True).fillna('')

        return df_cadastro, None

    except Exception as e:
        print(f"Erro ao buscar dados do Cadastro: {e}")
        return None, "Erro ao conectar à tabela de Cadastro."

# --- FUNÇÃO 3: INDICADORES ---
def get_indicadores_sincrono(
    supabase: Client, 
    data_inicio_str: str, 
    data_fim_str: str
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Busca os resultados consolidados da tabela 'Resultados_Indicadores'.
    Garante o retorno de colunas mínimas para evitar KeyError no processamento de incentivos.
    """
    try:
        response = (
            supabase.table("Resultados_Indicadores")
            .select("*")
            .lte("data_inicio_periodo", data_fim_str)
            .gte("data_fim_periodo", data_inicio_str)
            .execute()
        )
        
        colunas_esperadas = ["Codigo_M", "dev_pdv", "Rating_tx", "refugo", "data_inicio_periodo", "data_fim_periodo"]
        
        if not response.data:
            # Retorna DataFrame vazio com as colunas esperadas em vez de None
            return pd.DataFrame(columns=colunas_esperadas), None 
        
        df_indicadores = pd.DataFrame(response.data)
        df_indicadores.columns = df_indicadores.columns.str.strip()

        return df_indicadores, None

    except Exception as e:
        print(f"Erro ao buscar dados de Indicadores: {e}")
        return None, "Erro ao conectar à tabela de Indicadores."

# --- FUNÇÃO 4: CAIXAS ---
def get_caixas_sincrono(
    supabase: Client, 
    data_inicio_str: str, 
    data_fim_str: str
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Busca dados de caixas entregues da tabela 'Caixas'.
    Garante a conversão de tipos para cálculo numérico.
    """
    try:
        response = (
            supabase.table("Caixas")
            .select("data, mapa, caixas")
            .gte("data", data_inicio_str)
            .lte("data", data_fim_str)
            .execute()
        )
        
        if not response.data:
            return pd.DataFrame(columns=["data", "mapa", "caixas"]), None
        
        df_caixas = pd.DataFrame(response.data)
        
        # Converte tipos para garantir o cálculo correto no bônus de caixas
        df_caixas['mapa'] = df_caixas['mapa'].astype(str)
        df_caixas['caixas'] = pd.to_numeric(df_caixas['caixas'], errors='coerce')
        df_caixas.dropna(subset=['mapa', 'caixas'], inplace=True)
        df_caixas['caixas'] = df_caixas['caixas'].astype(float) 

        return df_caixas, None

    except Exception as e:
        print(f"Erro ao buscar dados de Caixas: {e}")
        return None, "Erro ao conectar à tabela de Caixas."

def clear_cache():
    """Limpa o cache das funções de banco de dados."""
    get_cadastro_sincrono.cache_clear()
    print("--- CACHE DO BANCO DE DADOS LIMPO ---")