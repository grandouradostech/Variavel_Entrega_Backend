import datetime
import pandas as pd
from fastapi import APIRouter, Request, Depends
from typing import Optional, Dict, Any
from fastapi.concurrency import run_in_threadpool
from supabase import Client
from core.database import get_dados_apurados, get_cadastro_sincrono, get_indicadores_sincrono, get_caixas_sincrono, get_supabase
from core.analysis import gerar_dashboard_e_mapas
from .metas import _get_metas_sincrono
from core.security import get_current_user

router = APIRouter(prefix="/incentivo", tags=["Incentivo"])

# --- FUNÇÃO DE PROCESSAMENTO (Mantida igual, apenas colapsada para brevidade) ---
def processar_incentivos_sincrono(df_viagens, df_cadastro, df_indicadores, df_caixas, metas):
    # ... (Mantenha o código original desta função processar_incentivos_sincrono aqui) ...
    # Se precisar do código completo desta função novamente, me avise, mas ele não muda.
    # Vou replicar a lógica principal abaixo para garantir que funcione:
    
    incentivo_motoristas = []
    incentivo_ajudantes = []
    metas_motorista = metas.get("motorista", {})
    metas_ajudante = metas.get("ajudante", {})
    premio_motorista_map = {}
    default_premio_info = {"dev_pdv_val": "N/A", "dev_pdv_passou": False, "rating_val": "N/A", "rating_passou": False, "refugo_val": "N/A", "refugo_passou": False}
    
    cpf_motorista_map = {}
    cpf_ajudante_map = {}
    indicadores_map = {}

    if df_cadastro is not None and not df_cadastro.empty:
        df_m = df_cadastro[pd.notna(df_cadastro['Codigo_M'])].drop_duplicates(subset=['Codigo_M'])
        df_m['Codigo_M_int'] = pd.to_numeric(df_m['Codigo_M'], errors='coerce').fillna(0).astype(int)
        cpf_motorista_map = df_m.set_index('Codigo_M_int')['CPF_M'].to_dict()

        df_j = df_cadastro[pd.notna(df_cadastro['Codigo_J'])].drop_duplicates(subset=['Codigo_J'])
        df_j['Codigo_J_int'] = pd.to_numeric(df_j['Codigo_J'], errors='coerce').fillna(0).astype(int)
        cpf_ajudante_map = df_j.set_index('Codigo_J_int')['CPF_J'].to_dict()

    if df_indicadores is not None and not df_indicadores.empty:
        # Garante numérico
        for col in ['dev_pdv', 'Rating_tx', 'refugo']:
            if col in df_indicadores.columns:
                df_indicadores[col] = pd.to_numeric(df_indicadores[col], errors='coerce')
        indicadores_map = df_indicadores.set_index('Codigo_M').to_dict('index')

    if df_viagens is not None and not df_viagens.empty:
        # Motoristas
        motoristas_un = df_viagens[['COD', 'MOTORISTA']].drop_duplicates(subset=['COD'])
        for _, row in motoristas_un.iterrows():
            cod = int(row['COD'])
            ind = indicadores_map.get(cod, {})
            
            # Valores (multiplica por 100 se necessário, ajuste conforme seu dado no banco)
            # Se no banco já está 0.11 (11%), multiplicar por 100 é correto.
            dev = ind.get('dev_pdv')
            dev = dev * 100 if pd.notna(dev) else None
            
            rat = ind.get('Rating_tx')
            rat = rat * 100 if pd.notna(rat) else None
            
            ref = ind.get('refugo')
            ref = ref * 100 if pd.notna(ref) else None

            # Metas
            pass_dev = (dev is not None and dev <= metas_motorista.get("dev_pdv_meta_perc", 0))
            pass_rat = (rat is not None and rat >= metas_motorista.get("rating_meta_perc", 0))
            pass_ref = (ref is not None and ref <= metas_motorista.get("refugo_meta_perc", 0))

            premio_dev = metas_motorista.get("dev_pdv_premio", 0) if pass_dev else 0.0
            premio_rat = metas_motorista.get("rating_premio", 0) if pass_rat else 0.0
            premio_ref = metas_motorista.get("refugo_premio", 0) if pass_ref else 0.0

            info = {
                "dev_pdv_val": f"{dev:.2f}%" if dev is not None else "N/A", "dev_pdv_passou": pass_dev,
                "rating_val": f"{rat:.2f}%" if rat is not None else "N/A", "rating_passou": pass_rat,
                "refugo_val": f"{ref:.2f}%" if ref is not None else "N/A", "refugo_passou": pass_ref
            }
            premio_motorista_map[cod] = info

            incentivo_motoristas.append({
                "cpf": cpf_motorista_map.get(cod, ""),
                "cod": cod,
                "nome": str(row['MOTORISTA']).strip(),
                "dev_pdv_val": info["dev_pdv_val"], "dev_pdv_premio_val": premio_dev,
                "rating_val": info["rating_val"], "rating_premio_val": premio_rat,
                "refugo_val": info["refugo_val"], "refugo_premio_val": premio_ref,
                "total_premio": premio_dev + premio_rat + premio_ref
            })

        # Ajudantes (Herança)
        res_xadrez = gerar_dashboard_e_mapas(df_viagens)
        motorista_fixo_map = res_xadrez["mapas"].get("motorista_fixo_map", {})
        df_melted = res_xadrez["df_melted"]
        
        # --- CORREÇÃO: Filtrar apenas ajudantes visíveis no Xadrez ---
        ids_visiveis = set(res_xadrez.get("ids_visiveis", []))

        if not df_melted.empty:
            ajudantes_unicos = df_melted.drop_duplicates(subset=['AJUDANTE_COD'])
            for _, aj in ajudantes_unicos.iterrows():
                cod_aj = aj['AJUDANTE_COD']
                
                # Se não estiver na lista de visíveis (Fixo ou Visitante Qualificado), pula
                if cod_aj not in ids_visiveis:
                    continue

                cod_mot_pai = motorista_fixo_map.get(cod_aj)
                dados_pai = premio_motorista_map.get(cod_mot_pai, default_premio_info) if cod_mot_pai else default_premio_info
                
                p_dev = metas_ajudante.get("dev_pdv_premio", 0) if dados_pai["dev_pdv_passou"] else 0.0
                p_rat = metas_ajudante.get("rating_premio", 0) if dados_pai["rating_passou"] else 0.0
                p_ref = metas_ajudante.get("refugo_premio", 0) if dados_pai["refugo_passou"] else 0.0
                
                incentivo_ajudantes.append({
                    "cpf": cpf_ajudante_map.get(cod_aj, ""),
                    "cod": cod_aj,
                    "nome": aj['AJUDANTE_NOME'],
                    "dev_pdv_val": dados_pai["dev_pdv_val"], "dev_pdv_premio_val": p_dev,
                    "rating_val": dados_pai["rating_val"], "rating_premio_val": p_rat,
                    "refugo_val": dados_pai["refugo_val"], "refugo_premio_val": p_ref,
                    "total_premio": p_dev + p_rat + p_ref
                })

    return sorted(incentivo_motoristas, key=lambda x: x['nome']), sorted(incentivo_ajudantes, key=lambda x: x['nome'])

@router.get("/")
async def ler_relatorio_incentivo(
    request: Request, 
    data_inicio: str,
    data_fim: str,
    current_user: dict = Depends(get_current_user),
    supabase: Client = Depends(get_supabase)
):
    # CORREÇÃO: Não recalculamos datas. Usamos o que o usuário pediu.
    # O filtro no banco agora usa .lte e .gte para achar sobreposição de períodos.
    d_ini_str, d_fim_str = data_inicio, data_fim

    metas = await run_in_threadpool(_get_metas_sincrono, supabase)
    # Busca Viagens (Xadrez)
    df_viagens, err1 = await run_in_threadpool(get_dados_apurados, supabase, d_ini_str, d_fim_str, "")
    # Busca Cadastro
    df_cadastro, err2 = await run_in_threadpool(get_cadastro_sincrono, supabase)
    # Busca Indicadores (KPIs) - Agora com busca flexível
    df_indicadores, err3 = await run_in_threadpool(get_indicadores_sincrono, supabase, d_ini_str, d_fim_str)
    # Busca Caixas (para completar contexto se necessário, mas aqui é só incentivo)
    df_caixas, err4 = await run_in_threadpool(get_caixas_sincrono, supabase, d_ini_str, d_fim_str)
    
    error = err1 or err2 or err3 or err4
    
    motoristas, ajudantes = [], []
    if not error and df_viagens is not None:
        if 'MAPA' in df_viagens.columns: 
            df_viagens_dedup = df_viagens.drop_duplicates(subset=['MAPA'])
        else:
            df_viagens_dedup = df_viagens.drop_duplicates()

        motoristas, ajudantes = await run_in_threadpool(
            processar_incentivos_sincrono, df_viagens_dedup, df_cadastro, df_indicadores, df_caixas, metas
        )

    # Filtro de Segurança
    if current_user["role"] != "admin":
        cpf_user = current_user["username"].replace(".", "").replace("-", "")
        motoristas = [m for m in motoristas if str(m.get("cpf", "")).replace(".", "").replace("-", "") == cpf_user]
        ajudantes = [a for a in ajudantes if str(a.get("cpf", "")).replace(".", "").replace("-", "") == cpf_user]

    return {
        "motoristas": motoristas,
        "ajudantes": ajudantes,
        "error": error
    }