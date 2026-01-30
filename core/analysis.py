import pandas as pd
import unicodedata
from typing import Dict, Any

def limpar_texto(text):
    if not isinstance(text, str):
        return text
    text_upper = text.upper()
    nfkd_form = unicodedata.normalize('NFKD', text_upper)
    ascii_bytes = nfkd_form.encode('ASCII', 'ignore')
    return ascii_bytes.decode('utf-8')

def _preparar_dataframe_ajudantes(df: pd.DataFrame) -> pd.DataFrame:
    ajudantes_dfs = []
    colunas_ajudante = sorted(df.filter(regex=r'^AJUDANTE_\d+$').columns)
    for aj_col in colunas_ajudante:
        try:
            num = aj_col.split('_')[-1]
            cod_col = f'CODJ_{num}'
            if cod_col in df.columns:
                temp_df = df[['COD', 'MOTORISTA', aj_col, cod_col]].copy()
                temp_df.rename(columns={
                    'COD': 'MOTORISTA_COD', 
                    'MOTORISTA': 'MOTORISTA_NOME',
                    aj_col: 'AJUDANTE_NOME', 
                    cod_col: 'AJUDANTE_COD'
                }, inplace=True)
                temp_df['POSICAO'] = f'AJUDANTE {num}'
                ajudantes_dfs.append(temp_df)
        except Exception as e:
            print(f"Erro ao processar coluna {aj_col}: {e}")
            continue

    if not ajudantes_dfs:
        return pd.DataFrame(columns=['MOTORISTA_COD', 'MOTORISTA_NOME', 'AJUDANTE_NOME', 'AJUDANTE_COD', 'POSICAO'])
    
    df_global_melted = pd.concat(ajudantes_dfs)
    
    # --- SANITIZAÇÃO RIGOROSA ---
    # 1. Remove caracteres inválidos
    df_global_melted['AJUDANTE_NOME'] = df_global_melted['AJUDANTE_NOME'].astype(str)
    
    # 2. Split de nomes compostos com '/' (Ex: "JOAO / MARIA")
    # Assume-se que o ID (CODJ) pertence ao primeiro nome listado.
    df_global_melted['AJUDANTE_NOME'] = df_global_melted['AJUDANTE_NOME'].apply(
        lambda x: x.split('/')[0].strip() if '/' in x else x
    )

    df_global_melted.dropna(subset=['AJUDANTE_NOME'], inplace=True)
    df_global_melted = df_global_melted[df_global_melted['AJUDANTE_NOME'].str.strip() != '']
    df_global_melted['AJUDANTE_COD'] = pd.to_numeric(df_global_melted['AJUDANTE_COD'], errors='coerce')
    df_global_melted.dropna(subset=['AJUDANTE_COD'], inplace=True)
    df_global_melted['AJUDANTE_COD'] = df_global_melted['AJUDANTE_COD'].astype(int)
    
    return df_global_melted.drop_duplicates()

def _calcular_mapas_referencia(df_melted: pd.DataFrame, df_original: pd.DataFrame) -> dict:
    motorista_fixo_map = df_melted.groupby('AJUDANTE_COD')['MOTORISTA_COD'].apply(
        lambda x: x.mode().iloc[0] if not x.mode().empty else None
    ).to_dict()
    
    posicao_fixa_map = df_melted.groupby('AJUDANTE_COD')['POSICAO'].apply(
        lambda x: x.mode().iloc[0] if not x.mode().empty else 'AJUDANTE 1'
    ).to_dict()
    
    nome_ajudante_map = df_melted.groupby('AJUDANTE_COD')['AJUDANTE_NOME'].apply(
        lambda x: x.mode().iloc[0] if not x.mode().empty else ''
    ).to_dict()
    
    contagem_viagens_motorista = df_original['COD'].value_counts().to_dict()
    
    motorista_nome_map = {}
    if not df_original.empty and 'COD' in df_original.columns and 'MOTORISTA' in df_original.columns:
        motorista_nome_map = df_original.drop_duplicates(subset=['COD']).set_index('COD')['MOTORISTA'].to_dict()
    
    return {
        "motorista_fixo_map": motorista_fixo_map,
        "posicao_fixa_map": posicao_fixa_map,
        "nome_ajudante_map": nome_ajudante_map,
        "contagem_viagens_motorista": contagem_viagens_motorista,
        "motorista_nome_map": motorista_nome_map
    }

def _classificar_e_atribuir_viagens(
    info_linha: Dict[str, Any], 
    viagens_com_motorista: pd.DataFrame, 
    mapas: Dict[str, Any], 
    total_viagens: int,
    regras: Dict[str, Any],
    ids_visiveis: set
):
    viagens_fixas = []
    viagens_visitantes = []
    
    for _, viagem in viagens_com_motorista.iterrows():
        viagem_data = {
            'cod_ajudante': int(viagem['AJUDANTE_COD']),
            'nome_ajudante': viagem['AJUDANTE_NOME'],
            'num_viagens': int(viagem['VIAGENS']) # Garante int
        }
        is_primary_fixed = mapas["motorista_fixo_map"].get(viagem_data['cod_ajudante']) == info_linha['COD']
        significance_ratio = (viagem_data['num_viagens'] / total_viagens) if total_viagens > 0 else 0
        is_significant = significance_ratio > regras["RATIO_SIGNIFICANCIA_FIXO"]
        if is_primary_fixed or is_significant:
            viagem_data['posicao_fixa'] = mapas["posicao_fixa_map"].get(viagem_data['cod_ajudante'], 'AJUDANTE 1')
            viagens_fixas.append(viagem_data)
            ids_visiveis.add(viagem_data['cod_ajudante'])
        else:
            viagens_visitantes.append(viagem_data)

    tem_fixo_acima_de_10 = False
    for fixo in viagens_fixas:
        if fixo['num_viagens'] > regras["MIN_VIAGENS_PARA_ATIVAR_REGRA_ESTRITA"]:
            tem_fixo_acima_de_10 = True
            
        posicao_str = fixo['posicao_fixa'].replace(' ', '_') # Ex: AJUDANTE_1
        cod_posicao_str = f"CODJ_{posicao_str.split('_')[-1]}" # Ex: CODJ_1
        
        if info_linha.get(posicao_str):
             info_linha[posicao_str] += f" / {fixo['nome_ajudante'].strip()} ({fixo['num_viagens']})"
        else:
             info_linha[posicao_str] = f"{fixo['nome_ajudante'].strip()} ({fixo['num_viagens']})"
             info_linha[cod_posicao_str] = fixo['cod_ajudante']

    condicao_motorista = total_viagens > regras["MIN_VIAGENS_MOTORISTA_REGRA_ESTRITA"]
    limite_minimo_visitante = regras["LIMITE_VISITANTE_PADRAO"]
    
    if condicao_motorista and tem_fixo_acima_de_10:
        limite_minimo_visitante = regras["LIMITE_VISITANTE_ESTRITO"]

    for visitante in viagens_visitantes:
        if visitante['num_viagens'] > limite_minimo_visitante:
            info_linha['VISITANTES'].append(f"{visitante['nome_ajudante'].strip()} ({visitante['num_viagens']}x)")
            ids_visiveis.add(visitante['cod_ajudante'])

def gerar_dashboard_e_mapas(df: pd.DataFrame) -> dict:
    regras = {
        "RATIO_SIGNIFICANCIA_FIXO": 0.40,
        "MIN_VIAGENS_PARA_ATIVAR_REGRA_ESTRITA": 10,
        "MIN_VIAGENS_MOTORISTA_REGRA_ESTRITA": 15,
        "LIMITE_VISITANTE_ESTRITO": 2,
        "LIMITE_VISITANTE_PADRAO": 1,
    }
    
    df_melted = _preparar_dataframe_ajudantes(df)
    
    if df_melted.empty:
        return {
            "dashboard_data": [], 
            "mapas": {}, 
            "df_melted": df_melted,
            "ids_visiveis": set()
        }

    mapas = _calcular_mapas_referencia(df_melted, df)
    contagem_viagens_ajudantes = df_melted.groupby(['MOTORISTA_COD', 'AJUDANTE_COD']).size().reset_index(name='VIAGENS')
    contagem_viagens_ajudantes['AJUDANTE_NOME'] = contagem_viagens_ajudantes['AJUDANTE_COD'].map(mapas["nome_ajudante_map"])
    
    dashboard_data = []
    ids_visiveis = set() # Set para coletar IDs de ajudantes que aparecem no Xadrez
    
    colunas_motorista_base = ['COD', 'MOTORISTA', 'MOTORISTA_2', 'COD_2']
    colunas_existentes = [col for col in colunas_motorista_base if col in df.columns]
    
    if 'COD' not in df.columns:
         return {"dashboard_data": [], "mapas": mapas, "df_melted": df_melted, "ids_visiveis": set()}

    motoristas_no_periodo = df[colunas_existentes].drop_duplicates(subset=['COD'])
    
    for _, motorista_row in motoristas_no_periodo.iterrows():
        cod_motorista = int(motorista_row['COD'])
        total_viagens = mapas["contagem_viagens_motorista"].get(cod_motorista, 0)
        
        nome_motorista = motorista_row.get('MOTORISTA')
        nome_formatado = f"COD: {cod_motorista} ({total_viagens})" 
        if pd.notna(nome_motorista) and str(nome_motorista).strip() != '':
             nome_formatado = f"{nome_motorista} ({total_viagens})"
        
        info_linha = {
            'MOTORISTA': nome_formatado, 
            'COD': cod_motorista,
            'MOTORISTA_2': motorista_row.get('MOTORISTA_2'), 
            'COD_2': motorista_row.get('COD_2'),
            'VISITANTES': []
        }
        max_pos = df_melted['POSICAO'].nunique() if not df_melted.empty else 3
        if max_pos < 3: max_pos = 3
            
        for i in range(1, max_pos + 1):
            info_linha[f'AJUDANTE_{i}'] = ''
            info_linha[f'CODJ_{i}'] = ''
        
        viagens_com_motorista = contagem_viagens_ajudantes[contagem_viagens_ajudantes['MOTORISTA_COD'] == cod_motorista]
        
        _classificar_e_atribuir_viagens(
            info_linha, viagens_com_motorista, mapas, total_viagens, regras, ids_visiveis
        )
        dashboard_data.append(info_linha)
    if dashboard_data:
        df_final = pd.DataFrame(dashboard_data)
        df_final = df_final.fillna("")
        if 'MOTORISTA' in df_final.columns:
            df_final.sort_values(by='MOTORISTA', inplace=True)
        dashboard_final = df_final.to_dict('records')
    else:
        dashboard_final = []
    
    return {
        "dashboard_data": dashboard_final,
        "mapas": mapas,
        "df_melted": df_melted,
        "ids_visiveis": list(ids_visiveis) # Retorna como lista para ser serializável
    }