from datetime import datetime
from functools import reduce
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import seaborn as sns
import yfinance as yf


def rentabilidade_fundo(df: pd.DataFrame, cnpj: str, nome_fundo: str):
    """
    Calcula a rentabilidade mensal e anual do fundo selecionado.

    Parameters:
    df: Dataframe que contem as cotas dos fundos.
    cnpj: cnpj do fundo de investimento.
    nome_fundo: nome do fundo.

    Returns:
    df_ret_mensal: Dataframe dos retornos mensais.
    df_ret_anual: Dataframe dos retornos anuais.

    NOTE:Para calcular a rentabilidade mensal dos fundos - o site MaisRetorno utiliza última cota do mês anterior com a última cota do mês seguinte.
    Para calcular a rentabilidade anual dos fundos - o site MaisRetorno utiliza última cota do ano anterior com a última cota do ano seguinte.
    Por isso, eu tive que baixar a cota do mês 12/2022.
    """
    # Selecionando o fundo de investimentos específicos
    filt_cnpj = (df['CNPJ_FUNDO'] == cnpj)
    fundo_espec = df.loc[filt_cnpj]

    # Selecionando os últimos dias de cada mês
    last_days = fundo_espec.groupby(fundo_espec.index.to_period('M')).tail(1)
    # Selecionando apenas a coluna 'VL_QUOTA'
    last_days = last_days.loc[:, 'VL_QUOTA']
    # Calculando a rentabilidade do mês
    ret_mensal = round(last_days.pct_change()*100, 2)
    # Como eu estou utilizando a última cota do mês anterior com a a última cota do mês seguinte, a 1º rentabilidade ('2022-12-30') dessa série irá ser um NaN
    ret_mensal = ret_mensal.dropna()
    # Criando o df p/ os retornos mensais
    df_ret_mensal = pd.DataFrame(ret_mensal)
    # Renomeando a coluna 'VL_QUOTA'
    df_ret_mensal = df_ret_mensal.rename(columns={f'VL_QUOTA':'ret'})

    # Transformando as poncentagens em taxa unitária
    df_ret_mensal['taxa_unit'] = 1 + (df_ret_mensal[f'ret'] / 100) 
    # Lista dos anos
    lst_years = df_ret_mensal.index.year.unique()
    # Transformando em string
    lst_years = lst_years.astype(str)
    # Calculando o retorno anual - aculumando as porcentagens de cada ano
    lst_ret_anual = []
    for _ in lst_years:
        ret_anual = round((df_ret_mensal.loc[_, 'taxa_unit'].agg(lambda x : x.prod()) -1) * 100, 2)
        lst_ret_anual.append(ret_anual)
    # Criando o df p/ os retornos anuais
    df_ret_anual = pd.DataFrame(lst_ret_anual, index=lst_years, columns=['ret'])
    # Adicionando o nome do fundo na string 
    df_ret_anual = df_ret_anual.rename(columns={'ret': f'ret_{nome_fundo}'})
    # Transformando o index em datetime
    df_ret_anual.index = pd.to_datetime(df_ret_anual.index)
    # Transformando o formato do index ('ano') p/ conseguir concatenar com as rentabilidades dos fundos
    df_ret_anual.index = df_ret_anual.index.to_period('Y')

    # Para não mostrar a coluna 'taxa_unit', selecionando apenas a coluna 'ret_mensal'
    df_ret_mensal_final = df_ret_mensal[[f'ret']]
    # Transformando o formato do index para 'ano-mes'
    df_ret_mensal_final.index = df_ret_mensal.index.to_period('M')
    # Adicionando o nome do fundo na string 
    df_ret_mensal_final = df_ret_mensal_final.rename(columns={'ret': f'ret_{nome_fundo}'})

    return df_ret_mensal_final, df_ret_anual


def rentabilidade_fundo_benchmark(
        df_fundo: pd.DataFrame, 
        df_benchmark: pd.DataFrame, 
        nome_fundo: str,
        nome_benchmark: str
    ) -> pd.DataFrame:
    """
    Calcula a rentabiliade do fundo selecionado.

    Parameters:
    df_fundo: Dataframe de rentabilidade do fundo.
    df_benchmark: Dataframe de rentabilidade do benchmark.
    nome_fundo: nome do fundo.
    nome_benchmark: nome do benchmark.

    Returns:
    Dataframe que contém a rentabilidade do fundo e do benchmark.
    """
    # Juntando com o df da rentabilidade mensal com o df de rentabilidade do benchmark
    df_ret_fundo_benchmark = pd.concat([df_fundo, df_benchmark], axis=1)

    # Calculando a performance do fundo - ele conseguiu bater o benchmark?
    df_ret_fundo_benchmark['performance'] = df_ret_fundo_benchmark[f'ret_{nome_fundo}'] - df_ret_fundo_benchmark[f'ret_{nome_benchmark}']

    return df_ret_fundo_benchmark


def open_cda_1(path: str) -> pd.DataFrame:
    """
    Formata o arquivo 'cda_fi_BLC_1'.

    Parameters:
    path: caminho do arquivo 'open_cda_1'.

    Returns:
    Dataframe do arquivo 'cda_fi_BLC_1'
    """
    # Lendo o arquivo
    df = pd.read_parquet(path)

    # Verificando as colunas 'TP_FUNDO' ou 'TP_FUNDO_CLASSE'
    if 'TP_FUNDO' in df.columns:
        tp_fundo_col = 'TP_FUNDO'
    elif 'TP_FUNDO_CLASSE' in df.columns:
        tp_fundo_col = 'TP_FUNDO_CLASSE'
    else:
        raise ValueError("Nenhuma das colunas 'TP_FUNDO' ou 'TP_FUNDO_CLASSE' foi encontrada no arquivo.")

    # Verificando as colunas 'CNPJ_FUNDO' ou 'CNPJ_FUNDO_CLASSE'
    if 'CNPJ_FUNDO' in df.columns:
        cnpj_fundo_col = 'CNPJ_FUNDO'
    elif 'CNPJ_FUNDO_CLASSE' in df.columns:
        cnpj_fundo_col = 'CNPJ_FUNDO_CLASSE'
    else:
        raise ValueError("Nenhuma das colunas 'CNPJ_FUNDO' ou 'CNPJ_FUNDO_CLASSE' foi encontrada no arquivo.")

    # Selecionando apenas os 'Fundos de Investimentos'
    filt_fi = df[tp_fundo_col] == 'FI'
    df = df.loc[filt_fi]

    # Selecionando as principais colunas
    columns_to_keep = [
        tp_fundo_col, 
        cnpj_fundo_col, 
        'DENOM_SOCIAL', 
        'DT_COMPTC', 
        'TP_APLIC', 
        'TP_ATIVO', 
        'VL_MERC_POS_FINAL', 
        'TP_TITPUB', 
        'DT_VENC'
    ]
    df = df[columns_to_keep]

    # Mesclando as colunas 'TP_TITPUB' e 'DT_VENC' em apenas uma coluna
    df['TP_TITPUB'] = df['TP_TITPUB'] + ' ' + df['DT_VENC']

    # Removendo a coluna 'DT_VENC'
    df = df.drop('DT_VENC', axis=1)

    # Renomeando as colunas para manter consistência
    df = df.rename(columns={tp_fundo_col: 'TP_FUNDO', cnpj_fundo_col: 'CNPJ_FUNDO', 'TP_TITPUB': 'CD_ATIVO'})

    # Transformando os dtypes das colunas
    df['TP_FUNDO'] = df['TP_FUNDO'].astype(str)
    df['CNPJ_FUNDO'] = df['CNPJ_FUNDO'].astype(str)
    df['DENOM_SOCIAL'] = df['DENOM_SOCIAL'].astype(str)
    df['DT_COMPTC'] = pd.to_datetime(df['DT_COMPTC'])
    df['TP_APLIC'] = df['TP_APLIC'].astype(str)
    df['TP_ATIVO'] = df['TP_ATIVO'].astype(str)
    df['VL_MERC_POS_FINAL'] = df['VL_MERC_POS_FINAL'].astype(float)
    df['CD_ATIVO'] = df['CD_ATIVO'].astype(str)

    return df


def open_cda_2(path: str) -> pd.DataFrame:
    """
    Formata o arquivo 'cda_fi_BLC_2'.

    Parameters:
    path: caminho do arquivo 'open_cda_2'.

    Returns:
    Dataframe do arquivo 'cda_fi_BLC_2'
    """
    # Lendo o arquivo. Adicionei o 'low_memory=False' para não dar o aviso -> DtypeWarning: Columns (7) have mixed types. Specify dtype option on import or set low_memory=False
    df = pd.read_parquet(path)

    # Verificando as colunas 'TP_FUNDO' ou 'TP_FUNDO_CLASSE'
    if 'TP_FUNDO' in df.columns:
        tp_fundo_col = 'TP_FUNDO'
    elif 'TP_FUNDO_CLASSE' in df.columns:
        tp_fundo_col = 'TP_FUNDO_CLASSE'
    else:
        raise ValueError("Nenhuma das colunas 'TP_FUNDO' ou 'TP_FUNDO_CLASSE' foi encontrada no arquivo.")

    # Verificando as colunas 'CNPJ_FUNDO' ou 'CNPJ_FUNDO_CLASSE'
    if 'CNPJ_FUNDO' in df.columns:
        cnpj_fundo_col = 'CNPJ_FUNDO'
    elif 'CNPJ_FUNDO_CLASSE' in df.columns:
        cnpj_fundo_col = 'CNPJ_FUNDO_CLASSE'
    else:
        raise ValueError("Nenhuma das colunas 'CNPJ_FUNDO' ou 'CNPJ_FUNDO_CLASSE' foi encontrada no arquivo.")
    
    # Verificando as colunas 'NM_FUNDO_COTA' ou 'NM_FUNDO_CLASSE_SUBCLASSE_COTA'
    if 'NM_FUNDO_COTA' in df.columns:
        nm_fundo_cota_col = 'NM_FUNDO_COTA'
    elif 'NM_FUNDO_CLASSE_SUBCLASSE_COTA' in df.columns:
        nm_fundo_cota_col = 'NM_FUNDO_CLASSE_SUBCLASSE_COTA'
    else:
        raise ValueError("Nenhuma das colunas 'NM_FUNDO_COTA' ou 'NM_FUNDO_CLASSE_SUBCLASSE_COTA' foi encontrada no arquivo.")

    # Selecionando apenas os 'Fundos de Investimentos'
    filt_fi = df[tp_fundo_col] == 'FI'
    df = df.loc[filt_fi]

    # Selecionando as principais colunas
    columns_to_keep = [
        tp_fundo_col, 
        cnpj_fundo_col, 
        'DENOM_SOCIAL', 
        'DT_COMPTC', 
        'TP_APLIC', 
        'TP_ATIVO', 
        'VL_MERC_POS_FINAL', 
        nm_fundo_cota_col
    ]
    df = df[columns_to_keep]

    # Renomeando as colunas. Assim fica igual ao df do arquivo cda_fi_BLC_4/7/8 para fazer depois juntar os dfs
    df = df.rename(columns={tp_fundo_col: 'TP_FUNDO', cnpj_fundo_col: 'CNPJ_FUNDO', nm_fundo_cota_col:'CD_ATIVO'})

    # Transformando os dtypes das colunas
    df['TP_FUNDO'] = df['TP_FUNDO'].astype(str)
    df['CNPJ_FUNDO'] = df['CNPJ_FUNDO'].astype(str)
    df['DENOM_SOCIAL'] = df['DENOM_SOCIAL'].astype(str)
    df['DT_COMPTC'] = pd.to_datetime(df['DT_COMPTC'])
    df['TP_APLIC'] = df['TP_APLIC'].astype(str)
    df['TP_ATIVO'] = df['TP_ATIVO'].astype(str)
    df['VL_MERC_POS_FINAL'] = df['VL_MERC_POS_FINAL'].astype(float)
    df['CD_ATIVO'] = df['CD_ATIVO'].astype(str)

    return df


def open_cda_4(path: str) -> pd.DataFrame:
    """
    Formata o arquivo 'cda_fi_BLC_4'.

    Parameters:
    path: caminho do arquivo 'open_cda_4'.

    Returns:
    Dataframe do arquivo 'cda_fi_BLC_4'
    """
    # Lendo o arquivo
    df = pd.read_parquet(path)

    # Verificando as colunas 'TP_FUNDO' ou 'TP_FUNDO_CLASSE'
    if 'TP_FUNDO' in df.columns:
        tp_fundo_col = 'TP_FUNDO'
    elif 'TP_FUNDO_CLASSE' in df.columns:
        tp_fundo_col = 'TP_FUNDO_CLASSE'
    else:
        raise ValueError("Nenhuma das colunas 'TP_FUNDO' ou 'TP_FUNDO_CLASSE' foi encontrada no arquivo.")

    # Verificando as colunas 'CNPJ_FUNDO' ou 'CNPJ_FUNDO_CLASSE'
    if 'CNPJ_FUNDO' in df.columns:
        cnpj_fundo_col = 'CNPJ_FUNDO'
    elif 'CNPJ_FUNDO_CLASSE' in df.columns:
        cnpj_fundo_col = 'CNPJ_FUNDO_CLASSE'
    else:
        raise ValueError("Nenhuma das colunas 'CNPJ_FUNDO' ou 'CNPJ_FUNDO_CLASSE' foi encontrada no arquivo.")

    # Selecionando apenas os 'Fundos de Investimentos' e 'Fundo de Investimento Financeiro'
    filt_fi = df[tp_fundo_col].isin(['FI', 'CLASSES - FIF'])
    df = df.loc[filt_fi]

    # Selecionando as principais colunas
    columns_to_keep = [
        tp_fundo_col, 
        cnpj_fundo_col, 
        'DENOM_SOCIAL', 
        'DT_COMPTC', 
        'TP_APLIC', 
        'TP_ATIVO', 
        'VL_MERC_POS_FINAL', 
        'CD_ATIVO'
    ]
    df = df[columns_to_keep]

    # Renomeando as colunas para manter consistência
    df = df.rename(columns={tp_fundo_col: 'TP_FUNDO', cnpj_fundo_col: 'CNPJ_FUNDO'})

    # Transformando os dtypes das colunas
    df['TP_FUNDO'] = df.loc[:, 'TP_FUNDO'].astype(str)
    df['CNPJ_FUNDO'] = df.loc[:, 'CNPJ_FUNDO'].astype(str)
    df['DENOM_SOCIAL'] = df.loc[:, 'DENOM_SOCIAL'].astype(str)
    df['DT_COMPTC'] = pd.to_datetime(df['DT_COMPTC'])
    df['TP_APLIC'] = df.loc[:, 'TP_APLIC'].astype(str)
    df['TP_ATIVO'] = df.loc[:, 'TP_ATIVO'].astype(str)
    df['VL_MERC_POS_FINAL'] = df.loc[:, 'VL_MERC_POS_FINAL'].astype(float)
    df['CD_ATIVO'] = df.loc[:, 'CD_ATIVO'].astype(str)

    return df


def open_cda_4_v2(path: str) -> pd.DataFrame:
    """
    Formata o arquivo 'cda_fi_BLC_4'.

    Parameters:
    path: caminho do arquivo 'open_cda_4'.

    Returns:
    Dataframe do arquivo 'cda_fi_BLC_4'

    NOTE: Eu criei essa função para analisar melhor as posições de opções dos fundos com duas colunas a mais ('DT_INI_VIGENCIA' e 'DT_FIM_VIGENCIA')
    """
    # Lendo o arquivo
    df = pd.read_parquet(path)

    # Verificando as colunas 'TP_FUNDO' ou 'TP_FUNDO_CLASSE'
    if 'TP_FUNDO' in df.columns:
        tp_fundo_col = 'TP_FUNDO'
    elif 'TP_FUNDO_CLASSE' in df.columns:
        tp_fundo_col = 'TP_FUNDO_CLASSE'
    else:
        raise ValueError("Nenhuma das colunas 'TP_FUNDO' ou 'TP_FUNDO_CLASSE' foi encontrada no arquivo.")

    # Verificando as colunas 'CNPJ_FUNDO' ou 'CNPJ_FUNDO_CLASSE'
    if 'CNPJ_FUNDO' in df.columns:
        cnpj_fundo_col = 'CNPJ_FUNDO'
    elif 'CNPJ_FUNDO_CLASSE' in df.columns:
        cnpj_fundo_col = 'CNPJ_FUNDO_CLASSE'
    else:
        raise ValueError("Nenhuma das colunas 'CNPJ_FUNDO' ou 'CNPJ_FUNDO_CLASSE' foi encontrada no arquivo.")

    # Selecionando apenas os 'Fundos de Investimentos' e 'Fundo de Investimento Financeiro'
    filt_fi = df[tp_fundo_col].isin(['FI', 'CLASSES - FIF'])
    df = df.loc[filt_fi]

    # Selecionando as principais colunas
    columns_to_keep = [
        tp_fundo_col, 
        cnpj_fundo_col, 
        'DENOM_SOCIAL', 
        'DT_COMPTC', 
        'TP_APLIC', 
        'TP_ATIVO', 
        'VL_MERC_POS_FINAL', 
        'CD_ATIVO',
        'DT_INI_VIGENCIA',
        'DT_FIM_VIGENCIA'
    ]
    df = df[columns_to_keep]

    # Renomeando as colunas para manter consistência
    df = df.rename(columns={tp_fundo_col: 'TP_FUNDO', cnpj_fundo_col: 'CNPJ_FUNDO'})

    # Transformando os dtypes das colunas
    df['TP_FUNDO'] = df.loc[:, 'TP_FUNDO'].astype(str)
    df['CNPJ_FUNDO'] = df.loc[:, 'CNPJ_FUNDO'].astype(str)
    df['DENOM_SOCIAL'] = df.loc[:, 'DENOM_SOCIAL'].astype(str)
    df['DT_COMPTC'] = pd.to_datetime(df['DT_COMPTC'])
    df['TP_APLIC'] = df.loc[:, 'TP_APLIC'].astype(str)
    df['TP_ATIVO'] = df.loc[:, 'TP_ATIVO'].astype(str)
    df['VL_MERC_POS_FINAL'] = df.loc[:, 'VL_MERC_POS_FINAL'].astype(float)
    df['CD_ATIVO'] = df.loc[:, 'CD_ATIVO'].astype(str)

    return df


def open_cda_7(path: str) -> pd.DataFrame:
    """
    Formata o arquivo 'cda_fi_BLC_7'.

    Parameters:
    path: caminho do arquivo 'cda_fi_BLC_7'.

    Returns:
    Dataframe do arquivo 'cda_fi_BLC_7'
    """
    # Lendo o arquivo
    df = pd.read_parquet(path)

    # Verificando as colunas 'TP_FUNDO' ou 'TP_FUNDO_CLASSE'
    if 'TP_FUNDO' in df.columns:
        tp_fundo_col = 'TP_FUNDO'
    elif 'TP_FUNDO_CLASSE' in df.columns:
        tp_fundo_col = 'TP_FUNDO_CLASSE'
    else:
        raise ValueError("Nenhuma das colunas 'TP_FUNDO' ou 'TP_FUNDO_CLASSE' foi encontrada no arquivo.")

    # Verificando as colunas 'CNPJ_FUNDO' ou 'CNPJ_FUNDO_CLASSE'
    if 'CNPJ_FUNDO' in df.columns:
        cnpj_fundo_col = 'CNPJ_FUNDO'
    elif 'CNPJ_FUNDO_CLASSE' in df.columns:
        cnpj_fundo_col = 'CNPJ_FUNDO_CLASSE'
    else:
        raise ValueError("Nenhuma das colunas 'CNPJ_FUNDO' ou 'CNPJ_FUNDO_CLASSE' foi encontrada no arquivo.")

    # Selecionando apenas os 'Fundos de Investimentos'
    filt_fi = df[tp_fundo_col] == 'FI'
    df = df.loc[filt_fi]

    # Selecionando as principais colunas
    columns_to_keep = [
        tp_fundo_col, 
        cnpj_fundo_col, 
        'DENOM_SOCIAL', 
        'DT_COMPTC', 
        'TP_APLIC', 
        'TP_ATIVO', 
        'VL_MERC_POS_FINAL', 
        'EMISSOR'
    ]
    df = df[columns_to_keep]

    # Renomeando as colunas. Assim fica igual ao df do arquivo cda_fi_BLC_4 p/ fazer depois juntar os dfs.
    df.rename(columns={tp_fundo_col: 'TP_FUNDO', cnpj_fundo_col: 'CNPJ_FUNDO', 'EMISSOR': 'CD_ATIVO'}, inplace=True)

    # Transformando os dtypes das colunas.
    df['TP_FUNDO'] = df.loc[:, 'TP_FUNDO'].astype(str)
    df['CNPJ_FUNDO'] = df.loc[:, 'CNPJ_FUNDO'].astype(str)
    df['DENOM_SOCIAL'] = df.loc[:, 'DENOM_SOCIAL'].astype(str)
    df['DT_COMPTC'] = pd.to_datetime(df['DT_COMPTC'])
    df['TP_APLIC'] = df.loc[:, 'TP_APLIC'].astype(str)
    df['TP_ATIVO'] = df.loc[:, 'TP_ATIVO'].astype(str)
    df['VL_MERC_POS_FINAL'] = df.loc[:, 'VL_MERC_POS_FINAL'].astype(float)
    df['CD_ATIVO'] = df.loc[:, 'CD_ATIVO'].astype(str)

    return df


def open_cda_8(path: str) -> pd.DataFrame:
    """
    Formata o arquivo 'cda_fi_BLC_8'.

    Parameters:
    path: caminho do arquivo 'cda_fi_BLC_8'.

    Returns:
    Dataframe do arquivo 'cda_fi_BLC_8'
    """
    # Lendo o arquivo
    df = pd.read_parquet(path)

    # Verificando as colunas 'TP_FUNDO' ou 'TP_FUNDO_CLASSE'
    if 'TP_FUNDO' in df.columns:
        tp_fundo_col = 'TP_FUNDO'
    elif 'TP_FUNDO_CLASSE' in df.columns:
        tp_fundo_col = 'TP_FUNDO_CLASSE'
    else:
        raise ValueError("Nenhuma das colunas 'TP_FUNDO' ou 'TP_FUNDO_CLASSE' foi encontrada no arquivo.")

    # Verificando as colunas 'CNPJ_FUNDO' ou 'CNPJ_FUNDO_CLASSE'
    if 'CNPJ_FUNDO' in df.columns:
        cnpj_fundo_col = 'CNPJ_FUNDO'
    elif 'CNPJ_FUNDO_CLASSE' in df.columns:
        cnpj_fundo_col = 'CNPJ_FUNDO_CLASSE'
    else:
        raise ValueError("Nenhuma das colunas 'CNPJ_FUNDO' ou 'CNPJ_FUNDO_CLASSE' foi encontrada no arquivo.")

    # Selecionando apenas os 'Fundos de Investimentos'
    filt_fi = df[tp_fundo_col] == 'FI'
    df = df.loc[filt_fi]

    # Selecionando as principais colunas
    columns_to_keep = [
        tp_fundo_col, 
        cnpj_fundo_col, 
        'DENOM_SOCIAL', 
        'DT_COMPTC', 
        'TP_APLIC', 
        'TP_ATIVO', 
        'VL_MERC_POS_FINAL', 
        'DS_ATIVO'
    ]
    df = df[columns_to_keep]

    # Renomeando as colunas. Assim fica igual ao df do arquivo cda_fi_BLC_4 p/ fazer depois juntar os dfs
    df.rename(columns={tp_fundo_col: 'TP_FUNDO', cnpj_fundo_col: 'CNPJ_FUNDO', 'DS_ATIVO': 'CD_ATIVO'}, inplace=True)

    # Transformando os dtypes das colunas
    df['TP_FUNDO'] = df.loc[:, 'TP_FUNDO'].astype(str)
    df['CNPJ_FUNDO'] = df.loc[:, 'CNPJ_FUNDO'].astype(str)
    df['DENOM_SOCIAL'] = df.loc[:, 'DENOM_SOCIAL'].astype(str)
    df['DT_COMPTC'] = pd.to_datetime(df['DT_COMPTC'])
    df['TP_APLIC'] = df.loc[:, 'TP_APLIC'].astype(str)
    df['TP_ATIVO'] = df.loc[:, 'TP_ATIVO'].astype(str)
    df['VL_MERC_POS_FINAL'] = df.loc[:, 'VL_MERC_POS_FINAL'].astype(float)
    df['CD_ATIVO'] = df.loc[:, 'CD_ATIVO'].astype(str)

    # Selecionando apenas o ativo 'BDR' e 'Ações' (Units), porque neste arquivo também possui um ativo chamado 'Títulos Públicos', mas não é o principal 'Títulos Públicos', que está no 'cda_fi_BLC_1'
    filt = (df['TP_APLIC'] == 'Brazilian Depository Receipt - BDR') | (df['TP_APLIC'] == 'Ações')
    df = df.loc[filt].sort_values(by='VL_MERC_POS_FINAL', ascending=False)
    
    return df


def pl_fundo(path: str, cnpj: str) -> pd.DataFrame:
    """
    Formata o arquivo 'cda_fi_PL'.
    
    Paramenters:
    path: caminho do arquivo 'cda_fi_PL'.
    cnpj: cnpj do fundo de investimento.

    Returns:
    Dataframe com o valor do patrimônio líquido do fundo de investimentos específico.
    """
    # Lendo o arquivo
    df = pd.read_parquet(path)

    # Verificando as colunas 'TP_FUNDO' ou 'TP_FUNDO_CLASSE'
    if 'TP_FUNDO' in df.columns:
        tp_fundo_col = 'TP_FUNDO'
    elif 'TP_FUNDO_CLASSE' in df.columns:
        tp_fundo_col = 'TP_FUNDO_CLASSE'
    else:
        raise ValueError("Nenhuma das colunas 'TP_FUNDO' ou 'TP_FUNDO_CLASSE' foi encontrada no arquivo.")

    # Verificando as colunas 'CNPJ_FUNDO' ou 'CNPJ_FUNDO_CLASSE'
    if 'CNPJ_FUNDO' in df.columns:
        cnpj_fundo_col = 'CNPJ_FUNDO'
    elif 'CNPJ_FUNDO_CLASSE' in df.columns:
        cnpj_fundo_col = 'CNPJ_FUNDO_CLASSE'
    else:
        raise ValueError("Nenhuma das colunas 'CNPJ_FUNDO' ou 'CNPJ_FUNDO_CLASSE' foi encontrada no arquivo.")

    # Selecionando apenas os 'Fundos de Investimentos' e 'Fundo de Investimento Financeiro'
    filt_fi = df[tp_fundo_col].isin(['FI', 'CLASSES - FIF'])
    df = df.loc[filt_fi]

    # Selecionando o fundo de investimentos específicos
    filt_cnpj = df[cnpj_fundo_col] == cnpj
    fundo_espec = df.loc[filt_cnpj]

    # # Transformando os dtypes da coluna
    # fundo_espec['VL_PATRIM_LIQ'] = fundo_espec.loc[:, 'VL_PATRIM_LIQ'].astype(float)

    return fundo_espec['VL_PATRIM_LIQ']


def fundo_cnpj(df: pd.DataFrame, cnpj: str) -> pd.DataFrame:
    """
    Separa o df do fundo de investimentos em várias categorias.

    Parameters:
    df: Dataframe que contém os ativos dos fundos.
    cnpj: cnpj do fundo de investimento.

    Returns:
    Vários dataframes de categorias diferentes: ações, BDRs, investimentos no exterior, cotas de fundos e títulos públicos.
    """
    # Lendo o df concatenado
    filt_cnpj = df['CNPJ_FUNDO'] == cnpj
    fundo_espec = df.loc[filt_cnpj]

    # Ações
    filt_acoes = (fundo_espec['TP_APLIC'] == 'Ações')
    # Selecionando pelo em ordem da maior posição do fundo p/ a menor
    df_acoes = fundo_espec.loc[filt_acoes].sort_values(by='VL_MERC_POS_FINAL', ascending=False)
    # Calculando quantos porcentos representa cada ação
    porcentagem_acao = lambda x: (x / df_acoes['VL_MERC_POS_FINAL'].sum())
    # Criando a coluna 'PORCENTAGEM'
    df_acoes['PORCENTAGEM'] = list(map(porcentagem_acao, df_acoes['VL_MERC_POS_FINAL']))
    # Selecionando apenas as colunas necessárias
    df_acoes = df_acoes.loc[:,['DENOM_SOCIAL', 'CD_ATIVO', 'PORCENTAGEM', 'VL_MERC_POS_FINAL']]

    # BDRs
    filt_bdr = (fundo_espec['TP_APLIC'] == 'Brazilian Depository Receipt - BDR')
    # Selecionando pelo em ordem da maior posição do fundo p/ a menor
    df_bdr = fundo_espec.loc[filt_bdr].sort_values(by='VL_MERC_POS_FINAL', ascending=False)
    # Calculando quantos porcentos representa cada ação
    porcentagem_bdr = lambda x: (x / df_bdr['VL_MERC_POS_FINAL'].sum())
    # Criando a coluna 'PORCENTAGEM'
    df_bdr['PORCENTAGEM'] = list(map(porcentagem_bdr, df_bdr['VL_MERC_POS_FINAL']))
    # Selecionando apenas as colunas necessárias
    df_bdr =  df_bdr.loc[:,['DENOM_SOCIAL', 'CD_ATIVO', 'PORCENTAGEM', 'VL_MERC_POS_FINAL']]

    # Investimentos Exterior
    filt_exterior = (fundo_espec['TP_APLIC'] == 'Investimento no Exterior')
    # Selecionando pelo em ordem da maior posição do fundo p/ a menor
    df_exterior = fundo_espec.loc[filt_exterior].sort_values(by='VL_MERC_POS_FINAL', ascending=False)
    # Calculando quantos porcentos representa cada ação
    porcentagem_exterior = lambda x: (x / df_exterior['VL_MERC_POS_FINAL'].sum())
    # Criando a coluna 'PORCENTAGEM'
    df_exterior['PORCENTAGEM'] = list(map(porcentagem_exterior, df_exterior['VL_MERC_POS_FINAL']))
    # Selecionando apenas as colunas necessárias
    df_exterior = df_exterior.loc[:,['DENOM_SOCIAL', 'CD_ATIVO', 'PORCENTAGEM', 'VL_MERC_POS_FINAL']]

    # Cotas de Fundos
    filt_cotas_fundos = (fundo_espec['TP_APLIC'] == 'Cotas de Fundos')
    # Selecionando pelo em ordem da maior posição do fundo p/ a menor
    df_cotas_fundos = fundo_espec.loc[filt_cotas_fundos].sort_values(by='VL_MERC_POS_FINAL', ascending=False)
    # Calculando quantos porcentos representa cada cota de fundo
    porcentagem_cotas = lambda x: (x / df_cotas_fundos['VL_MERC_POS_FINAL'].sum())
    # Criando a coluna 'PORCENTAGEM'
    df_cotas_fundos['PORCENTAGEM'] = list(map(porcentagem_cotas, df_cotas_fundos['VL_MERC_POS_FINAL']))
    # Selecionando apenas as colunas necessárias
    df_cotas_fundos = df_cotas_fundos.loc[:,['DENOM_SOCIAL', 'CD_ATIVO', 'PORCENTAGEM', 'VL_MERC_POS_FINAL']]

    # Títulos públicos
    filt_titulos_pub = (fundo_espec['TP_APLIC'] == 'Títulos Públicos')
    # Selecionando pelo em ordem da maior posição do fundo p/ a menor
    df_titulos_pub = fundo_espec.loc[filt_titulos_pub].sort_values(by='VL_MERC_POS_FINAL', ascending=False)
    # Calculando quantos porcentos representa cada título público
    porcentagem_titulos = lambda x: (x / df_titulos_pub['VL_MERC_POS_FINAL'].sum())
    # Criando a coluna 'PORCENTAGEM'
    df_titulos_pub['PORCENTAGEM'] = list(map(porcentagem_titulos, df_titulos_pub['VL_MERC_POS_FINAL']))
    # Selecionando apenas as colunas necessárias
    df_titulos_pub = df_titulos_pub.loc[:,['DENOM_SOCIAL', 'CD_ATIVO', 'PORCENTAGEM', 'VL_MERC_POS_FINAL']]

    # Obrigações por ações e outros TVM recebidos em empréstimo
    filt_vendido_acoes = (fundo_espec['TP_APLIC'] == 'Obrigações por ações e outros TVM recebidos em empréstimo')
    # Selecionando pelo em ordem da maior posição do fundo p/ a menor
    df_vendido_acoes = fundo_espec.loc[filt_vendido_acoes].sort_values(by='VL_MERC_POS_FINAL', ascending=False)
    # Calculando quantos porcentos representa cada título público
    porcentagem_vendido = lambda x: (x / df_vendido_acoes['VL_MERC_POS_FINAL'].sum())
    # Criando a coluna 'PORCENTAGEM'
    df_vendido_acoes['PORCENTAGEM'] = list(map(porcentagem_vendido, df_vendido_acoes['VL_MERC_POS_FINAL']))
    # Selecionando apenas as colunas necessárias
    df_vendido_acoes = df_vendido_acoes.loc[:,['DENOM_SOCIAL', 'CD_ATIVO', 'PORCENTAGEM', 'VL_MERC_POS_FINAL']]

    return (
        df_acoes, 
        df_bdr, 
        df_exterior, 
        df_cotas_fundos, 
        df_titulos_pub, 
        df_vendido_acoes
    )


def fundo_cnpj_acoes(df: pd.DataFrame, cnpj: str) -> pd.DataFrame:
    """
    Separa o df do fundo de investimento apenas na categoria de ações.

    Parameters:
    df: Dataframe que contém os ativos dos fundos.
    cnpj: cnpj do fundo de investimento.

    Returns:
    Dataframe das ações do fundo selecionado.
    """
    # Lendo o df concatenado
    filt_cnpj = df['CNPJ_FUNDO'] == cnpj
    fundo_espec = df.loc[filt_cnpj]

    # Ações
    filt_acoes = (fundo_espec['TP_APLIC'] == 'Ações')
    # Selecionando em ordem da maior posição do fundo p/ a menor
    df_acoes = fundo_espec.loc[filt_acoes].sort_values(by='VL_MERC_POS_FINAL', ascending=False)
    # Calculando quantos porcentos representa cada ação
    porcentagem_acao = lambda x: (x / df_acoes['VL_MERC_POS_FINAL'].sum())
    # Criando a coluna 'PORCENTAGEM'
    df_acoes['PORCENTAGEM'] = list(map(porcentagem_acao, df_acoes['VL_MERC_POS_FINAL']))
    # Selecionando apenas as colunas necessárias
    df_acoes = df_acoes.loc[:,['DT_COMPTC', 'DENOM_SOCIAL', 'CD_ATIVO', 'PORCENTAGEM', 'VL_MERC_POS_FINAL']]
    # Renomeando a coluna 'DT_COMPTC' para 'data'
    df_acoes = df_acoes.rename(columns={'DT_COMPTC' : 'data'})
    # Selecionando a coluna 'data' como index
    df_acoes = df_acoes.set_index('data')

    return df_acoes


def fundo_cnpj_debentures(df: pd.DataFrame, cnpj: str) -> pd.DataFrame:
    """
    Separa o df do fundo de investimento apenas na categoria de debêntures.

    Parameters:
    df: Dataframe que contém os ativos dos fundos.
    cnpj: cnpj do fundo de investimento.

    Returns:
    Dataframe das debêntures do fundo selecionado.
    """
    # Lendo o df concatenado
    filt_cnpj = df['CNPJ_FUNDO'] == cnpj
    fundo_espec = df.loc[filt_cnpj]

    # Debêntures
    filt_debentures = (fundo_espec['TP_APLIC'] == 'Debêntures')
    # Selecionando pelo em ordem da maior posição do fundo p/ a menor
    df_debentures = fundo_espec.loc[filt_debentures].sort_values(by='VL_MERC_POS_FINAL', ascending=False)
    # Calculando quantos porcentos representa cada debênture
    porcentagem_debentures = lambda x: (x / df_debentures['VL_MERC_POS_FINAL'].sum())
    # Criando a coluna 'PORCENTAGEM'
    df_debentures['PORCENTAGEM'] = list(map(porcentagem_debentures, df_debentures['VL_MERC_POS_FINAL']))
    # Selecionando apenas as colunas necessárias
    df_debentures = df_debentures.loc[:,['DENOM_SOCIAL', 'CD_ATIVO', 'PORCENTAGEM', 'VL_MERC_POS_FINAL']]

    return df_debentures


def fundo_cnpj_opcoes(df: pd.DataFrame, cnpj: str) -> pd.DataFrame:
    """
    Separa o df do fundo de investimento apenas na categoria de opções.

    Parameters:
    df: Dataframe que contém os ativos dos fundos.
    cnpj: cnpj do fundo de investimento.

    Returns:
    df_opcoes_compradas: Dataframe das opções compradas do fundo selecionado.
    df_opcoes_vendidas: Dataframe das opções vendidas do fundo selecionado.
    """
    # Lendo o df concatenado
    filt_cnpj = df['CNPJ_FUNDO'] == cnpj
    fundo_espec = df.loc[filt_cnpj]

    # Opções - posições titulares
    filt_opcoes_compradas = (fundo_espec['TP_APLIC'] == 'Opções - Posições titulares')
    # Selecionando em ordem da maior posição do fundo p/ a menor
    df_opcoes_compradas = fundo_espec.loc[filt_opcoes_compradas].sort_values(by='DT_COMPTC', ascending=True)
    # Calculando quantos porcentos representa cada ação
    porcentagem_opcoes_compradas = lambda x: (x / df_opcoes_compradas['VL_MERC_POS_FINAL'].sum())
    # Criando a coluna 'PORCENTAGEM'
    df_opcoes_compradas['PORCENTAGEM'] = list(map(porcentagem_opcoes_compradas, df_opcoes_compradas['VL_MERC_POS_FINAL']))
    # Selecionando apenas as colunas necessárias
    df_opcoes_compradas = df_opcoes_compradas.loc[:,['DT_COMPTC', 'DENOM_SOCIAL', 'CD_ATIVO', 'PORCENTAGEM', 'VL_MERC_POS_FINAL', 'DT_INI_VIGENCIA', 'DT_FIM_VIGENCIA']]
    # Renomeando a coluna 'DT_COMPTC' para 'data'
    df_opcoes_compradas = df_opcoes_compradas.rename(columns={'DT_COMPTC' : 'data'})
    # Selecionando a coluna 'data' como index
    df_opcoes_compradas = df_opcoes_compradas.set_index('data')

    # Opções - posições lançadas
    filt_opcoes_vendidas = (fundo_espec['TP_APLIC'] == 'Opções - Posições lançadas')
    # Selecionando em ordem da maior posição do fundo p/ a menor
    df_opcoes_vendidas = fundo_espec.loc[filt_opcoes_vendidas].sort_values(by='DT_COMPTC', ascending=True)
    # Calculando quantos porcentos representa cada ação
    porcentagem_opcoes_compradas = lambda x: (x / df_opcoes_vendidas['VL_MERC_POS_FINAL'].sum())
    # Criando a coluna 'PORCENTAGEM'
    df_opcoes_vendidas['PORCENTAGEM'] = list(map(porcentagem_opcoes_compradas, df_opcoes_vendidas['VL_MERC_POS_FINAL']))
    # Selecionando apenas as colunas necessárias
    df_opcoes_vendidas = df_opcoes_vendidas.loc[:,['DT_COMPTC', 'DENOM_SOCIAL', 'CD_ATIVO', 'PORCENTAGEM', 'VL_MERC_POS_FINAL', 'DT_INI_VIGENCIA', 'DT_FIM_VIGENCIA']]
    # Renomeando a coluna 'DT_COMPTC' para 'data'
    df_opcoes_vendidas = df_opcoes_vendidas.rename(columns={'DT_COMPTC' : 'data'})
    # Selecionando a coluna 'data' como index
    df_opcoes_vendidas = df_opcoes_vendidas.set_index('data')

    return df_opcoes_compradas, df_opcoes_vendidas


def comparar_portfolios(df: pd.DataFrame, nome_fundo: str) -> str:
    """
    Comparação do portfólio - quais foram as ações que foram compradas e vendidas em relação ao mês anteirior.

    Pameters:
    df: DataFrame de cada mês do portfólio do fundo.
    nome_fundo: nome do fundo.

    Returns:
    Texto com as mudanças do portfólio.
    """
    for i in range(1, len(df)):
        mes_atual = df.iloc[i]
        mes_anterior = df.iloc[i - 1]
        
        data_atual = mes_atual['data']
        data_anterior = mes_anterior['data']
        
        acoes_atual = mes_atual['CD_ATIVO']
        acoes_anterior = mes_anterior['CD_ATIVO']
        
        vendidas = acoes_anterior - acoes_atual
        compradas = acoes_atual - acoes_anterior
        
        print(f'Comparando o portfólio de {data_anterior.strftime("%m/%Y")} e {data_atual.strftime("%m/%Y")}:')
        print(f'O {nome_fundo} vendeu as ações: {vendidas}')
        print(f'O {nome_fundo} comprou as ações: {compradas}')
        print('-' * 80)


def num_total_acoes(df: pd.DataFrame) -> pd.Series:
    """
    Mostra o número total do portfólio do fundo.

    Parameters:
    df: DataFrame do portfólio do fundo.

    Returns:
    Número total de ações de cada mês do portfpolio.
    """
    num_total_acoes = df.groupby('data')['CD_ATIVO'].count()
    
    return num_total_acoes


def rank_top_5(df: pd.DataFrame) -> pd.Series:
    """
    Mostra o rank das 5 maiores posições do fundo.

    Parameters:
    df: DataFrame do portfólio do fundo.
    
    Returns:
    rank_portfolio_fundo: rank das 5 maiores posições do fundo.
    """
    # Selecionando os valores únicos (datas) do df
    lst_data = df.index.unique()

    # Rank das 5 maiores posições do fundo
    lst_rank = []
    for _ in lst_data:
        rank = df.loc[_].nlargest(5, 'PORCENTAGEM')[['CD_ATIVO', 'PORCENTAGEM']]
        lst_rank.append(rank)

    # Concatendo os dfs de rank
    rank_portfolio = pd.concat(lst_rank)
    rank_portfolio_fundo = rank_portfolio.groupby('data')['CD_ATIVO'].apply(list)

    return rank_portfolio_fundo


def plot_portfolio(df: pd.DataFrame, nome_fundo: str):
    """
    Faz o plot do portfólio do fundo.

    Parameters:
    df: Dataframe do portfólio do fundo selecionado.
    nome_fundo: nome do fundo.

    Returns:
    Plot do portfólio do fundo.
    """
    # Extraindo os meses únicos do índice
    months = df.index.to_period('M').unique()

    # Títulos dos subplots
    titulos = [f'Distribuição Percentual do Portfólio - {month.strftime("%m/%Y")}' for month in months]

    # Criando a figura com subplots
    fig = make_subplots(rows=len(months),
                        cols=1,
                        subplot_titles=titulos,
                        vertical_spacing=0.02  # Espaço entre os plots
    )

    # Iterando sobre os meses
    for idx, month in enumerate(months):
        # Extraindo os dados do portfólio para o mês atual
        portfolio_mes = df.loc[month.strftime('%Y-%m'), ['CD_ATIVO', 'PORCENTAGEM']]
        
        fig.add_trace(go.Bar(
            x=portfolio_mes['PORCENTAGEM'] * 100,
            y=portfolio_mes['CD_ATIVO'],
            orientation='h',
            name=month.strftime('%m/%Y')
        ), row=idx + 1, col=1)

    fig.update_layout(
        title=f'Portfólio do Fundo {nome_fundo}',
        height=6000,
        width=900
    )

    return fig.show()


def open_arquivos_fii(fii_ativo_passivo_path: str, fii_complemento_path: str, fii_geral_path: str) -> pd.DataFrame:
    """
    Formata e compila os arquivos mensais dos FIIs.

    Parameters:
    fii_ativo_passivo_path: caminho do arquivo "inf_mensal_fii_ativo_passivo_XXXX".
    fii_complemento_path: caminho do arquivo "inf_mensal_fii_complemento_XXXX".
    fii_geral_path: caminho do arquivo "inf_mensal_fii_geral_XXXX".

    Returns:
    df_fii: df com os principais dados sobre os FIIs.
    """
    # Lendo os arquivos parquet
    df_ativo_passivo = pd.read_parquet(fii_ativo_passivo_path)
    df_complemento = pd.read_parquet(fii_complemento_path)
    df_geral = pd.read_parquet(fii_geral_path)

    # Identificando o nome correto da coluna de CNPJ em cada DataFrame
    cnpj_fundo_col_ativo_passivo = 'CNPJ_Fundo' if 'CNPJ_Fundo' in df_ativo_passivo.columns else 'CNPJ_Fundo_Classe'
    cnpj_fundo_col_complemento = 'CNPJ_Fundo' if 'CNPJ_Fundo' in df_complemento.columns else 'CNPJ_Fundo_Classe'
    cnpj_fundo_col_geral = 'CNPJ_Fundo' if 'CNPJ_Fundo' in df_geral.columns else 'CNPJ_Fundo_Classe'

    # Renomeando a coluna de CNPJ para o mesmo nome p/ fazer a junção dos dfs
    df_ativo_passivo.rename(columns={cnpj_fundo_col_ativo_passivo: 'CNPJ_Fundo'}, inplace=True)
    df_complemento.rename(columns={cnpj_fundo_col_complemento: 'CNPJ_Fundo'}, inplace=True)
    df_geral.rename(columns={cnpj_fundo_col_geral: 'CNPJ_Fundo'}, inplace=True)

    # Selecionando as principais colunas
    df_ativo_passivo = df_ativo_passivo[[
        'Data_Referencia', 
        'CNPJ_Fundo', 
        'Obrigacoes_Aquisicao_Imoveis', 
        'Obrigacoes_Securitizacao_Recebiveis'
    ]]
    df_complemento = df_complemento[[
        'Data_Referencia', 
        'CNPJ_Fundo', 
        'Valor_Ativo', 
        'Patrimonio_Liquido', 
        'Cotas_Emitidas',
        'Valor_Patrimonial_Cotas', 
        'Percentual_Rentabilidade_Efetiva_Mes', 
        'Percentual_Dividend_Yield_Mes'
    ]]
    df_geral = df_geral[[
        'Data_Referencia', 
        'CNPJ_Fundo', 
        'Segmento_Atuacao'
    ]]

    # Ajustando a escala das colunas de porcentagem
    df_complemento['Percentual_Rentabilidade_Efetiva_Mes'] = round(df_complemento['Percentual_Rentabilidade_Efetiva_Mes'] * 100, 2)
    df_complemento['Percentual_Dividend_Yield_Mes'] = round(df_complemento['Percentual_Dividend_Yield_Mes'] * 100, 2)

    # Transformando em datetime a coluna 'Data_Referencia' e removendos os espaços em branco da coluna 'CNPJ_Fundo'
    for df in [df_ativo_passivo, df_complemento, df_geral]:
        df['Data_Referencia'] = pd.to_datetime(df['Data_Referencia'], format='%Y-%m-%d')
        df['CNPJ_Fundo'] = df['CNPJ_Fundo'].str.strip()

    # Lista dos dfs que vão ser mesclados
    lst_dfs = [df_ativo_passivo, df_complemento, df_geral]

    # Mesclando os dataframes
    df_fii = reduce(lambda left, right: pd.merge(left, right, on=['CNPJ_Fundo', 'Data_Referencia']), lst_dfs)

    # Calculando a dívida total
    df_fii['Divida_Total'] = df_fii['Obrigacoes_Aquisicao_Imoveis'] + df_fii['Obrigacoes_Securitizacao_Recebiveis']

    # Calculando o grau de alavancagem
    df_fii['Grau_Alavancagem'] = (df_fii['Divida_Total'] / df_fii['Valor_Ativo']) * 100

    # Preenchendo os NaN com zero
    df_fii = df_fii.fillna(0)

    return df_fii


def fii_cnpj(df: pd.DataFrame, cnpj: str, ticker: str) -> pd.DataFrame:
    """
    Mostra os principais indicadores do FII selecionado.

    Parameters:
    df: df que contém todos os FIIs
    cnpj: cnpj do FII selecionado.
    ticker: ticker do FII selecionado.

    Returns:
    df_fii_final: df do FII selecionado com os seus principais indicadores.
    """
    # Selecionando um FII específico
    filt = df['CNPJ_Fundo'] == cnpj
    df_fii_espec = df.loc[filt]

    # Primeiro Ano
    primeiro_ano = df.index[0].year
    # Primeiro mês
    primeiro_mes = df.index[0].month
    # Último Ano
    ultimo_ano = df.index[-1].year
    # Último mês
    ultimo_mes = df.index[-1].month

    # Se o 'ultimo_mes' for diferente de 12, adicionar somar 1 no 'ultimo_mes'
    if ultimo_mes != 12:
        # Fazendo o download dos preço do FII
        fii_preco = yf.download(ticker, start=f'{primeiro_ano}-{primeiro_mes}-01', end=f'{ultimo_ano}-{ultimo_mes+1}-01', auto_adjust=True)['Close']

    else: 
        # Fazendo o download dos preço do FII
        fii_preco = yf.download(ticker, start=f'{primeiro_ano}-{primeiro_mes}-01', end=f'{ultimo_ano}-{ultimo_mes}-01', auto_adjust=True)['Close']

    # Resetando o index do df
    fii_preco = fii_preco.reset_index()
    # Renomeando as colunas 
    fii_preco.columns = ['Data_Referencia', 'Close']
    # Transformando a coluna 'Data_Referencia' em  index 
    fii_preco = fii_preco.set_index('Data_Referencia')
    # Usando o resample para agrupar por mês e selecionando o último valor de cada mês
    fii_preco = fii_preco.resample('M').last()
    # Transformando os dias do index para 01 p/ juntar com o 'df_ifix'
    new_index = fii_preco.index.to_period('M').to_timestamp() + pd.offsets.Day(0)
    # Index novo em que o dia é 01
    fii_preco.index = new_index
    # Cortando o df para ficar no mesmo tamanho do 'df_ifix'
    fii_preco = fii_preco.loc[:f'{ultimo_ano}-{ultimo_mes}']

    # Juntando os dfs
    df_fii_final = pd.merge(fii_preco, df_fii_espec, left_index=True, right_index=True)

    # Calculando o P/VP
    df_fii_final['P/VP'] = round(df_fii_final['Close'] / df_fii_final['Valor_Patrimonial_Cotas'], 2)

    return df_fii_final


def plot_historico_p_vp(df: pd.DataFrame, nome_segmento: str):
    """
    Parameters:
    df:  df do FII que contém a coluna 'P/VP'.
    nome_segmento: nome do segmento do FII.

    Returns:
    Plot do histórico do indicador P/VP dos FIIs.
    """
    # Plotando o histórico do indicador P/VP dos FIIs
    fig = go.Figure()

    for empresa in df['Ticker'].unique():
        fig.add_trace(go.Scatter(
                x=df.loc[df['Ticker'] == empresa].index,
                y=df.loc[df['Ticker'] == empresa, 'P/VP'],
                name=empresa
            ))

    fig.add_hline(y=1, line_color='red', line_width=0.5)

    fig.update_layout(title=f'Histórico do P/VP dos FIIs de {nome_segmento}')

    fig.show()


def filtro_etf(path: str) -> pd.DataFrame:
  """
  Função que filtra a composição da carteira do ETF (IBOV e SMAL11).

  Parameters:
  path: caminho do arquivo do ETF.
  
  Returns:
  DataFrame do ETF.
  """
  # Abrindo arquivo Ibovespa (carteira teórica)
  df = pd.read_csv(path, 
                   sep=';',
                   encoding='ISO-8859-1', 
                   engine='python', 
                   header=1, #cabeçalho vira a 2º linha da tabela
                   skipfooter=2, #pula as duas últimas linhas da tabela
                   index_col=False)
  
  # Convertendo o dtypes das colunas
  df['Código'] = df['Código'].astype(str)
  df['Ação'] = df['Ação'].astype(str)
  df['Tipo'] = df['Tipo'].astype(str)
  df['Qtde. Teórica'] = df['Qtde. Teórica'].str.replace('.', '', regex=False)
  df['Qtde. Teórica'] = df['Qtde. Teórica'].astype(float)
  df['Part. (%)'] = df['Part. (%)'].str.replace(',', '.', regex=False)
  df['Part. (%)'] = df['Part. (%)'].astype(float)

  return df


def vol_anual(ticker: str, ano: str) -> pd.Series:
    """
    Função que calcula a volatilidade anualizada.

    Parameters:
    ticker: ticker da empresa.
    ano: período escolhido.

    Returns:
    annualized_volatility : volatilidade anualiazada.

    NOTE: para calcular a vol mensal trocar apenas o np.sqrt(12).
    NOTE: para calcular a vol semanal trocar apenas o np.sqrt(52).
    """
    # Preço de fechamento do ativo
    df_preco = yf.download(ticker, auto_adjust=True)['Close']
    # Resetando o index do df
    df_preco = df_preco.reset_index()
    # Transformando a coluna 'Date' em  index 
    df_preco = df_preco.set_index('Date')
    # Calculando o retorno logarítmico
    log_return = np.log(df_preco.loc[ano, ticker] / df_preco.loc[ano, ticker].shift(1))

    # Calculando a volatilidade anualizada
    annualized_volatility = round((np.std(log_return) * np.sqrt(252)) * 100, 2)

    return annualized_volatility 


def drawdown(ticker: str) -> pd.Series:
    """
    Função que calcula drawdown.

    Parameters:
    ticker: ticker da empresa.

    Returns:
    Ponto mínimo do drawdown.
    """
    # Df do preço do ativo
    df_preco = yf.download(ticker, auto_adjust=True)['Close']
    # Resetando o index do df
    df_preco = df_preco.reset_index()
    # Transformando a coluna 'Date' em  index 
    df_preco = df_preco.set_index('Date')

    # Calculando o retorno diário
    df_returns = df_preco.pct_change().dropna()

    # Calculando o retorno acumulado
    cumulative_returns = (1+df_returns).cumprod()

    # Calculando o pico
    peak = cumulative_returns.expanding(min_periods=1).max()

    # Calculando o drawdown
    drawdown = round(((cumulative_returns / peak) - 1) * 100, 2)

    return drawdown.min()


def ret_acumulado(ticker: str, setor: str, df_benchmark: pd.DataFrame):
    """
    Parameters:
    ticker: ticker do ativo.
    setor: nome do setor.
    df_benchmark: df com os preços de fechamento ('Close') e variação percentual ('pct_change') do benchmark.

    Return:
    Plot do retorno acumulado do(s) ativo(s).
    """
    # Df do preço do ativo
    df_preco = yf.download(ticker, auto_adjust=True)['Close']
    # Resetando o index do df
    df_preco = df_preco.reset_index()
    # Transformando a coluna 'Date' em  index 
    df_preco = df_preco.set_index('Date')

    # Calculando o retorno diário
    df_returns = df_preco.pct_change().dropna()
    # Calculando o retorno acumulado
    ret_accum = (1 + df_returns).cumprod()
    # Primeiro dia começa em 1
    ret_accum.iloc[0] = 1

    # Selecionando o mesmo período dos FIIs para o df do benchmark
    benchmark_precos_sliced = df_benchmark.loc[ret_accum.index[0]:, ['Close', 'pct_change']]
    # Calculando a variação percentual acumulada
    benchmark_precos_sliced['pct_change_accum'] = round((1 + (benchmark_precos_sliced['pct_change']/100)).cumprod(), 4) 
    # Primeiro dia começa em 1
    benchmark_precos_sliced['pct_change_accum'].iloc[0] = 1

    # Plotando o retorno acumulado
    fig = go.Figure()

    for empresa in ret_accum:
        fig.add_trace(go.Scatter(
            x=ret_accum.index,
            y=ret_accum[empresa],
            name=empresa
        ))

    fig.add_trace(go.Scatter(
        x=benchmark_precos_sliced.index,
        y=benchmark_precos_sliced['pct_change_accum'],
        name='IFIX'
    ))

    fig.update_layout(
        height=800,
        title_text=f'Retorno Acumulado - Setor de {setor}',
        template='seaborn'
    )

    fig.add_hline(y=1, line_width=1, line_color='red')

    return ret_accum, fig.show()


def ret_anual(ticker:str, setor: str, df_benchmark: pd.DataFrame):
    """
    Parameters:
    ticker: ticker do ativo.
    setor: nome do setor.
    df_benchmark: df com os preços de fechamento ('Close') do benchmark.

    Return:
    Plot do retorno anual do(s) ativo(s).
    """
    # Download dos preços do ativo
    df_preco = yf.download(ticker, auto_adjust=True)['Close']
    # Resetando o index do df
    df_preco = df_preco.reset_index()
    # Transformando a coluna 'Date' em  index 
    df_preco = df_preco.set_index('Date')

    # Ano atual para calcular o retorno do ano anterior
    ano_atual = datetime.now().year

    # Calculando o retorno anual
    lst_ret_anual = []
    for empresa in df_preco.columns:
        ret_anual = [round(((df_preco.loc[f'{ano}-12', empresa][-1] / df_preco.loc[f'{ano}-01', empresa][0])-1)*100, 2) for ano in range(df_preco.index[0].year+1, ano_atual)]
        lst_ret_anual.append(ret_anual)

    # Lista dos anos negociados p/ se tornar o index do df
    lista_anos_idx = [ano for ano in range(df_preco.index[0].year+1, ano_atual)]

    # Criando o df 
    df_ret_anual = pd.DataFrame(lst_ret_anual).T
    df_ret_anual.columns =df_preco.columns
    df_ret_anual.index = lista_anos_idx

    # Calculando o retorno anual do IFIX
    ret_anual_ifix = [round(((df_benchmark.loc[f'{ano}-12', 'Close'][-1] / df_benchmark.loc[f'{ano}-01', 'Close'][0]) - 1) * 100, 2) for ano in df_benchmark.index.year.unique()[:-1]]
    # Lista dos anos negociados p/ se tornar o index do df
    lista_anos_idx_ifix = [ano for ano in df_benchmark.index.year.unique()[:-1]]
    # Criando o df do benchmark
    df_ret_anual_ifix = pd.DataFrame(ret_anual_ifix, index=lista_anos_idx_ifix)
    # Renomeando a coluna
    df_ret_anual_ifix = df_ret_anual_ifix.rename(columns={0:'IFIX'})
    # Cortando o df do benchmark p/ ficar do mesmo tamanho do df dos FIIs
    df_ret_anual_ifix = df_ret_anual_ifix.loc[df_preco.index[0].year+1:]

    # Concatenando os dfs
    df_ret_anual = pd.concat([df_ret_anual, df_ret_anual_ifix], axis=1)

    # Plotando o retorno anual em um heatmap
    plt.figure(figsize=(20, 10))
    sns.heatmap(df_ret_anual, annot=True, cmap='Blues', fmt=".2f", linewidths=0.8)
    plt.title(f'Retorno Anual - Setor de {setor}')

    return df_ret_anual, plt


def plot_risk_return(ticker: str, setor: str):
    """
    Parameters:
    df_setor: df que contém os preços de fechamento das empresas do setor selecionado.
    setor: nome do setor.
    
    Returns:
    Plot do gráfico da relação risco x retorno do setor selecionado.
    """

    # Df do preço do ativo
    df_preco = yf.download(ticker, auto_adjust=True)['Close']
    # Resetando o index do df
    df_preco = df_preco.reset_index()
    # Transformando a coluna 'Date' em  index 
    df_preco = df_preco.set_index('Date')

    # Calculando o retorno diário
    df_returns = np.log(df_preco / df_preco.shift(1))

    # Listas da média do retorno logarítmico e do desvio-padrão do retorno logarítmico
    lst_ret_mean = []
    lst_ret_std = []

    for empresa in df_returns:
        # Média do retorno logarítmico
        ret_mean = df_returns[empresa].dropna().mean() * 100
        # Desvio-padrão do retorno logarítmico médio
        ret_std = df_returns[empresa].dropna().std() * 100
        lst_ret_mean.append(ret_mean)
        lst_ret_std.append(ret_std)

    # Dataframe da relação risco x retorno
    df_risk_return = pd.DataFrame([lst_ret_mean, lst_ret_std], columns=df_returns.columns, index=['mean', 'std'])

    # Plotando o grafico da relação risco x retorno
    fig = go.Figure()

    for empresa in df_risk_return: 
        fig.add_trace(go.Scatter(
            x=[df_risk_return.loc['mean', empresa]],
            y=[df_risk_return.loc['std', empresa]],
            mode='markers',
            marker=dict(symbol='star', size=10),
            name=empresa
        ))

    # Atualizando o layout
    fig.update_layout(
        title=f'Setor de {setor} - Gráfico risco x retorno',
        xaxis=dict(title='Média Esperada Retorno Diário'),
        yaxis=dict(title='Risco Diário'),
        showlegend=True
    )

    fig.add_hline(y=0, line_width=2, line_color='red')
    fig.add_vline(x=0, line_width=2, line_color='red')

    return fig.show()


def dy_fii(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula o DY médio, máximo e mínimo do FII.

    Parameters:
    df: dataframe do FII que contém a coluna 'Percentual_Dividend_Yield_Mes'.

    Returns:
    df_dy: dataframe que contém os DY médio, máximo e mínimo.
    """

    # Calculando o DY médio, máximo e mínimo
    lst_dy = []

    for empresa in df['Ticker'].unique():
        dy_medio = round(df.loc[df['Ticker'] == empresa, 'Percentual_Dividend_Yield_Mes'].mean(), 2)
        dy_max = round(df.loc[df['Ticker'] == empresa, 'Percentual_Dividend_Yield_Mes'].max(), 2)
        dy_min = round(df.loc[df['Ticker'] == empresa, 'Percentual_Dividend_Yield_Mes'].min(), 2)
        lst_dy.append({
            'dy_medio': dy_medio, 
            'dy_max': dy_max, 
            'dy_min': dy_min
        })

    # Transformando a lista em um df
    df_dy = pd.DataFrame(lst_dy)

    # Renomeando as colunas
    df_dy.index = df['Ticker'].unique()

    return df_dy


def dy_fii_acumm_12m(df: pd.DataFrame) -> pd.Series:
    """
    Calcula o dividend yield acumulado dos últimos 12 meses.

    Parameters:
    df: dataframe do FII que contém a coluna 'Percentual_Dividend_Yield_Mes'.

    Returns:
    dy_acumulado: dividend yield acumulado dos últimos 12 meses.
    """

    # DY acumulado dos últimos 12 meses
    lst_dy_accum = []
    for empresa in df['Ticker'].unique():
        # Calculando a taxa unitária
        taxa_unitaria = 1 + (df.loc[df['Ticker'] == empresa, 'Percentual_Dividend_Yield_Mes'] / 100)
        # Calculando o DY acumulado dos últimos 12 meses
        dy_acumulado = round((taxa_unitaria.rolling(window=12).agg(lambda x: x.prod()) - 1) * 100, 2)
        # Retirando os NaN
        dy_acumulado = dy_acumulado.dropna()
        # Adicionando na lista
        lst_dy_accum.append(dy_acumulado)

    # Criando o df
    df_dy_accum = pd.DataFrame(lst_dy_accum).T

    # Renomeando as colunas
    df_dy_accum.columns = df['Ticker'].unique()

    return df_dy_accum


def consulta_bc(codigo_bcb: str, data_inicial: str, data_final: str):
    """
    Parameters:
    codigo_bcb: código da série do BC.
    data_inicial: data inicial da série - no formato dia/mes/ano -> 01/01/2024.
    data_final: data final da série - no formato dia/mes/ano -> 01/01/2024.

    Returns:
    df: Dataframe da série do BC.
    """
    url = f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_bcb}/dados?formato=json&dataInicial={data_inicial}&dataFinal={data_final}'
    df = pd.read_json(url)
    df['data'] = pd.to_datetime(df['data'], dayfirst=True)
    df.set_index('data', inplace= True)

    return df


def vm_igti(ano: str, mes: str, num_on: int, num_pn: int):
    """
    Calcula o valor de mercado da empresa Iguatemi (IGTI3 e IGTI4).

    Parameters:
    ano: ano do período do ITR/DFP.
    mes: mes do período do ITR/DFP.
    num_on: número de ações ordinárias do período do ITR/DFP.
    num_pn: número de ações preferenciais do período do ITR/DFP.

    Returns:
    igti_vm: valor de mercado da Iguatemi.

    """
    # Selecioando os últimos preços do IGTI
    # ITR 1T -> mes: '03'
    # ITR 2T -> mes: '06'
    # ITR 3T -> mes: '09'
    # DFP 4T -> mes: '12'

    # Lista com os tickers ON e PN da Iguatemi
    lst_iguatemi = ['IGTI3.SA', 'IGTI4.SA']
    # Df do preço do ativo
    preco_iguatemi = yf.download(lst_iguatemi, auto_adjust=True)['Close']
    # Resetando o index do df
    preco_iguatemi = preco_iguatemi.reset_index()
    # Transformando a coluna 'Date' em  index 
    preco_iguatemi = preco_iguatemi.set_index('Date')
    # Selecionando pelo ano e mês
    preco_iguatemi = preco_iguatemi.loc[f'{ano}-{mes}']
    # Selecionando o último dado
    preco_iguatemi = preco_iguatemi.iloc[-1]

    # Número de ações do IGTI
    dict_num_acoes_igti = {
        'on': num_on,
        'pn': num_pn
    }

    # Valor de mercado do IGTI
    igti3_vm = preco_iguatemi['IGTI3.SA'] * dict_num_acoes_igti['on']
    igti4_vm = preco_iguatemi['IGTI4.SA'] * dict_num_acoes_igti['pn']
    igti_vm = igti3_vm + igti4_vm

    return igti_vm


def indicadores_acoes_shoppings(ano: str, mes: str, dict_shoppings: dict, ticker: str, vm_igti):
    """
    Parameters:
    ano: ano do período do ITR/DFP.
    mes: mes do período do ITR/DFP.
    dict_shoppings: dicionário que contém os dados das empresas de shoppings.
    ticker: tickers das empreas de shoppings.
    vm_igti: função que calcula o valor de mercado da Iguatemi.

    Returns:
    df_acoes_shoppings: df que contém o indicador P/VP das empresas de shoppings.
    """

    # Selecionando os últimos preços das ações de shoppings
    # ITR 1T -> mes: '03'
    # ITR 2T -> mes: '06'
    # ITR 3T -> mes: '09'
    # DFP 4T -> mes: '12'

    # Df do preço do ativo
    preco_shopping = yf.download(ticker, auto_adjust=True)['Close']
    # Resetando o index do df
    preco_shopping = preco_shopping.reset_index()
    # Transformando a coluna 'Date' em  index 
    preco_shopping = preco_shopping.set_index('Date')

    # Selecionando pelo ano e mês
    preco_shopping = preco_shopping.loc[f'{ano}-{mes}']
    # Selecionando o último dado
    ultimo_preco_shopping = preco_shopping.iloc[-1]
    # Renomeando a coluna para 'preco'
    ultimo_preco_shopping = ultimo_preco_shopping.rename('preco')

    # Transformando em um df
    df_acoes_shoppings = pd.DataFrame(dict_shoppings, index=ticker)

    # Concatenando os dfs
    df_acoes_shoppings = pd.concat([df_acoes_shoppings, ultimo_preco_shopping], axis=1)

    # Calculando a diferença entre o 'valor_justo' e 'propriedades_investimentos'
    df_acoes_shoppings['diferenca'] = df_acoes_shoppings['valor_justo'] - df_acoes_shoppings['propriedades_investimento']
    # Calculando o 'pl_ajustado'
    df_acoes_shoppings['pl_ajustado'] = df_acoes_shoppings['pl_contabil'] + df_acoes_shoppings['diferenca']
    # Calculando o 'vpa'
    df_acoes_shoppings['vpa'] = round(df_acoes_shoppings['pl_ajustado'] / df_acoes_shoppings['num_acoes'], 2)
    # Calculando o 'p/vp'
    df_acoes_shoppings['p/vp'] = round(df_acoes_shoppings['preco'] / df_acoes_shoppings['vpa'], 2)
    # A unit da Iguatemi é formada por 3 ações (1 Unit = 1 ação ON + 2 ações PN)
    df_acoes_shoppings.loc['IGTI11.SA', 'p/vp'] = df_acoes_shoppings.loc['IGTI11.SA', 'p/vp']/3
    # Calculando o 'valor_mercado'
    df_acoes_shoppings['valor_mercado'] = df_acoes_shoppings['num_acoes'] * df_acoes_shoppings['preco']
    # Calculando o 'valor_mercado' do IGTI com os valores de ON e PN
    vm_igti_2T24 = vm_igti
    # Substituindo o 'valor_mercado' do IGTI11 pelo correto
    df_acoes_shoppings.loc['IGTI11.SA', 'valor_mercado'] = vm_igti_2T24
    # Calculando o 'enterprise_value'
    df_acoes_shoppings['enterprise_value'] = df_acoes_shoppings['valor_mercado'] + df_acoes_shoppings['divida_liquida'] 
    # Calculando 'ev/abl' (EV/m²)
    df_acoes_shoppings['ev/abl'] =  round(df_acoes_shoppings['enterprise_value'] / df_acoes_shoppings['abl_propria'], 2)

    return df_acoes_shoppings


def valor_absoluto_grafico_pizza(val: float, contagem: pd.Series) -> str:
    """
    Mostra os valores absolutos de uma contagem específica para ser plotado no gráfico de pizza.

    Parameters:
    val: o valor percentual a ser convertido em um valor absoluto.
    contagem: a série de contagem que será usada como base para o cálculo.

    Returns:
    str: o valor absoluto como uma string, adequado para ser exibido em gráficos.
    """
    a = int(val / 100. * contagem.sum())
    return f'{a}'
