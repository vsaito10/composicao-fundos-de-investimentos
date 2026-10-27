"""
Interface Streamlit para analisar os fundos Verde, Dynamo, IP, Squadra e Guepardo.

Para rodar:
    streamlit run app_fundos.py
"""
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import dados_fundos as dados

st.set_page_config(page_title='Fundos de Ações', page_icon='📊', layout='wide')

# Cores pensadas para o fundo escuro (tema em .streamlit/config.toml)
CORES = {
    'Verde': '#2a9d8f', 'Dynamo': '#e76f51', 'IP': '#4cc9f0', 'Squadra': '#e9c46a', 'Guepardo': '#b07cd8',
    'BOVA11': '#9e9e9e', 'CDI': '#d0d0d0',
}
CNPJ_PARA_NOME = {cnpj: nome for nome, cnpj in dados.FUNDOS.items()}
# O st.cache_data só percebe mudanças no corpo da função cacheada, não em dados_fundos.py. Passar a data de
# modificação do arquivo como argumento invalida o cache quando ele muda (ex.: coluna nova nas carteiras).
VERSAO_DADOS = Path(dados.__file__).stat().st_mtime


# ---------------------------------------------------------------------------------------------------------------------
# Dados (cache)
# ---------------------------------------------------------------------------------------------------------------------
@st.cache_data(show_spinner='Lendo as cotas diárias...')
def cotas(versao: float) -> pd.DataFrame:
    df = dados.carregar_cotas(list(dados.FUNDOS.values()))
    df['fundo'] = df['cnpj'].map(CNPJ_PARA_NOME)
    return df


@st.cache_data(show_spinner='Lendo BOVA11 e CDI...')
def benchmarks(versao: float) -> pd.DataFrame:
    return dados.carregar_benchmarks()


@st.cache_data(show_spinner='Lendo as carteiras (CDA)...')
def carteiras(versao: float) -> pd.DataFrame:
    df = dados.carregar_carteiras(list(dados.FUNDOS.values()))
    df['fundo'] = df['cnpj'].map(CNPJ_PARA_NOME)
    return df


@st.cache_data(show_spinner='Lendo as cotações das ações...')
def retornos_diarios_acoes(versao: float) -> pd.DataFrame:
    return dados.carregar_retornos_diarios(dados.tickers_atribuicao(dados.posicoes_por_ativo(carteiras(versao))))


@st.cache_data(show_spinner='Calculando a atribuição...')
def atribuicao(fundo: str, versao: float):
    df = carteiras(versao)
    pos = dados.posicoes_por_ativo(df[df['fundo'] == fundo])
    return dados.atribuicao_mensal(pos, serie_cota(fundo), retornos_diarios_acoes(versao), benchmarks(versao)['CDI'])


def serie_cota(fundo: str) -> pd.Series:
    df = cotas(VERSAO_DADOS)
    return df[df['fundo'] == fundo].set_index('data')['cota'].dropna()


def fmt_pct(x: float, casas: int = 2) -> str:
    return '-' if pd.isna(x) else f'{x:.{casas}%}'


def fmt_num(x: float) -> str:
    return '-' if pd.isna(x) else f'{x:.2f}'


def fmt_mi(x: float) -> str:
    if pd.isna(x):
        return '-'
    return f'R$ {x / 1e6:,.1f} mi'.replace(',', 'X').replace('.', ',').replace('X', '.')


# ---------------------------------------------------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------------------------------------------------
st.sidebar.title('Fundos de Ações')
fundo = st.sidebar.radio('Fundo', list(dados.FUNDOS))
st.sidebar.caption(f'CNPJ: {dados.FUNDOS[fundo]}')

df_cotas = cotas(VERSAO_DADOS)
df_bench = benchmarks(VERSAO_DADOS)
df_carteiras = carteiras(VERSAO_DADOS)

data_min_cota, data_max_cota = df_cotas['data'].min().date(), df_cotas['data'].max().date()
periodo = st.sidebar.date_input(
    'Período (rentabilidade)', value=(data_min_cota, data_max_cota),
    min_value=data_min_cota, max_value=data_max_cota, format='DD/MM/YYYY',
)
inicio, fim = (pd.Timestamp(periodo[0]), pd.Timestamp(periodo[1])) if len(periodo) == 2 else (pd.Timestamp(data_min_cota), pd.Timestamp(data_max_cota))

st.sidebar.divider()
st.sidebar.caption(
    f"Cotas: {data_min_cota:%d/%m/%Y} a {data_max_cota:%d/%m/%Y}  \n"
    f"Carteiras (CDA): até {df_carteiras['data'].max():%m/%Y}  \n"
    'Fonte: CVM (inf_diario e CDA), B3 (COTAHIST) e BCB (CDI). BOVA11 é usado como proxy do Ibovespa.'
)

aba_geral, aba_rent, aba_atrib, aba_carteira, aba_mov = st.tabs(
    ['Visão geral', 'Rentabilidade', 'Atribuição', 'Carteira', 'Movimentações']
)

# ---------------------------------------------------------------------------------------------------------------------
# Visão geral - comparação dos 5 fundos
# ---------------------------------------------------------------------------------------------------------------------
with aba_geral:
    st.subheader(f'Comparação entre os fundos ({inicio:%d/%m/%Y} a {fim:%d/%m/%Y})')

    fig = go.Figure()
    linhas_metricas = []
    retornos_diarios = {}
    for nome in dados.FUNDOS:
        cota = serie_cota(nome).loc[inicio:fim]
        if len(cota) < 2:
            continue
        fig.add_trace(go.Scatter(x=cota.index, y=cota / cota.iloc[0] - 1, name=nome, line=dict(color=CORES[nome])))
        m = dados.metricas(cota, df_bench)
        mb = dados.metricas_benchmark(cota, df_bench)
        retornos_diarios[nome] = cota.pct_change()

        # PL e carteira na data final do período escolhido (não os últimos disponíveis)
        pl = df_cotas[(df_cotas['fundo'] == nome) & (df_cotas['data'] <= fim)].set_index('data')['pl'].dropna()
        carteira = df_carteiras[(df_carteiras['fundo'] == nome) & (df_carteiras['data'] <= fim)]
        ultima = carteira[carteira['data'] == carteira['data'].max()]
        linhas_metricas.append({
            'Fundo': nome,
            'PL no fim do período': pl.iloc[-1] if len(pl) else float('nan'),
            'Retorno': m['Retorno no período'],
            'Retorno anualizado': m['Retorno anualizado'],
            'Volatilidade': m['Volatilidade anual'],
            'Sharpe': m['Sharpe'],
            'Sortino': mb['Sortino'],
            'Drawdown máx.': m['Drawdown máximo'],
            'Beta': m['Beta vs BOVA11'],
            'Alfa anual': mb['Alfa anual'],
            'Information ratio': mb['Information ratio'],
            'Ações líquido (% PL)': ultima.loc[ultima['classe'].str.startswith('Ações'), 'peso'].sum(),
        })

    bench = df_bench.loc[inicio:fim]
    for col in ['BOVA11', 'CDI']:
        fig.add_trace(go.Scatter(x=bench.index, y=bench[col] / bench[col].iloc[0] - 1, name=col,
                                 line=dict(color=CORES[col], dash='dash')))
    fig.update_layout(yaxis_tickformat='.0%', hovermode='x unified', height=450, legend=dict(orientation='h', y=1.1),
                      yaxis_title='Retorno acumulado')
    st.plotly_chart(fig, width="stretch")

    if not linhas_metricas:
        st.warning('Nenhum fundo tem cotas suficientes no período selecionado.')
    else:
        df_metricas = pd.DataFrame(linhas_metricas).set_index('Fundo')
        st.dataframe(
            df_metricas.style.format({
                'PL no fim do período': fmt_mi, 'Retorno': fmt_pct, 'Retorno anualizado': fmt_pct, 'Volatilidade': fmt_pct,
                'Sharpe': fmt_num, 'Sortino': fmt_num, 'Drawdown máx.': fmt_pct, 'Beta': fmt_num, 'Alfa anual': fmt_pct,
                'Information ratio': fmt_num, 'Ações líquido (% PL)': fmt_pct,
            }),
            width="stretch",
        )
    st.caption('Sharpe e Sortino calculados contra o CDI; alfa e information ratio contra o BOVA11. '
               'PL na última cota até o fim do período. '
               '"Ações líquido" = ações compradas - vendidas no último CDA até o fim do período.')

    # Retornos em janelas padrão (referência: fim do período)
    st.subheader(f'Retornos em janelas padrão (até {fim:%d/%m/%Y})')
    inicio_cotas = df_cotas['data'].min()
    janelas = pd.DataFrame(
        {nome: dados.retornos_janelas(serie_cota(nome), fim) for nome in dados.FUNDOS}
        | {col: dados.retornos_janelas(df_bench.loc[inicio_cotas:, col], fim) for col in ['BOVA11', 'CDI']}
    ).T
    st.dataframe(
        janelas.style.format(fmt_pct)
        .background_gradient(cmap='RdYlGn', axis=0,
                             subset=pd.IndexSlice[list(dados.FUNDOS), janelas.columns[janelas.notna().any()]]),
        width="stretch",
    )
    st.caption(f'As janelas ignoram a data inicial do período. "Desde o início" começa em {inicio_cotas:%d/%m/%Y}, '
               'primeira cota da base. Cores comparam os fundos dentro de cada janela.')

    # Correlação entre os fundos
    st.subheader('Correlação entre os fundos (retornos diários no período)')
    df_ret = pd.DataFrame(retornos_diarios)
    df_ret['BOVA11'] = df_bench['BOVA11'].pct_change().reindex(df_ret.index)
    corr = df_ret.dropna().corr()
    if len(df_ret.dropna()) < 20:
        st.info('Período curto demais para calcular as correlações.')
    else:
        fig = px.imshow(corr, text_auto='.2f', zmin=0, zmax=1, color_continuous_scale='Blues', height=420, aspect='auto')
        fig.update_layout(xaxis_title=None, yaxis_title=None, coloraxis_showscale=False)
        st.plotly_chart(fig, width="stretch")
    st.caption('Correlação alta entre dois fundos = pouca diversificação ao combiná-los. '
               'Com retornos diários, diferenças no horário de marcação podem reduzir um pouco as correlações.')

    # Ações em comum
    st.subheader('Ações em comum (último CDA de cada fundo)')
    acoes = df_carteiras[df_carteiras['classe'] == 'Ações (comprado)']
    acoes = acoes[acoes['data'] == acoes.groupby('fundo')['data'].transform('max')]
    df_comum = acoes.pivot_table(index='ativo', columns='fundo', values='peso', aggfunc='sum')
    df_comum = df_comum[df_comum.notna().sum(axis=1) >= 2]
    df_comum = (
        df_comum.assign(n_fundos=df_comum.notna().sum(axis=1), _soma=df_comum.sum(axis=1))
        .sort_values(['n_fundos', '_soma'], ascending=False)
        .drop(columns='_soma')
    )
    if df_comum.empty:
        st.info('Nenhuma ação em comum entre os fundos.')
    else:
        st.dataframe(
            df_comum.style.format(fmt_pct, subset=[c for c in df_comum.columns if c != 'n_fundos'])
            .background_gradient(cmap='Greens', subset=[c for c in df_comum.columns if c != 'n_fundos'], axis=None),
            width="stretch",
        )

# ---------------------------------------------------------------------------------------------------------------------
# Rentabilidade do fundo selecionado
# ---------------------------------------------------------------------------------------------------------------------
with aba_rent:
    cota = serie_cota(fundo).loc[inicio:fim]
    if len(cota) < 2:
        st.warning('Sem cotas no período selecionado.')
    else:
        st.subheader(f'{fundo} - rentabilidade ({inicio:%d/%m/%Y} a {fim:%d/%m/%Y})')
        m = dados.metricas(cota, df_bench)
        cols = st.columns(5)
        cols[0].metric('Retorno', fmt_pct(m['Retorno no período']),
                       f"{m['Retorno no período'] - m['BOVA11 no período']:+.2%} vs BOVA11")
        cols[1].metric('Retorno anualizado', fmt_pct(m['Retorno anualizado']),
                       f"{m['Retorno no período'] - m['CDI no período']:+.2%} vs CDI (período)")
        cols[2].metric('Volatilidade anual', fmt_pct(m['Volatilidade anual']))
        cols[3].metric('Sharpe', f"{m['Sharpe']:.2f}")
        cols[4].metric('Drawdown máximo', fmt_pct(m['Drawdown máximo']))
        cols = st.columns(5)
        cols[0].metric('Beta vs BOVA11', f"{m['Beta vs BOVA11']:.2f}")
        cols[1].metric('Correlação vs BOVA11', f"{m['Correlação vs BOVA11']:.2f}")
        cols[2].metric('% meses acima do CDI', fmt_pct(m['% meses acima do CDI']))
        cols[3].metric('% meses abaixo do CDI', fmt_pct(m['% meses abaixo do CDI']))

        # Métricas contra o benchmark e risco de cauda
        mb = dados.metricas_benchmark(cota, df_bench)
        st.markdown('**Contra o BOVA11**')
        cols = st.columns(5)
        cols[0].metric('Alfa anual', fmt_pct(mb['Alfa anual']),
                       help='Alfa de Jensen: retorno acima do CDI não explicado pelo beta ao BOVA11 (anualizado).')
        cols[1].metric('Tracking error', fmt_pct(mb['Tracking error']),
                       help='Volatilidade anual da diferença diária entre o fundo e o BOVA11.')
        cols[2].metric('Information ratio', fmt_num(mb['Information ratio']),
                       help='Retorno ativo anualizado (fundo - BOVA11) / tracking error.')
        cols[3].metric('Captura de alta', fmt_pct(mb['Captura de alta'], 0),
                       help='Nos meses de alta do BOVA11: retorno médio (geométrico) do fundo / do BOVA11. '
                            'Acima de 100% = sobe mais que o índice.')
        cols[4].metric('Captura de baixa', fmt_pct(mb['Captura de baixa'], 0),
                       help='Nos meses de queda do BOVA11: retorno médio (geométrico) do fundo / do BOVA11. '
                            'Abaixo de 100% = cai menos que o índice.')
        st.markdown('**Risco de cauda**')
        cols = st.columns(5)
        cols[0].metric('Sortino', fmt_num(mb['Sortino']),
                       help='Como o Sharpe, mas só a volatilidade dos dias abaixo do CDI entra no denominador.')
        cols[1].metric('Pior mês', fmt_pct(mb['Pior mês']))
        cols[2].metric('Melhor mês', fmt_pct(mb['Melhor mês']))
        cols[3].metric('VaR 95% (1 dia)', fmt_pct(mb['VaR 95% (1 dia)']),
                       help='Histórico: em 95% dos dias a perda foi menor que isso.')
        cols[4].metric('CVaR 95% (1 dia)', fmt_pct(mb['CVaR 95% (1 dia)']),
                       help='Perda média nos 5% piores dias.')

        # Retornos em janelas padrão (referência: fim do período)
        st.markdown(f'**Retornos em janelas padrão (até {cota.index[-1]:%d/%m/%Y})**')
        cota_completa = serie_cota(fundo)
        bench_desde_inicio = df_bench.loc[cota_completa.index[0]:]
        janelas = pd.DataFrame({
            fundo: dados.retornos_janelas(cota_completa, fim),
            'BOVA11': dados.retornos_janelas(bench_desde_inicio['BOVA11'], fim),
            'CDI': dados.retornos_janelas(bench_desde_inicio['CDI'], fim),
        }).T
        janelas.loc['Fundo - BOVA11 (p.p.)'] = janelas.loc[fundo] - janelas.loc['BOVA11']
        janelas.loc['Fundo / CDI'] = janelas.loc[fundo] / janelas.loc['CDI']
        st.dataframe(
            janelas.style.format(fmt_pct).format(lambda x: fmt_pct(x, 0), subset=pd.IndexSlice[['Fundo / CDI'], :]),
            width="stretch",
        )
        st.caption('As janelas terminam no fim do período escolhido e ignoram a data inicial. "Desde o início" começa na '
                   'primeira cota disponível do fundo. Janelas sem cota no início ficam em branco.')

        # Retorno acumulado e drawdown
        bench = df_bench.reindex(cota.index).ffill()
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=cota.index, y=cota / cota.iloc[0] - 1, name=fundo, line=dict(color=CORES[fundo])))
        for col in ['BOVA11', 'CDI']:
            fig.add_trace(go.Scatter(x=bench.index, y=bench[col] / bench[col].iloc[0] - 1, name=col,
                                     line=dict(color=CORES[col], dash='dash')))
        fig.update_layout(title='Retorno acumulado', yaxis_tickformat='.0%', hovermode='x unified', height=400)
        st.plotly_chart(fig, width="stretch")

        col_dd, col_beta = st.columns(2)
        dd = dados.drawdown(cota)
        dd_bova = dados.drawdown(bench['BOVA11'])
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=dd.index, y=dd, name=fundo, fill='tozeroy', line=dict(color=CORES[fundo])))
        fig.add_trace(go.Scatter(x=dd_bova.index, y=dd_bova, name='BOVA11', line=dict(color=CORES['BOVA11'], dash='dash')))
        fig.update_layout(title='Drawdown', yaxis_tickformat='.0%', hovermode='x unified', height=350)
        col_dd.plotly_chart(fig, width="stretch")

        beta = dados.beta_rolante(cota, df_bench['BOVA11'])
        fig = px.line(beta, title='Beta rolante (63 pregões) vs BOVA11', height=350)
        fig.update_traces(line_color=CORES[fundo])
        fig.update_layout(showlegend=False, yaxis_title='Beta', xaxis_title=None)
        col_beta.plotly_chart(fig, width="stretch")

        # Janelas móveis de 12 meses: usa a série completa para a janela já começar cheia no início do período
        moveis = dados.janelas_moveis(serie_cota(fundo), df_bench['BOVA11']).loc[inicio:fim]
        if moveis.empty:
            st.info('Janelas móveis de 12 meses precisam de pelo menos 252 pregões de histórico.')
        else:
            col_ret12, col_vol12 = st.columns(2)
            for col, tipo in [(col_ret12, 'Retorno'), (col_vol12, 'Volatilidade')]:
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=moveis.index, y=moveis[f'{tipo} fundo'], name=fundo,
                                         line=dict(color=CORES[fundo])))
                fig.add_trace(go.Scatter(x=moveis.index, y=moveis[f'{tipo} benchmark'], name='BOVA11',
                                         line=dict(color=CORES['BOVA11'], dash='dash')))
                fig.update_layout(title=f'{tipo} em 12 meses (janela móvel)', yaxis_tickformat='.0%',
                                  hovermode='x unified', height=350, legend=dict(orientation='h', y=1.12))
                col.plotly_chart(fig, width="stretch")

        # Tabela mensal
        st.markdown('**Rentabilidade mensal**')
        tabela = dados.tabela_rentabilidade(cota, df_bench)
        st.dataframe(
            tabela.style.format(fmt_pct).background_gradient(cmap='RdYlGn', vmin=-0.1, vmax=0.1,
                                                             subset=[c for c in tabela.columns
                                                                     if 'no ano' not in c and c != 'Acumulado']),
            width="stretch",
        )
        st.caption('Mensal: última cota do mês vs. última cota do mês anterior. O primeiro mês conta a partir da data inicial do período. '
                   '"Acumulado": retorno composto do fundo desde o início do período até o fim de cada ano.')

        # Captação líquida e PL
        fluxo = df_cotas[(df_cotas['fundo'] == fundo) & df_cotas['data'].between(inicio, fim)].set_index('data')
        fluxo_mensal = (fluxo['captacao'] - fluxo['resgate']).groupby(fluxo.index.to_period('M')).sum()
        fig = go.Figure()
        fig.add_trace(go.Bar(x=fluxo_mensal.index.to_timestamp(), y=fluxo_mensal / 1e6, name='Captação líquida (R$ mi)',
                             marker_color=['#2a9d8f' if v >= 0 else '#e76f51' for v in fluxo_mensal]))
        fig.add_trace(go.Scatter(x=fluxo.index, y=fluxo['pl'] / 1e6, name='PL (R$ mi)', yaxis='y2',
                                 line=dict(color=CORES['BOVA11'])))
        fig.update_layout(title='Captação líquida mensal e PL', height=380, hovermode='x unified',
                          yaxis=dict(title='Captação líquida (R$ mi)'),
                          yaxis2=dict(title='PL (R$ mi)', overlaying='y', side='right'),
                          legend=dict(orientation='h', y=1.12))
        st.plotly_chart(fig, width="stretch")
        st.caption('Os CNPJs são de fundos master: a captação reflete as aplicações e resgates dos fundos feeder.')

# ---------------------------------------------------------------------------------------------------------------------
# Atribuição de desempenho do fundo selecionado
# ---------------------------------------------------------------------------------------------------------------------
with aba_atrib:
    df_resumo, df_detalhe = atribuicao(fundo, VERSAO_DADOS)
    meses_periodo = pd.period_range(inicio, fim, freq='M')
    df_resumo = df_resumo[df_resumo.index.isin(meses_periodo)]
    df_detalhe = df_detalhe[df_detalhe['mes'].isin(df_resumo.index)]

    if df_resumo.empty:
        st.warning('Sem meses com carteira (CDA) e cota no período selecionado.')
    else:
        st.subheader(
            f"{fundo} - atribuição de desempenho "
            f"({df_resumo.index.min().strftime('%m/%Y')} a {df_resumo.index.max().strftime('%m/%Y')})"
        )
        st.caption(
            'Para cada mês, usa a carteira do fim do mês anterior: contribuição = peso no início do mês x retorno do ativo no mês. '
            'Caixa e renda fixa rendem o CDI. O **resíduo** é o retorno da cota menos a soma das contribuições.'
        )

        cols = st.columns(5)
        cols[0].metric('Retorno da cota', fmt_pct(df_resumo['Retorno da cota'].sum()),
                       f"composto: {(1 + df_resumo['Retorno da cota']).prod() - 1:.2%}", delta_color='off',
                       help='Soma dos retornos mensais, para fechar com as contribuições '
                            '(cota = ações compradas + vendidas + CDI + resíduo). Abaixo, o retorno composto.')
        cols[1].metric('Ações compradas', fmt_pct(df_resumo['Ações compradas'].sum()), help='Soma das contribuições mensais.')
        cols[2].metric('Ações vendidas', fmt_pct(df_resumo['Ações vendidas'].sum()), help='Soma das contribuições mensais.')
        cols[3].metric('Resíduo (return gap)', fmt_pct(df_resumo['Resíduo (return gap)'].sum()),
                       f"{df_resumo['Resíduo (return gap)'].mean():+.2%} por mês", delta_color='off',
                       help='Soma dos resíduos mensais.')
        cols[4].metric('Não precificado (média)', fmt_pct(df_resumo['Não precificado (% PL)'].mean()),
                       help='Peso médio de exterior, opções, futuros, cotas de fundos e ações sem preço no mês.')

        with st.expander('O que entra no resíduo?'):
            st.markdown(
                '- **Operações feitas dentro do mês** (o cálculo supõe a carteira parada desde o fim do mês anterior);\n'
                '- **Posições não precificadas**: investimento no exterior, opções, futuros, cotas de fundos que não são ETF/renda fixa '
                'e ações que trocaram de ticker no mês;\n'
                '- **Dividendos e JCP**: o COTAHIST não é ajustado por proventos, então eles aparecem aqui (tipicamente ~0,3-0,5% ao mês '
                'para uma carteira de ações);\n'
                '- **Posições confidenciais** do CDA, taxas e custos.\n\n'
                'Um resíduo positivo e persistente, acima do que os dividendos explicam, sugere ganho com operações dentro do mês. '
                'Em fundos com muito peso não precificado (ex.: IP, com bastante exterior), o resíduo mistura principalmente o retorno dessas posições.'
            )

        # Decomposição mensal
        cores_grupos = {'Ações compradas': CORES[fundo], 'Ações vendidas': '#e76f51',
                        'Caixa e renda fixa (CDI)': CORES['CDI'], 'Resíduo (return gap)': '#f4a261'}
        x_meses = df_resumo.index.to_timestamp()
        fig = go.Figure()
        for grupo, cor in cores_grupos.items():
            fig.add_trace(go.Bar(x=x_meses, y=df_resumo[grupo], name=grupo, marker_color=cor))
        fig.add_trace(go.Scatter(x=x_meses, y=df_resumo['Retorno da cota'], name='Retorno da cota', mode='lines+markers',
                                 line=dict(color='#f5f5f5')))
        fig.update_layout(title='Decomposição do retorno mensal', barmode='relative', yaxis_tickformat='.1%',
                          hovermode='x unified', height=430, legend=dict(orientation='h', y=1.12))
        st.plotly_chart(fig, width="stretch")

        col_contrib, col_gap = st.columns([3, 2])

        # Maiores contribuições no período
        contrib_ativo = (
            df_detalhe[df_detalhe['grupo'].isin(['Ações compradas', 'Ações vendidas'])]
            .groupby(['ativo', 'grupo'], as_index=False)
            .agg(contribuicao=('contribuicao', 'sum'), meses=('mes', 'nunique'), peso_medio=('peso', 'mean'))
        )
        n_top = 10
        destaques = pd.concat([contrib_ativo.nlargest(n_top, 'contribuicao'), contrib_ativo.nsmallest(n_top, 'contribuicao')])
        destaques = destaques.drop_duplicates().sort_values('contribuicao')
        destaques['rotulo'] = destaques['ativo'] + destaques['grupo'].map({'Ações compradas': '', 'Ações vendidas': ' (short)'})
        fig = px.bar(destaques, x='contribuicao', y='rotulo', orientation='h', height=520,
                     title=f'{n_top} maiores e {n_top} menores contribuições no período',
                     color=destaques['contribuicao'] > 0, color_discrete_map={True: '#2a9d8f', False: '#e76f51'},
                     hover_data={'meses': True, 'peso_medio': ':.2%', 'rotulo': False})
        fig.update_layout(showlegend=False, xaxis_tickformat='.1%', xaxis_title='Soma das contribuições mensais', yaxis_title=None)
        col_contrib.plotly_chart(fig, width="stretch")

        # Resíduo acumulado
        gap_acum = df_resumo['Resíduo (return gap)'].cumsum()
        fig = go.Figure(go.Scatter(x=x_meses, y=gap_acum, fill='tozeroy', line=dict(color='#f4a261'), name='Resíduo acumulado'))
        fig.update_layout(title='Resíduo acumulado (soma dos meses)', yaxis_tickformat='.1%', height=520, hovermode='x unified')
        col_gap.plotly_chart(fig, width="stretch")

        st.markdown('**Resumo mensal**')
        st.dataframe(
            df_resumo.sort_index(ascending=False).set_axis(df_resumo.sort_index(ascending=False).index.strftime('%m/%Y'))
            .style.format(fmt_pct)
            .background_gradient(cmap='RdYlGn', vmin=-0.03, vmax=0.03, subset=['Resíduo (return gap)']),
            width="stretch",
        )

        # Detalhe de um mês
        st.subheader('Detalhe do mês')
        meses_ordenados = [p.strftime('%m/%Y') for p in sorted(df_resumo.index, reverse=True)]
        mes_sel = pd.Period(pd.to_datetime(st.selectbox('Mês', meses_ordenados, key=f'atrib_mes_{fundo}'), format='%m/%Y'), freq='M')
        linha = df_resumo.loc[mes_sel]
        st.caption(
            f"Carteira de {(mes_sel - 1).strftime('%m/%Y')} | Retorno da cota: {fmt_pct(linha['Retorno da cota'])} | "
            f"Ações: {fmt_pct(linha['Ações compradas'] + linha['Ações vendidas'])} | "
            f"CDI: {fmt_pct(linha['Caixa e renda fixa (CDI)'])} | Resíduo: {fmt_pct(linha['Resíduo (return gap)'])}"
        )

        det_mes = df_detalhe[df_detalhe['mes'] == mes_sel]
        acoes_mes = det_mes[det_mes['grupo'].isin(['Ações compradas', 'Ações vendidas'])].sort_values('contribuicao')
        if not acoes_mes.empty:
            fig = px.bar(acoes_mes, x='contribuicao', y='ativo', orientation='h', height=max(350, 18 * len(acoes_mes)),
                         color=acoes_mes['contribuicao'] > 0, color_discrete_map={True: '#2a9d8f', False: '#e76f51'},
                         hover_data={'peso': ':.2%', 'retorno': ':.2%'}, title=f"Contribuição por ação em {mes_sel.strftime('%m/%Y')}")
            fig.update_layout(showlegend=False, xaxis_tickformat='.2%', xaxis_title=None, yaxis_title=None)
            st.plotly_chart(fig, width="stretch")

        st.dataframe(
            det_mes.sort_values('contribuicao', ascending=False, na_position='last')
            [['grupo', 'classe', 'ativo', 'descricao', 'peso', 'retorno', 'contribuicao']]
            .rename(columns={'grupo': 'Grupo', 'classe': 'Classe', 'ativo': 'Ativo', 'descricao': 'Descrição',
                             'peso': 'Peso no início', 'retorno': 'Retorno no mês', 'contribuicao': 'Contribuição'})
            .style.format({'Peso no início': fmt_pct, 'Retorno no mês': fmt_pct, 'Contribuição': fmt_pct}),
            width="stretch", hide_index=True,
        )

# ---------------------------------------------------------------------------------------------------------------------
# Carteira do fundo selecionado
# ---------------------------------------------------------------------------------------------------------------------
with aba_carteira:
    carteira = df_carteiras[df_carteiras['fundo'] == fundo]
    pos = dados.posicoes_por_ativo(carteira)
    datas_cda = sorted(pos['data'].unique(), reverse=True)

    data_cda = st.selectbox('Data da carteira (CDA)', datas_cda, format_func=lambda d: f'{pd.Timestamp(d):%m/%Y}')
    pos_data = pos[pos['data'] == data_cda]
    pl = carteira.loc[carteira['data'] == data_cda, 'pl'].iloc[0]

    acoes_long = pos_data[pos_data['classe'] == 'Ações (comprado)'].sort_values('peso', ascending=False)
    pesos_norm = acoes_long['peso'] / acoes_long['peso'].sum()
    cols = st.columns(5)
    cols[0].metric('PL', fmt_mi(pl))
    cols[1].metric('Ações comprado', fmt_pct(acoes_long['peso'].sum()))
    cols[2].metric('Nº de ações', len(acoes_long))
    cols[3].metric('Top 10 (% PL)', fmt_pct(acoes_long['peso'].head(10).sum()))
    cols[4].metric('Nº efetivo de ações', f'{1 / (pesos_norm ** 2).sum():.1f}' if len(acoes_long) else '-',
                   help='1 / soma dos pesos² (HHI). Indica quantas ações com peso igual teriam a mesma concentração.')

    col_classe, col_top = st.columns([1, 2])
    por_classe = pos_data.groupby('classe')['peso'].sum().sort_values()
    fig = px.bar(por_classe, orientation='h', title='Alocação por classe (% PL)', height=400)
    fig.update_layout(showlegend=False, xaxis_tickformat='.0%', xaxis_title=None, yaxis_title=None)
    col_classe.plotly_chart(fig, width="stretch")

    top = acoes_long.head(15).sort_values('peso')
    fig = px.bar(top, x='peso', y='ativo', orientation='h', title='Maiores posições em ações (% PL)', height=400,
                 color_discrete_sequence=[CORES[fundo]])
    fig.update_layout(xaxis_tickformat='.1%', xaxis_title=None, yaxis_title=None)
    col_top.plotly_chart(fig, width="stretch")

    n_confid = carteira.loc[carteira['data'] == data_cda, 'confidencial'].sum()
    if n_confid:
        st.caption(f'{n_confid} posições desta carteira estão marcadas como confidenciais no CDA.')

    classes = sorted(pos_data['classe'].unique())
    filtro_classes = st.multiselect('Classes', classes, default=[c for c in classes if c.startswith('Ações')])
    tabela = pos_data[pos_data['classe'].isin(filtro_classes)].sort_values('peso', ascending=False)
    st.dataframe(
        tabela[['classe', 'ativo', 'descricao', 'valor', 'peso', 'valor_compras', 'valor_vendas']]
        .rename(columns={'classe': 'Classe', 'ativo': 'Ativo', 'descricao': 'Descrição', 'valor': 'Valor',
                         'peso': '% PL', 'valor_compras': 'Compras no mês', 'valor_vendas': 'Vendas no mês'})
        .style.format({'Valor': fmt_mi, '% PL': fmt_pct, 'Compras no mês': fmt_mi, 'Vendas no mês': fmt_mi}),
        width="stretch", hide_index=True,
    )

    # Histórico da alocação por classe
    hist_classe = pos.groupby(['data', 'classe'])['peso'].sum().reset_index()
    fig = px.bar(hist_classe, x='data', y='peso', color='classe', title='Histórico da alocação por classe (% PL)', height=450)
    fig.update_layout(yaxis_tickformat='.0%', xaxis_title=None, yaxis_title=None, barmode='relative',
                      legend=dict(orientation='h', y=-0.15))
    st.plotly_chart(fig, width="stretch")

    hist_acoes = pos[pos['classe'] == 'Ações (comprado)'].groupby('data').agg(n_acoes=('ativo', 'nunique'))
    fig = px.line(hist_acoes, title='Número de ações em carteira', height=300, markers=True)
    fig.update_traces(line_color=CORES[fundo])
    fig.update_layout(showlegend=False, xaxis_title=None, yaxis_title=None)
    st.plotly_chart(fig, width="stretch")

    # Ações vendidas: carteiras sem short entram com zero
    hist_vendidas = (
        pos[pos['classe'] == 'Ações (vendido)'].groupby('data')['ativo'].nunique()
        .reindex(sorted(pos['data'].unique()), fill_value=0)
    )
    if hist_vendidas.sum() == 0:
        st.info('O fundo não teve ações vendidas (short) em nenhuma carteira do CDA.')
    else:
        fig = px.line(hist_vendidas, title='Número de ações vendidas em carteira', height=300, markers=True)
        fig.update_traces(line_color='#e76f51')
        fig.update_layout(showlegend=False, xaxis_title=None, yaxis_title=None)
        st.plotly_chart(fig, width="stretch")
        st.caption('Ações vendidas = posições em "Obrigações por ações e outros TVM recebidos em empréstimo" (short).')

    # Ações no exterior (bloco 7, sem fundos offshore, caixa, títulos e derivativos); carteiras sem ações entram com zero
    acoes_exterior = carteira[carteira['acao_exterior']]
    hist_exterior = (
        dados.nome_acao_exterior(acoes_exterior).groupby(acoes_exterior['data']).nunique()
        .reindex(sorted(pos['data'].unique()), fill_value=0)
    )
    if hist_exterior.sum() == 0:
        st.info('O fundo não teve ações no exterior em nenhuma carteira do CDA (só fundos offshore, se houver).')
    else:
        fig = px.line(hist_exterior, title='Número de ações (exterior) em carteira', height=300, markers=True)
        fig.update_traces(line_color=CORES['IP'])
        fig.update_layout(showlegend=False, xaxis_title=None, yaxis_title=None)
        st.plotly_chart(fig, width="stretch")
        st.caption('Ativos de "Investimento no Exterior" exceto fundos offshore, caixa, títulos, opções e swaps. '
                   'Ações detidas via fundo offshore (ex.: Squadra, Dynamo Cougar) não aparecem no CDA e ficam de fora.')

# ---------------------------------------------------------------------------------------------------------------------
# Movimentações entre carteiras
# ---------------------------------------------------------------------------------------------------------------------
with aba_mov:
    carteira = df_carteiras[df_carteiras['fundo'] == fundo]
    pos = dados.posicoes_por_ativo(carteira)
    datas_cda = sorted(pos['data'].unique(), reverse=True)

    if len(datas_cda) < 2:
        st.info('O fundo tem só uma carteira no CDA; não há com o que comparar.')
        st.stop()

    # Chaves por fundo: cada fundo tem datas de CDA diferentes
    col_a, col_b, col_c = st.columns(3)
    data_atual = col_a.selectbox('Carteira', datas_cda[:-1], format_func=lambda d: f'{pd.Timestamp(d):%m/%Y}',
                                 key=f'mov_atual_{fundo}')
    anteriores = [d for d in datas_cda if d < data_atual]
    data_anterior = col_b.selectbox('Comparar com', anteriores, format_func=lambda d: f'{pd.Timestamp(d):%m/%Y}',
                                    key=f'mov_ant_{fundo}_{pd.Timestamp(data_atual):%Y%m}')
    opcoes_classes = sorted(pos['classe'].unique())
    classes_mov = col_c.multiselect('Classes', opcoes_classes, key=f'mov_classes_{fundo}',
                                    default=[c for c in opcoes_classes if c.startswith('Ações')])

    # Carteira que não soma ~100% do PL tem posições omitidas: o que sumiu pode não ter sido vendido
    totais = dados.peso_total(carteira)
    for data in [data_anterior, data_atual]:
        total = totais.get(data)
        if total is not None and abs(total - 1) > 0.02:
            st.warning(f'A carteira de {pd.Timestamp(data):%m/%Y} soma {total:.1%} do PL. Parte das posições pode estar '
                       'omitida (ex.: confidencial), então algumas Entradas/Saídas podem não ser compras ou vendas.')

    df_mov = dados.movimentacoes(pos, data_atual, data_anterior)
    df_mov = df_mov[df_mov['classe'].isin(classes_mov)]

    cols = st.columns(5)
    rotulos = {'Entrada': 'Entradas', 'Saída': 'Saídas', 'Aumento': 'Aumentos', 'Redução': 'Reduções',
               'Troca de código': 'Trocas de código'}
    for col, (mov, rotulo) in zip(cols, rotulos.items()):
        sel = df_mov[df_mov['movimento'] == mov]
        col.metric(rotulo, len(sel))
        nomes = sel['ativo'] + sel['observacao'].map(lambda o: f' ({o})' if o else '')
        col.write(', '.join(nomes) if len(sel) else '-')
    st.caption('Uma Saída pode ser posição que ficou confidencial ou que mudou de código, não necessariamente uma venda. '
               'Trocas de código (ex.: ELET3 → AXIA3, RENT1 → RENT9) são separadas; Saídas que voltam nas 3 carteiras seguintes são indicadas.')

    relevantes = df_mov[df_mov['movimento'] != 'Sem mudança relevante'].sort_values('variacao_peso')
    if not relevantes.empty:
        fig = px.bar(relevantes, x='variacao_peso', y='ativo', orientation='h', color='movimento',
                     title='Variação de peso (p.p. do PL)', height=max(300, 22 * len(relevantes)),
                     color_discrete_map={'Entrada': '#2a9d8f', 'Aumento': '#8ac926', 'Redução': '#f4a261', 'Saída': '#e76f51',
                                        'Troca de código': '#9e9e9e'})
        fig.update_layout(xaxis_tickformat='.1%', xaxis_title=None, yaxis_title=None)
        st.plotly_chart(fig, width="stretch")
    st.caption('Aumento/Redução: variação de peso maior que 0,25 p.p. Parte da variação vem da oscilação de preço, não só de compras e vendas.')

    st.dataframe(
        df_mov[['movimento', 'classe', 'ativo', 'peso_anterior', 'peso_atual', 'variacao_peso', 'compras_no_mes', 'vendas_no_mes',
                'observacao']]
        .rename(columns={'movimento': 'Movimento', 'classe': 'Classe', 'ativo': 'Ativo', 'peso_anterior': 'Peso anterior',
                         'peso_atual': 'Peso atual', 'variacao_peso': 'Variação', 'compras_no_mes': 'Compras no mês',
                         'vendas_no_mes': 'Vendas no mês', 'observacao': 'Observação'})
        .style.format({'Peso anterior': fmt_pct, 'Peso atual': fmt_pct, 'Variação': '{:+.2%}',
                       'Compras no mês': fmt_mi, 'Vendas no mês': fmt_mi}),
        width="stretch", hide_index=True,
    )

    # Histórico de peso por ativo
    st.subheader('Histórico de peso por ativo')
    acoes_hist = pos[pos['classe'].isin(classes_mov)]
    ultimos = acoes_hist[acoes_hist['data'] == acoes_hist['data'].max()].nlargest(5, 'peso')['ativo'].tolist()
    selecionados = st.multiselect('Ativos', sorted(acoes_hist['ativo'].unique()), default=ultimos)
    if selecionados:
        hist = (
            acoes_hist[acoes_hist['ativo'].isin(selecionados)]
            .pivot_table(index='data', columns='ativo', values='peso', aggfunc='sum')
            .reindex(sorted(pos['data'].unique())).fillna(0)
        )
        fig = px.line(hist, markers=True, height=420)
        fig.update_layout(yaxis_tickformat='.1%', xaxis_title=None, yaxis_title='% PL', hovermode='x unified')
        st.plotly_chart(fig, width="stretch")
