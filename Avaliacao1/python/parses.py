import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def regiao(pip_df,tabelas_arquivos):
    try:
        # Lê o dataset
        df = pip_df
        
        # Extrai region_code e region_name, remove duplicatas
        regioes = df[['region_code', 'region_name']].drop_duplicates()
        
        # Renomeia colunas para o schema
        regioes = regioes.rename(columns={'region_code': 'regiao_code', 'region_name': 'nome'})
        
        # Validações
        regioes['regiao_code'] = regioes['regiao_code'].astype(str).str[:4]  # Trunca para 4 caracteres
        regioes['nome'] = regioes['nome'].astype(str).str[:50]  # Trunca para 50 caracteres
        regioes = regioes.dropna()  # Remove linhas com valores nulos
        
        # Remove duplicatas de regiao_code (mantém o primeiro)
        regioes = regioes.drop_duplicates(subset='regiao_code', keep='first')
        
        # Adiciona WLD como região padrão, se não estiver presente
        if 'WLD' not in regioes['regiao_code'].values:
            wld = pd.DataFrame([{'regiao_code': 'WLD', 'nome': 'World'}])
            regioes = pd.concat([regioes, wld], ignore_index=True)
        
        # Salva CSV
        regioes_path = os.path.join(BASE_DIR, "../dados-pre-processados/regiao.csv")
        regioes.to_csv(regioes_path, index=False, encoding='utf-8')
        print(f"Arquivo regiao.csv gerado com sucesso!")
        
        # Adiciona à lista de tabelas para importação
        tabelas_arquivos["Região"] = regioes_path
        
    except Exception as e:
        print(f"Erro ao gerar regioes.csv: {e}")

# Faz o parse de Países
def paises(co2_df, energy_df, pip_df, tabelas_arquivos):
    try:
        # Combina iso_code e country de co2_df e energy_df
        countries_co2 = co2_df[['iso_code', 'country']].dropna()
        countries_energy = energy_df[['iso_code', 'country']].dropna()
        countries = pd.concat([countries_co2, countries_energy]).drop_duplicates()
        countries = countries[['iso_code', 'country']].rename(columns={'country': 'nome'})
        
        # Extrai iso_code, nome e regiao_code do pip_df
        pip_countries = pip_df[['country_code', 'country_name', 'region_code']].drop_duplicates()
        pip_countries = pip_countries.rename(columns={
            'country_code': 'iso_code',
            'country_name': 'nome',
            'region_code': 'regiao_code'
        })
        
        # Combina com pip_df para adicionar regiao_code
        countries = countries.merge(
            pip_countries[['iso_code', 'regiao_code']],
            on='iso_code',
            how='left'
        )
        
        # Preenche regiao_code nulos com 'WLD'
        if countries['regiao_code'].isna().any():
            missing = countries[countries['regiao_code'].isna()]['iso_code'].tolist()
            print(f"Aviso: Países sem regiao_code: {missing}. Atribuindo 'WLD'.")
            countries['regiao_code'] = countries['regiao_code'].fillna('WLD')
        
        # Validações
        countries['iso_code'] = countries['iso_code'].astype(str).str[:4]
        countries['regiao_code'] = countries['regiao_code'].astype(str).str[:4]
        countries['nome'] = countries['nome'].astype(str).str[:50]
        countries = countries.dropna(subset=['iso_code', 'nome'])
        countries = countries.drop_duplicates(subset='iso_code')
        
        # Salva CSV
        countries_path = os.path.join(BASE_DIR, '../dados-pre-processados/paises.csv')
        countries[['iso_code', 'regiao_code', 'nome']].to_csv(
            countries_path, index=False, encoding='utf-8'
        )
        print("Arquivo paises.csv gerado com sucesso!")
        
        # Adiciona à lista de tabelas para importação
        tabelas_arquivos["Países"] = countries_path
        
    except Exception as e:
        print(f"Erro ao gerar paises.csv: {e}")

# Cria a tabela de tipo Gases
def tipo_gases(tabelas_arquivos):
    # Cria a tabela Tipo Gases
    try:
        # Lista fixa de tipos de gases (categorias de poluentes)
        tipo_gases = [
            {'tipo_gas_id': 1, 'nome': 'Greenhouse Gas'}
        ]
    
        # Cria um DataFrame com os dados
        df = pd.DataFrame(tipo_gases)
    
        # Salva como CSV
        tipo_gases_path = os.path.join(BASE_DIR, '../dados-pre-processados/tipo_gases.csv')
        df.to_csv(tipo_gases_path, index=False, encoding='utf-8')
        print("Arquivo tipo_gases.csv gerado com sucesso!")
    
        # Adiciona à lista de tabelas para importação
        tabelas_arquivos["TipoGases"] = tipo_gases_path
    
    except Exception as e:
        print(f"Erro ao gerar tipo_gases.csv: {e}")

# Cria a tabela fonte_poluentes
def fontes_poluente(tabelas_arquivos):
    try:
        # Lista fixa de fontes de poluição
        fontes_poluente = [
            {'fonte_poluente_id': 1, 'nome': 'Cement'},
            {'fonte_poluente_id': 2, 'nome': 'Coal'},
            {'fonte_poluente_id': 3, 'nome': 'Oil'},
            {'fonte_poluente_id': 4, 'nome': 'Gas'},
            {'fonte_poluente_id': 5, 'nome': 'Flaring'},
            {'fonte_poluente_id': 6, 'nome': 'Other Industry'},
            {'fonte_poluente_id': 7, 'nome': 'Land Use Change'}
        ]
        
        # Cria um DataFrame com os dados
        df = pd.DataFrame(fontes_poluente)
        
        # Salva como CSV
        fontes_poluente_path = os.path.join(BASE_DIR, '../dados-pre-processados/fontes_poluente.csv')
        df.to_csv(fontes_poluente_path, index=False, encoding='utf-8')
        print("Arquivo fontes_poluente.csv gerado com sucesso!")
        
        # Adiciona à lista de tabelas para importação
        tabelas_arquivos["FontesPoluente"] = fontes_poluente_path
        
    except Exception as e:
        print(f"Erro ao gerar fontes_poluente.csv: {e}")

# Cria a tabela Fontes Energia
def fontes_energia(tabelas_arquivos):
    try:
        # Lista fixa de fontes de energia
        fontes_energia = [
            {'fonte_energia_id': 1, 'nome': 'Biofuel'},
            {'fonte_energia_id': 2, 'nome': 'Coal'},
            {'fonte_energia_id': 3, 'nome': 'Gas'},
            {'fonte_energia_id': 4, 'nome': 'Hydro'},
            {'fonte_energia_id': 5, 'nome': 'Nuclear'},
            {'fonte_energia_id': 6, 'nome': 'Oil'},
            {'fonte_energia_id': 7, 'nome': 'Solar'},
            {'fonte_energia_id': 8, 'nome': 'Wind'},
            {'fonte_energia_id': 9, 'nome': 'Other Renewables'}
        ]
        
        # Cria um DataFrame com os dados
        df = pd.DataFrame(fontes_energia)
        
        # Salva como CSV
        fontes_energia_path = os.path.join(BASE_DIR, '../dados-pre-processados/fontes_energia.csv')
        df.to_csv(fontes_energia_path, index=False, encoding='utf-8')
        print("Arquivo fontes_energia.csv gerado com sucesso!")
        
        # Adiciona à lista de tabelas para importação
        tabelas_arquivos["FontesEnergia"] = fontes_energia_path
        
    except Exception as e:
        print(f"Erro ao gerar fontes_energia.csv: {e}")
 
# Faz o parse dos indicadores economicos
def indicadores_economicos(co2_df,energy_df,tabelas_arquivos):
    try:
        # Combina iso_code, year, gdp dos dois datasets
        indicadores_co2 = co2_df[['iso_code', 'year', 'gdp']].dropna()
        indicadores_energy = energy_df[['iso_code', 'year', 'gdp']].dropna()
        indicadores = pd.concat([indicadores_co2, indicadores_energy]).drop_duplicates()
        
        # Renomeia as colunas para corresponder à tabela
        indicadores = indicadores.rename(columns={'year': 'ano'})
        
        # Salva como CSV
        indicadores_path = os.path.join(BASE_DIR, '../dados-pre-processados/indicadores_economicos.csv')
        indicadores.to_csv(indicadores_path, index=False, encoding='utf-8')
        print("Arquivo indicadores_economicos.csv gerado com sucesso!")
        
        # Adiciona à lista de tabelas para importação
        tabelas_arquivos["IndicadoresEconômicos"] = indicadores_path
        
    except Exception as e:
        print(f"Erro ao gerar indicadores_economicos.csv: {e}")

# Cria a tababela gases
def gases(tabelas_arquivos):
    try:
        # Lista fixa de gases
        gases = [
            {'gas_id': 1, 'tipo_gas_id': 1, 'nome': 'CO2', },  # CO₂ como Greenhouse Gas
            {'gas_id': 2, 'tipo_gas_id': 1, 'nome': 'Methane', },  # Metano como Greenhouse Gas
            {'gas_id': 3, 'tipo_gas_id': 1, 'nome': 'Nitrous Oxide', }  # Óxido nitroso como Greenhouse Gas
        ]
        
        # Cria um DataFrame com os dados
        df = pd.DataFrame(gases)
        
        # Salva como CSV
        gases_path = os.path.join(BASE_DIR, '../dados-pre-processados/gases.csv')
        df.to_csv(gases_path, index=False, encoding='utf-8')
        print("Arquivo gases.csv gerado com sucesso!")
        
        # Adiciona à lista de tabelas para importação
        tabelas_arquivos["Gases"] = gases_path
        
    except Exception as e:
        print(f"Erro ao gerar gases.csv: {e}")    

# Faz o parse da demografia
def demografia(co2_df,energy_df,tabelas_arquivos):
    try:
        # Combina iso_code, year, population dos dois datasets
        demografia_co2 = co2_df[['iso_code', 'year', 'population']].dropna()
        demografia_energy = energy_df[['iso_code', 'year', 'population']].dropna()
        demografia = pd.concat([demografia_co2, demografia_energy])
        
        # Agrupa por iso_code e year, calculando a média de population
        demografia = demografia.groupby(['iso_code', 'year'])['population'].mean().reset_index()
        
        # Renomeia as colunas para corresponder à tabela
        demografia = demografia.rename(columns={'year': 'ano'})
        
        # Converte populacao para inteiro (arredondando a média)
        demografia['population'] = demografia['population'].round().astype(int)
        
        # Salva como CSV
        demografia_path = os.path.join(BASE_DIR, '../dados-pre-processados/demografia.csv')
        demografia.to_csv(demografia_path, index=False, encoding='utf-8')
        print("Arquivo demografia.csv gerado com sucesso!")
        
        # Adiciona à lista de tabelas para importação
        tabelas_arquivos["Demografia"] = demografia_path
        
    except Exception as e:
        print(f"Erro ao gerar demografia.csv: {e}")

#Faz o parse da emissao total de ghg
def emissao_total_ghg(co2_df,tabelas_arquivos):
    try:
        # Seleciona iso_code, year, total_ghg, total_ghg_excluding_lucf
        emissao_ghg = co2_df[['iso_code', 'year', 'total_ghg', 'total_ghg_excluding_lucf']].dropna(subset=['iso_code', 'year'])
        
        # Filtra linhas onde pelo menos um dos campos (total_ghg ou total_ghg_excluding_lucf) é não nulo
        emissao_ghg = emissao_ghg.dropna(subset=['total_ghg', 'total_ghg_excluding_lucf'], how='all')
        
        # Renomeia as colunas para corresponder à tabela
        emissao_ghg = emissao_ghg.rename(columns={'year': 'ano'})
        
        # Salva como CSV
        emissao_ghg_path = os.path.join(BASE_DIR, '../dados-pre-processados/emissao_total_ghg.csv')
        emissao_ghg.to_csv(emissao_ghg_path, index=False, encoding='utf-8')
        print("Arquivo emissao_total_ghg.csv gerado com sucesso!")
        
        # Adiciona à lista de tabelas para importação
        tabelas_arquivos["EmissãoTotalGHG"] = emissao_ghg_path
        
    except Exception as e:
        print(f"Erro ao gerar emissao_total_ghg.csv: {e}")

#Faz o parse de emissao comércio
def emissao_comercio(co2_df,tabelas_arquivos):
    try:
        # Seleciona iso_code, year, trade_co2, consumption_co2
        emissao_comercio = co2_df[['iso_code', 'year', 'trade_co2', 'consumption_co2']].dropna(subset=['iso_code', 'year'])
        
        # Filtra linhas onde pelo menos um dos campos (trade_co2 ou consumption_co2) é não nulo
        emissao_comercio = emissao_comercio.dropna(subset=['trade_co2', 'consumption_co2'], how='all')
        
        # Adiciona gas_id fixo (1 para CO2)
        emissao_comercio['gas_id'] = 1
        
        # Renomeia as colunas para corresponder à tabela
        emissao_comercio = emissao_comercio.rename(columns={'year': 'ano'})
        
        # Reordena as colunas para corresponder à tabela
        emissao_comercio = emissao_comercio[['iso_code', 'gas_id','ano',  'trade_co2', 'consumption_co2']]
        
        # Salva como CSV
        emissao_comercio_path = os.path.join(BASE_DIR, '../dados-pre-processados/emissao_comercio.csv')
        emissao_comercio.to_csv(emissao_comercio_path, index=False, encoding='utf-8')
        print("Arquivo emissao_comercio.csv gerado com sucesso!")
        
        # Adiciona à lista de tabelas para importação
        tabelas_arquivos["EmissãoComércio"] = emissao_comercio_path
        
    except Exception as e:
        print(f"Erro ao gerar emissao_comercio.csv: {e}")

# Faz o parse da tabela Emissao poluentes
def emissao_poluentes(co2_df,fontes_poluente_path,tabelas_arquivos):
    try:
        fontes_df = pd.read_csv(fontes_poluente_path)
        # Cria mapeamento de nomes de fontes para fonte_poluente_id
        # Ajusta nomes para corresponder às colunas do dataset
        fonte_map_emissao = {
            'cement_co2': fontes_df[fontes_df['nome'] == 'Cement']['fonte_poluente_id'].iloc[0],
            'coal_co2': fontes_df[fontes_df['nome'] == 'Coal']['fonte_poluente_id'].iloc[0],
            'oil_co2': fontes_df[fontes_df['nome'] == 'Oil']['fonte_poluente_id'].iloc[0],
            'gas_co2': fontes_df[fontes_df['nome'] == 'Gas']['fonte_poluente_id'].iloc[0],
            'flaring_co2': fontes_df[fontes_df['nome'] == 'Flaring']['fonte_poluente_id'].iloc[0],
            'other_industry_co2': fontes_df[fontes_df['nome'] == 'Other Industry']['fonte_poluente_id'].iloc[0],
            'co2_including_luc': fontes_df[fontes_df['nome'] == 'Land Use Change']['fonte_poluente_id'].iloc[0]
        }
        
        # Mapeamento para colunas cumulativas
        fonte_map_cumulativa = {
            'cumulative_cement_co2': fontes_df[fontes_df['nome'] == 'Cement']['fonte_poluente_id'].iloc[0],
            'cumulative_coal_co2': fontes_df[fontes_df['nome'] == 'Coal']['fonte_poluente_id'].iloc[0],
            'cumulative_oil_co2': fontes_df[fontes_df['nome'] == 'Oil']['fonte_poluente_id'].iloc[0],
            'cumulative_gas_co2': fontes_df[fontes_df['nome'] == 'Gas']['fonte_poluente_id'].iloc[0],
            'cumulative_flaring_co2': fontes_df[fontes_df['nome'] == 'Flaring']['fonte_poluente_id'].iloc[0],
            'cumulative_other_co2': fontes_df[fontes_df['nome'] == 'Other Industry']['fonte_poluente_id'].iloc[0],
            'cumulative_co2_including_luc': fontes_df[fontes_df['nome'] == 'Land Use Change']['fonte_poluente_id'].iloc[0]
        }
        
        # Seleciona iso_code, year e as colunas de emissão e cumulativas
        colunas = ['iso_code', 'year'] + list(fonte_map_emissao.keys()) + list(fonte_map_cumulativa.keys())
        emissao_poluentes = co2_df[colunas].dropna(subset=['iso_code', 'year'])
        
        # Transforma colunas de emissão em linhas (formato longo)
        emissao_poluentes_melt = pd.melt(
            emissao_poluentes,
            id_vars=['iso_code', 'year'],
            value_vars=fonte_map_emissao.keys(),
            var_name='fonte',
            value_name='emissao'
        )
        
        # Transforma colunas de emissão cumulativa em linhas (formato longo)
        emissao_cumulativa_melt = pd.melt(
            emissao_poluentes,
            id_vars=['iso_code', 'year'],
            value_vars=fonte_map_cumulativa.keys(),
            var_name='fonte_cumulativa',
            value_name='emissao_cumulativa'
        )
        
        # Adiciona fonte_poluente_id aos DataFrames
        emissao_poluentes_melt['fonte_poluente_id'] = emissao_poluentes_melt['fonte'].map(fonte_map_emissao)
        emissao_cumulativa_melt['fonte_poluente_id'] = emissao_cumulativa_melt['fonte_cumulativa'].map(fonte_map_cumulativa)
        
        # Une os DataFrames de emissão e emissão cumulativa
        emissao_poluentes = emissao_poluentes_melt.merge(
            emissao_cumulativa_melt[['iso_code', 'year', 'fonte_poluente_id', 'emissao_cumulativa']],
            on=['iso_code', 'year', 'fonte_poluente_id'],
            how='left'
        )
        
        # Remove linhas com emissao ou emissao_cumulativa nula
        emissao_poluentes = emissao_poluentes.dropna(subset=['emissao', 'emissao_cumulativa'])
        
        # Adiciona gas_id fixo (1 para CO2)
        emissao_poluentes['gas_id'] = 1
        
        # Renomeia as colunas para corresponder à tabela
        emissao_poluentes = emissao_poluentes.rename(columns={'year': 'ano'})
        
        # Seleciona as colunas na ordem correta
        emissao_poluentes = emissao_poluentes[['iso_code', 'gas_id', 'fonte_poluente_id', 'ano','emissao', 'emissao_cumulativa']]
        
        # Salva como CSV
        emissao_poluentes_path = os.path.join(BASE_DIR, '../dados-pre-processados/emissao_poluentes.csv')
        emissao_poluentes.to_csv(emissao_poluentes_path, index=False, encoding='utf-8')
        print("Arquivo emissao_poluentes.csv gerado com sucesso!")
        
        # Adiciona à lista de tabelas para importação
        tabelas_arquivos["EmissãoPoluentes"] = emissao_poluentes_path
        
    except Exception as e:
        print(f"Erro ao gerar emissao_poluentes.csv: {e}")

# Faz o parse da tabela AtividadesEnergia
def atividades_energia(co2_df,energy_df,fontes_energia_path,tabelas_arquivos):
    try:
        # Carrega o dataset e o CSV de fontes de energia
        fontes_energia_df = pd.read_csv(fontes_energia_path)
        
        # Mapeamento de nomes de fontes para colunas do dataset (consumo, produção, geração)
        fonte_to_coluna = {
            'Biofuel': ('biofuel_consumption', None, 'biofuel_electricity'),
            'Coal': ('coal_consumption', 'coal_production', 'coal_electricity'),
            'Gas': ('gas_consumption', 'gas_production', 'gas_electricity'),
            'Hydro': ('hydro_consumption', None, 'hydro_electricity'),
            'Nuclear': ('nuclear_consumption', None, 'nuclear_electricity'),
            'Oil': ('oil_consumption', 'oil_production', 'oil_electricity'),
            'Solar': ('solar_consumption', None, 'solar_electricity'),
            'Wind': ('wind_consumption', None, 'wind_electricity'),
            'Other Renewables': ('other_renewable_consumption', None, 'other_renewable_electricity')
        }
        
        # Cria mapeamentos para consumo, produção e geração
        fonte_map_consumo = {}
        fonte_map_producao = {}
        fonte_map_geracao = {}
        for nome, (col_consumo, col_producao, col_geracao) in fonte_to_coluna.items():
            if col_consumo in energy_df.columns:
                fonte_id = fontes_energia_df[fontes_energia_df['nome'] == nome]['fonte_energia_id'].iloc[0]
                fonte_map_consumo[col_consumo] = fonte_id
            else:
                print(f"Aviso: Coluna de consumo '{col_consumo}' não encontrada no dataset. Ignorando.")
            if col_producao and col_producao in energy_df.columns:
                fonte_id = fontes_energia_df[fontes_energia_df['nome'] == nome]['fonte_energia_id'].iloc[0]
                fonte_map_producao[col_producao] = fonte_id
            elif col_producao:
                print(f"Aviso: Coluna de produção '{col_producao}' não encontrada no dataset. Ignorando.")
            if col_geracao in energy_df.columns:
                fonte_id = fontes_energia_df[fontes_energia_df['nome'] == nome]['fonte_energia_id'].iloc[0]
                fonte_map_geracao[col_geracao] = fonte_id
            else:
                print(f"Aviso: Coluna de geração '{col_geracao}' não encontrada no dataset. Ignorando.")
        
        # Verifica se há colunas válidas para prosseguir
        if not fonte_map_consumo:
            raise ValueError("Nenhuma coluna de consumo válida encontrada no dataset.")
        
        # Seleciona iso_code, year e as colunas de consumo, produção e geração
        colunas = ['iso_code', 'year'] + list(fonte_map_consumo.keys()) + list(fonte_map_producao.keys()) + list(fonte_map_geracao.keys())
        atividades_energia = energy_df[colunas].dropna(subset=['iso_code', 'year'])
        
        # Transforma colunas de consumo em linhas (formato longo)
        consumo_melt = pd.melt(
            atividades_energia,
            id_vars=['iso_code', 'year'],
            value_vars=fonte_map_consumo.keys(),
            var_name='fonte_consumo',
            value_name='consumo'
        )
        
        # Transforma colunas de produção em linhas (formato longo)
        producao_melt = pd.melt(
            atividades_energia,
            id_vars=['iso_code', 'year'],
            value_vars=fonte_map_producao.keys(),
            var_name='fonte_producao',
            value_name='producao'
        ) if fonte_map_producao else pd.DataFrame(columns=['iso_code', 'year', 'fonte_producao', 'producao'])
        
        # Transforma colunas de geração em linhas (formato longo)
        geracao_melt = pd.melt(
            atividades_energia,
            id_vars=['iso_code', 'year'],
            value_vars=fonte_map_geracao.keys(),
            var_name='fonte_geracao',
            value_name='geracao'
        )
        
        # Adiciona fonte_energia_id aos DataFrames
        consumo_melt['fonte_energia_id'] = consumo_melt['fonte_consumo'].map(fonte_map_consumo)
        producao_melt['fonte_energia_id'] = producao_melt['fonte_producao'].map(fonte_map_producao)
        geracao_melt['fonte_energia_id'] = geracao_melt['fonte_geracao'].map(fonte_map_geracao)
        
        # Une os DataFrames de consumo, produção e geração
        atividades_energia = consumo_melt.merge(
            producao_melt[['iso_code', 'year', 'fonte_energia_id', 'producao']],
            on=['iso_code', 'year', 'fonte_energia_id'],
            how='left'
        ).merge(
            geracao_melt[['iso_code', 'year', 'fonte_energia_id', 'geracao']],
            on=['iso_code', 'year', 'fonte_energia_id'],
            how='left'
        )
        
        # Remove linhas com consumo nulo (producao e geracao podem ser nulos)
        atividades_energia = atividades_energia.dropna(subset=['consumo'])
        
        # Renomeia as colunas para corresponder à tabela
        atividades_energia = atividades_energia.rename(columns={'year': 'ano'})
        
        # Seleciona as colunas na ordem correta
        atividades_energia = atividades_energia[['iso_code', 'fonte_energia_id', 'ano', 'producao', 'geracao', 'consumo']]
        
        # Salva como CSV
        atividades_energia_path = os.path.join(BASE_DIR, '../dados-pre-processados/atividades_energia.csv')
        atividades_energia.to_csv(atividades_energia_path, index=False, encoding='utf-8')
        print("Arquivo atividades_energia.csv gerado com sucesso!")
        
        # Adiciona à lista de tabelas para importação
        tabelas_arquivos["AtividadesEnergia"] = atividades_energia_path
        
    except Exception as e:
        print(f"Erro ao gerar atividades_energia.csv: {e}")
