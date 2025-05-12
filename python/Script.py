import pandas as pd
import psycopg2
from psycopg2 import sql
from tabulate import tabulate

import parses

# Conexão com o banco
def conectar():
    print("Conectando...")
    return psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="",
        host="localhost",
        port="5432"
    )

# Recria o esquema do banco
with conectar() as conn:
    print("Conectado!")
    with conn.cursor() as cursor:
        print("Com cursor")
        try:
            #Deleta as tabelas se elas existirem
            cursor.execute("""
                DROP TABLE IF EXISTS "Países" CASCADE;
                DROP TABLE IF EXISTS "Gases" CASCADE;
                DROP TABLE IF EXISTS "FontesPoluente" CASCADE;
                DROP TABLE IF EXISTS "FontesEnergia" CASCADE;
                DROP TABLE IF EXISTS "EmissãoPoluentes" CASCADE;
                DROP TABLE IF EXISTS "AtividadesEnergia" CASCADE;
                DROP TABLE IF EXISTS "MudançaTemperatura" CASCADE;
                DROP TABLE IF EXISTS "IndicadoresEconômicos" CASCADE;
                DROP TABLE IF EXISTS "Demografia" CASCADE;
                DROP TABLE IF EXISTS "EmissãoComércio" CASCADE;
                DROP TABLE IF EXISTS "EmissãoTotalGHG" CASCADE;
                DROP TABLE IF EXISTS "TipoGases" CASCADE;
                           """)
            with open('../modelos/Modelo-Fisico.sql', 'r', encoding='utf-8') as f:
                sql_script = f.read()
                # Executa o script SQL
                cursor.execute(sql_script)
                conn.commit()
                print("Tabelas criadas com sucesso")
        except Exception as e:
            conn.rollback()
            print(f"Erro: {e}")


# Carregar datasets
co2_df = pd.read_csv("../datasets/owid-co2-data.csv")
energy_df = pd.read_csv("../datasets/owid-energy-data.csv")

# Guardar o nome e referencia dos csv gerados
tabelas_arquivos = {}

#pre-processa os dados nos dadasets
parses.paises(co2_df,energy_df,tabelas_arquivos)
parses.tipo_gases(tabelas_arquivos)
parses.fonte_poluentes(tabelas_arquivos)
parses.fonte_energia(tabelas_arquivos)
parses.indicadores_economicos(co2_df,energy_df,tabelas_arquivos)
parses.gases(tabelas_arquivos)
parses.demografia(co2_df,energy_df,tabelas_arquivos)
parses.emissao_total_ghg(co2_df,tabelas_arquivos)
parses.emissao_comercio(co2_df,tabelas_arquivos)
parses.emissao_poluentes(co2_df,tabelas_arquivos["FontesPoluente"],tabelas_arquivos)
parses.atividades_energia(co2_df,energy_df,tabelas_arquivos["FontesEnergia"],tabelas_arquivos)

# Importa um csv pre processado para o banco
def importar_csv(cursor, tabela, arquivo):
    with open(arquivo, "r", encoding="utf-8") as f:
        cursor.copy_expert(
            sql.SQL("COPY {} FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',', NULL '')").format(sql.Identifier(tabela)),
            f
        )

# Importar dados para o banco de dados
print("Populando o Banco")
with conectar() as conn:
    with conn.cursor() as cursor:
        for tabela, caminho_csv in tabelas_arquivos.items():
            importar_csv(cursor, tabela, caminho_csv)
        conn.commit()
        print("Banco Populado com sucesso")

# Faz uma consulta simples de uma tabela no banco
def consultar_tabela(cursor, tabelas_arquivos):
    # Monta a consulta SQL dinamicamente
    query = sql.SQL('SELECT * FROM public.{} ORDER BY 1 LIMIT 10;').format(sql.Identifier(tabela))
    cursor.execute(query)
    
    # Obtém os resultados
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    
    # Exibe os resultados
    if rows:
        print(tabulate(rows, headers=columns, tablefmt='psql', floatfmt=".2f"))

# Verifica os dados inseridos no banco
with conectar() as conn:
    with conn.cursor() as cursor:
        for tabela, _ in tabelas_arquivos.items():
            consultar_tabela(cursor, tabela)

try:
    with conectar() as conn:
        with conn.cursor() as cursor:
            # Consulta 1: Tendência de Emissões por Fonte Poluente no Brasil (2000–2020)
            print("\nConsulta 1: Tendência de Emissões por Fonte Poluente no Brasil (2000–2020):")
            query1 = """
            SELECT 
                e.ano,
                f.nome AS fonte_poluente,
                SUM(e.emissao) AS total_emissao
            FROM public."EmissãoPoluentes" e
            JOIN public."FontesPoluente" f ON e.fonte_poluente_id = f.fonte_poluente_id
            WHERE e.iso_code = 'BRA' AND e.ano BETWEEN 2000 AND 2020
            GROUP BY e.ano, f.nome
            ORDER BY e.ano, f.nome;
            """
            cursor.execute(query1)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            if rows:
                print(tabulate(rows, headers=columns, tablefmt='psql', floatfmt=".2f"))
            else:
                print("Nenhum dado encontrado para Consulta 1.")

            # Consulta 2: Top 5 Países com Maior Emissão Per Capita em 2020
            print("\nConsulta 2: Top 5 Países com Maior Emissão Per Capita em 2020:")
            query2 = """
            SELECT 
                p.nome AS pais,
                SUM(e.emissao) / d.populacao AS emissao_per_capita
            FROM public."EmissãoPoluentes" e
            JOIN public."Países" p ON e.iso_code = p.iso_code
            JOIN public."Demografia" d ON e.iso_code = d.iso_code AND e.ano = d.ano
            WHERE e.ano = 2020
            GROUP BY p.nome, d.populacao
            ORDER BY emissao_per_capita DESC
            LIMIT 5;
            """
            cursor.execute(query2)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            if rows:
                print(tabulate(rows, headers=columns, tablefmt='psql', floatfmt=".6f"))
            else:
                print("Nenhum dado encontrado para Consulta 2.")

            # Consulta 3: Comparação de Consumo de Energia Renovável vs. Não Renovável por País
            print("\nConsulta 3: Comparação de Consumo de Energia Renovável vs. Não Renovável (2020):")
            query3 = """
            SELECT 
                p.nome AS pais,
                SUM(CASE WHEN fe.nome IN ('Hydro', 'Solar', 'Wind', 'Biofuel', 'Other Renewables') THEN a.consumo ELSE 0 END) AS consumo_renovavel,
                SUM(CASE WHEN fe.nome IN ('Coal', 'Oil', 'Gas') THEN a.consumo ELSE 0 END) AS consumo_nao_renovavel
            FROM public."AtividadesEnergia" a
            JOIN public."Países" p ON a.iso_code = p.iso_code
            JOIN public."FontesEnergia" fe ON a.fonte_energia_id = fe.fonte_energia_id
            WHERE a.ano = 2020
            GROUP BY p.nome
            ORDER BY consumo_renovavel DESC
            LIMIT 10;
            """
            cursor.execute(query3)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            if rows:
                print(tabulate(rows, headers=columns, tablefmt='psql', floatfmt=".2f"))
            else:
                print("Nenhum dado encontrado para Consulta 3.")

            # Consulta 4: Emissões Totais por Região e Fonte Poluente (2020) - Corrigida
            print("\nConsulta 4: Emissões Totais por Região e Fonte Poluente (2020):")
            query4 = """
            WITH regioes AS (
                SELECT 'BRA' AS iso_code, 'América Latina' AS regiao UNION
                SELECT 'ARE' AS iso_code, 'Oriente Médio' AS regiao UNION
                SELECT 'USA' AS iso_code, 'América do Norte' AS regiao UNION
                SELECT 'CHN' AS iso_code, 'Ásia' AS regiao UNION
                SELECT 'DEU' AS iso_code, 'Europa' AS regiao UNION
                SELECT 'IND' AS iso_code, 'Ásia' AS regiao UNION
                SELECT 'RUS' AS iso_code, 'Europa' AS regiao UNION
                SELECT 'JPN' AS iso_code, 'Ásia' AS regiao UNION
                SELECT 'CAN' AS iso_code, 'América do Norte' AS regiao UNION
                SELECT 'GBR' AS iso_code, 'Europa' AS regiao
            )
            SELECT 
                r.regiao,
                f.nome AS fonte_poluente,
                SUM(e.emissao) AS total_emissao
            FROM public."EmissãoPoluentes" e
            JOIN public."Países" p ON e.iso_code = p.iso_code
            JOIN public."FontesPoluente" f ON e.fonte_poluente_id = f.fonte_poluente_id
            LEFT JOIN regioes r ON e.iso_code = r.iso_code
            WHERE e.ano = 2020
            GROUP BY r.regiao, f.nome
            ORDER BY r.regiao, total_emissao DESC;
            """
            cursor.execute(query4)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            if rows:
                print(tabulate(rows, headers=columns, tablefmt='psql', floatfmt=".2f"))
            else:
                print("Nenhum dado encontrado para Consulta 4.")

            # Consulta 5: Países com Maior Redução de Emissões entre 2010 e 2020
            print("\nConsulta 5: Países com Maior Redução de Emissões entre 2010 e 2020:")
            query5 = """
            WITH emissao_2010 AS (
                SELECT 
                    iso_code,
                    SUM(emissao) AS emissao_2010
                FROM public."EmissãoPoluentes"
                WHERE ano = 2010
                GROUP BY iso_code
            ),
            emissao_2020 AS (
                SELECT 
                    iso_code,
                    SUM(emissao) AS emissao_2020
                FROM public."EmissãoPoluentes"
                WHERE ano = 2020
                GROUP BY iso_code
            )
            SELECT 
                p.nome AS pais,
                e2010.emissao_2010,
                e2020.emissao_2020,
                (e2020.emissao_2020 - e2010.emissao_2010) AS reducao_emissao
            FROM emissao_2010 e2010
            JOIN emissao_2020 e2020 ON e2010.iso_code = e2020.iso_code
            JOIN public."Países" p ON e2010.iso_code = p.iso_code
            WHERE e2020.emissao_2020 < e2010.emissao_2010
            ORDER BY reducao_emissao ASC
            LIMIT 5;
            """
            cursor.execute(query5)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            if rows:
                print(tabulate(rows, headers=columns, tablefmt='psql', floatfmt=".2f"))
            else:
                print("Nenhum dado encontrado para Consulta 5.")
except Exception as e:
    print(f"Erro ao executar consultas ODS 13: {e}")


try:
    with conectar() as conn:
        with conn.cursor() as cursor:
            # Consulta 1: Tendência de Emissões por Fonte Poluente no Brasil (2000–2020)
            print("\nConsulta 1: Tendência de Emissões por Fonte Poluente no Brasil (2000–2020):")
            query1 = """
            SELECT 
                e.ano,
                f.nome AS fonte_poluente,
                SUM(e.emissao) AS total_emissao
            FROM public."EmissãoPoluentes" e
            JOIN public."FontesPoluente" f ON e.fonte_poluente_id = f.fonte_poluente_id
            WHERE e.iso_code = 'BRA' AND e.ano BETWEEN 2000 AND 2020
            GROUP BY e.ano, f.nome
            ORDER BY e.ano, f.nome;
            """
            cursor.execute(query1)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            if rows:
                print(tabulate(rows, headers=columns, tablefmt='psql', floatfmt=".2f"))
            else:
                print("Nenhum dado encontrado para Consulta 1.")

            # Consulta 2: Top 5 Países com Maior Emissão Per Capita em 2020
            print("\nConsulta 2: Top 5 Países com Maior Emissão Per Capita em 2020:")
            query2 = """
            SELECT 
                p.nome AS pais,
                SUM(e.emissao) / d.populacao AS emissao_per_capita
            FROM public."EmissãoPoluentes" e
            JOIN public."Países" p ON e.iso_code = p.iso_code
            JOIN public."Demografia" d ON e.iso_code = d.iso_code AND e.ano = d.ano
            WHERE e.ano = 2020
            GROUP BY p.nome, d.populacao
            ORDER BY emissao_per_capita DESC
            LIMIT 5;
            """
            cursor.execute(query2)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            if rows:
                print(tabulate(rows, headers=columns, tablefmt='psql', floatfmt=".6f"))
            else:
                print("Nenhum dado encontrado para Consulta 2.")

            # Consulta 3: Comparação de Consumo de Energia Renovável vs. Não Renovável por País
            print("\nConsulta 3: Comparação de Consumo de Energia Renovável vs. Não Renovável (2020):")
            query3 = """
            SELECT 
                p.nome AS pais,
                SUM(CASE WHEN fe.nome IN ('Hydro', 'Solar', 'Wind', 'Biofuel', 'Other Renewables') THEN a.consumo ELSE 0 END) AS consumo_renovavel,
                SUM(CASE WHEN fe.nome IN ('Coal', 'Oil', 'Gas') THEN a.consumo ELSE 0 END) AS consumo_nao_renovavel
            FROM public."AtividadesEnergia" a
            JOIN public."Países" p ON a.iso_code = p.iso_code
            JOIN public."FontesEnergia" fe ON a.fonte_energia_id = fe.fonte_energia_id
            WHERE a.ano = 2020
            GROUP BY p.nome
            ORDER BY consumo_renovavel DESC
            LIMIT 10;
            """
            cursor.execute(query3)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            if rows:
                print(tabulate(rows, headers=columns, tablefmt='psql', floatfmt=".2f"))
            else:
                print("Nenhum dado encontrado para Consulta 3.")

            # Consulta 4: Emissões Totais por Região e Fonte Poluente (2020) - Corrigida
            print("\nConsulta 4: Emissões Totais por Região e Fonte Poluente (2020):")
            query4 = """
            WITH regioes AS (
                SELECT 'BRA' AS iso_code, 'América Latina' AS regiao UNION
                SELECT 'ARE' AS iso_code, 'Oriente Médio' AS regiao UNION
                SELECT 'USA' AS iso_code, 'América do Norte' AS regiao UNION
                SELECT 'CHN' AS iso_code, 'Ásia' AS regiao UNION
                SELECT 'DEU' AS iso_code, 'Europa' AS regiao UNION
                SELECT 'IND' AS iso_code, 'Ásia' AS regiao UNION
                SELECT 'RUS' AS iso_code, 'Europa' AS regiao UNION
                SELECT 'JPN' AS iso_code, 'Ásia' AS regiao UNION
                SELECT 'CAN' AS iso_code, 'América do Norte' AS regiao UNION
                SELECT 'GBR' AS iso_code, 'Europa' AS regiao
            )
            SELECT 
                r.regiao,
                f.nome AS fonte_poluente,
                SUM(e.emissao) AS total_emissao
            FROM public."EmissãoPoluentes" e
            JOIN public."Países" p ON e.iso_code = p.iso_code
            JOIN public."FontesPoluente" f ON e.fonte_poluente_id = f.fonte_poluente_id
            LEFT JOIN regioes r ON e.iso_code = r.iso_code
            WHERE e.ano = 2020
            GROUP BY r.regiao, f.nome
            ORDER BY r.regiao, total_emissao DESC;
            """
            cursor.execute(query4)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            if rows:
                print(tabulate(rows, headers=columns, tablefmt='psql', floatfmt=".2f"))
            else:
                print("Nenhum dado encontrado para Consulta 4.")

            # Consulta 5: Países com Maior Redução de Emissões entre 2010 e 2020
            print("\nConsulta 5: Países com Maior Redução de Emissões entre 2010 e 2020:")
            query5 = """
            WITH emissao_2010 AS (
                SELECT 
                    iso_code,
                    SUM(emissao) AS emissao_2010
                FROM public."EmissãoPoluentes"
                WHERE ano = 2010
                GROUP BY iso_code
            ),
            emissao_2020 AS (
                SELECT 
                    iso_code,
                    SUM(emissao) AS emissao_2020
                FROM public."EmissãoPoluentes"
                WHERE ano = 2020
                GROUP BY iso_code
            )
            SELECT 
                p.nome AS pais,
                e2010.emissao_2010,
                e2020.emissao_2020,
                (e2020.emissao_2020 - e2010.emissao_2010) AS reducao_emissao
            FROM emissao_2010 e2010
            JOIN emissao_2020 e2020 ON e2010.iso_code = e2020.iso_code
            JOIN public."Países" p ON e2010.iso_code = p.iso_code
            WHERE e2020.emissao_2020 < e2010.emissao_2010
            ORDER BY reducao_emissao ASC
            LIMIT 5;
            """
            cursor.execute(query5)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            if rows:
                print(tabulate(rows, headers=columns, tablefmt='psql', floatfmt=".2f"))
            else:
                print("Nenhum dado encontrado para Consulta 5.")
except Exception as e:
    print(f"Erro ao executar consultas ODS 13: {e}")


try:
    with conectar() as conn:
        with conn.cursor() as cursor:
            # Consulta 1: Tendência de Emissões por Fonte Poluente no Brasil (2000–2020)
            print("\nConsulta 1: Tendência de Emissões por Fonte Poluente no Brasil (2000–2020):")
            query1 = """
            SELECT 
                e.ano,
                f.nome AS fonte_poluente,
                SUM(e.emissao) AS total_emissao
            FROM public."EmissãoPoluentes" e
            JOIN public."FontesPoluente" f ON e.fonte_poluente_id = f.fonte_poluente_id
            WHERE e.iso_code = 'BRA' AND e.ano BETWEEN 2000 AND 2020
            GROUP BY e.ano, f.nome
            ORDER BY e.ano, f.nome;
            """
            cursor.execute(query1)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            if rows:
                print(tabulate(rows, headers=columns, tablefmt='psql', floatfmt=".2f"))
            else:
                print("Nenhum dado encontrado para Consulta 1.")

            # Consulta 2: Top 5 Países com Maior Emissão Per Capita em 2020
            print("\nConsulta 2: Top 5 Países com Maior Emissão Per Capita em 2020:")
            query2 = """
            SELECT 
                p.nome AS pais,
                SUM(e.emissao) / d.populacao AS emissao_per_capita
            FROM public."EmissãoPoluentes" e
            JOIN public."Países" p ON e.iso_code = p.iso_code
            JOIN public."Demografia" d ON e.iso_code = d.iso_code AND e.ano = d.ano
            WHERE e.ano = 2020
            GROUP BY p.nome, d.populacao
            ORDER BY emissao_per_capita DESC
            LIMIT 5;
            """
            cursor.execute(query2)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            if rows:
                print(tabulate(rows, headers=columns, tablefmt='psql', floatfmt=".6f"))
            else:
                print("Nenhum dado encontrado para Consulta 2.")

            # Consulta 3: Comparação de Consumo de Energia Renovável vs. Não Renovável por País
            print("\nConsulta 3: Comparação de Consumo de Energia Renovável vs. Não Renovável (2020):")
            query3 = """
            SELECT 
                p.nome AS pais,
                SUM(CASE WHEN fe.nome IN ('Hydro', 'Solar', 'Wind', 'Biofuel', 'Other Renewables') THEN a.consumo ELSE 0 END) AS consumo_renovavel,
                SUM(CASE WHEN fe.nome IN ('Coal', 'Oil', 'Gas') THEN a.consumo ELSE 0 END) AS consumo_nao_renovavel
            FROM public."AtividadesEnergia" a
            JOIN public."Países" p ON a.iso_code = p.iso_code
            JOIN public."FontesEnergia" fe ON a.fonte_energia_id = fe.fonte_energia_id
            WHERE a.ano = 2020
            GROUP BY p.nome
            ORDER BY consumo_renovavel DESC
            LIMIT 10;
            """
            cursor.execute(query3)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            if rows:
                print(tabulate(rows, headers=columns, tablefmt='psql', floatfmt=".2f"))
            else:
                print("Nenhum dado encontrado para Consulta 3.")

            # Consulta 4: Emissões Totais por Região e Fonte Poluente (2020) - Corrigida
            print("\nConsulta 4: Emissões Totais por Região e Fonte Poluente (2020):")
            query4 = """
            WITH regioes AS (
                SELECT 'BRA' AS iso_code, 'América Latina' AS regiao UNION
                SELECT 'ARE' AS iso_code, 'Oriente Médio' AS regiao UNION
                SELECT 'USA' AS iso_code, 'América do Norte' AS regiao UNION
                SELECT 'CHN' AS iso_code, 'Ásia' AS regiao UNION
                SELECT 'DEU' AS iso_code, 'Europa' AS regiao UNION
                SELECT 'IND' AS iso_code, 'Ásia' AS regiao UNION
                SELECT 'RUS' AS iso_code, 'Europa' AS regiao UNION
                SELECT 'JPN' AS iso_code, 'Ásia' AS regiao UNION
                SELECT 'CAN' AS iso_code, 'América do Norte' AS regiao UNION
                SELECT 'GBR' AS iso_code, 'Europa' AS regiao
            )
            SELECT 
                r.regiao,
                f.nome AS fonte_poluente,
                SUM(e.emissao) AS total_emissao
            FROM public."EmissãoPoluentes" e
            JOIN public."Países" p ON e.iso_code = p.iso_code
            JOIN public."FontesPoluente" f ON e.fonte_poluente_id = f.fonte_poluente_id
            LEFT JOIN regioes r ON e.iso_code = r.iso_code
            WHERE e.ano = 2020
            GROUP BY r.regiao, f.nome
            ORDER BY r.regiao, total_emissao DESC;
            """
            cursor.execute(query4)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            if rows:
                print(tabulate(rows, headers=columns, tablefmt='psql', floatfmt=".2f"))
            else:
                print("Nenhum dado encontrado para Consulta 4.")

            # Consulta 5: Países com Maior Redução de Emissões entre 2010 e 2020
            print("\nConsulta 5: Países com Maior Redução de Emissões entre 2010 e 2020:")
            query5 = """
            WITH emissao_2010 AS (
                SELECT 
                    iso_code,
                    SUM(emissao) AS emissao_2010
                FROM public."EmissãoPoluentes"
                WHERE ano = 2010
                GROUP BY iso_code
            ),
            emissao_2020 AS (
                SELECT 
                    iso_code,
                    SUM(emissao) AS emissao_2020
                FROM public."EmissãoPoluentes"
                WHERE ano = 2020
                GROUP BY iso_code
            )
            SELECT 
                p.nome AS pais,
                e2010.emissao_2010,
                e2020.emissao_2020,
                (e2020.emissao_2020 - e2010.emissao_2010) AS reducao_emissao
            FROM emissao_2010 e2010
            JOIN emissao_2020 e2020 ON e2010.iso_code = e2020.iso_code
            JOIN public."Países" p ON e2010.iso_code = p.iso_code
            WHERE e2020.emissao_2020 < e2010.emissao_2010
            ORDER BY reducao_emissao ASC
            LIMIT 5;
            """
            cursor.execute(query5)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            if rows:
                print(tabulate(rows, headers=columns, tablefmt='psql', floatfmt=".2f"))
            else:
                print("Nenhum dado encontrado para Consulta 5.")
except Exception as e:
    print(f"Erro ao executar consultas ODS 13: {e}")


try:
    with conectar() as conn:
        with conn.cursor() as cursor:
            # Consulta 1: Tendência de Emissões por Fonte Poluente no Brasil (2000–2020)
            print("\nConsulta 1: Tendência de Emissões por Fonte Poluente no Brasil (2000–2020):")
            query1 = """
            SELECT 
                e.ano,
                f.nome AS fonte_poluente,
                SUM(e.emissao) AS total_emissao
            FROM public."EmissãoPoluentes" e
            JOIN public."FontesPoluente" f ON e.fonte_poluente_id = f.fonte_poluente_id
            WHERE e.iso_code = 'BRA' AND e.ano BETWEEN 2000 AND 2020
            GROUP BY e.ano, f.nome
            ORDER BY e.ano, f.nome;
            """
            cursor.execute(query1)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            if rows:
                print(tabulate(rows, headers=columns, tablefmt='psql', floatfmt=".2f"))
            else:
                print("Nenhum dado encontrado para Consulta 1.")

            # Consulta 2: Top 5 Países com Maior Emissão Per Capita em 2020
            print("\nConsulta 2: Top 5 Países com Maior Emissão Per Capita em 2020:")
            query2 = """
            SELECT 
                p.nome AS pais,
                SUM(e.emissao) / d.populacao AS emissao_per_capita
            FROM public."EmissãoPoluentes" e
            JOIN public."Países" p ON e.iso_code = p.iso_code
            JOIN public."Demografia" d ON e.iso_code = d.iso_code AND e.ano = d.ano
            WHERE e.ano = 2020
            GROUP BY p.nome, d.populacao
            ORDER BY emissao_per_capita DESC
            LIMIT 5;
            """
            cursor.execute(query2)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            if rows:
                print(tabulate(rows, headers=columns, tablefmt='psql', floatfmt=".6f"))
            else:
                print("Nenhum dado encontrado para Consulta 2.")

            # Consulta 3: Comparação de Consumo de Energia Renovável vs. Não Renovável por País
            print("\nConsulta 3: Comparação de Consumo de Energia Renovável vs. Não Renovável (2020):")
            query3 = """
            SELECT 
                p.nome AS pais,
                SUM(CASE WHEN fe.nome IN ('Hydro', 'Solar', 'Wind', 'Biofuel', 'Other Renewables') THEN a.consumo ELSE 0 END) AS consumo_renovavel,
                SUM(CASE WHEN fe.nome IN ('Coal', 'Oil', 'Gas') THEN a.consumo ELSE 0 END) AS consumo_nao_renovavel
            FROM public."AtividadesEnergia" a
            JOIN public."Países" p ON a.iso_code = p.iso_code
            JOIN public."FontesEnergia" fe ON a.fonte_energia_id = fe.fonte_energia_id
            WHERE a.ano = 2020
            GROUP BY p.nome
            ORDER BY consumo_renovavel DESC
            LIMIT 10;
            """
            cursor.execute(query3)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            if rows:
                print(tabulate(rows, headers=columns, tablefmt='psql', floatfmt=".2f"))
            else:
                print("Nenhum dado encontrado para Consulta 3.")

            # Consulta 4: Emissões Totais por Região e Fonte Poluente (2020) - Corrigida
            print("\nConsulta 4: Emissões Totais por Região e Fonte Poluente (2020):")
            query4 = """
            WITH regioes AS (
                SELECT 'BRA' AS iso_code, 'América Latina' AS regiao UNION
                SELECT 'ARE' AS iso_code, 'Oriente Médio' AS regiao UNION
                SELECT 'USA' AS iso_code, 'América do Norte' AS regiao UNION
                SELECT 'CHN' AS iso_code, 'Ásia' AS regiao UNION
                SELECT 'DEU' AS iso_code, 'Europa' AS regiao UNION
                SELECT 'IND' AS iso_code, 'Ásia' AS regiao UNION
                SELECT 'RUS' AS iso_code, 'Europa' AS regiao UNION
                SELECT 'JPN' AS iso_code, 'Ásia' AS regiao UNION
                SELECT 'CAN' AS iso_code, 'América do Norte' AS regiao UNION
                SELECT 'GBR' AS iso_code, 'Europa' AS regiao
            )
            SELECT 
                r.regiao,
                f.nome AS fonte_poluente,
                SUM(e.emissao) AS total_emissao
            FROM public."EmissãoPoluentes" e
            JOIN public."Países" p ON e.iso_code = p.iso_code
            JOIN public."FontesPoluente" f ON e.fonte_poluente_id = f.fonte_poluente_id
            LEFT JOIN regioes r ON e.iso_code = r.iso_code
            WHERE e.ano = 2020
            GROUP BY r.regiao, f.nome
            ORDER BY r.regiao, total_emissao DESC;
            """
            cursor.execute(query4)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            if rows:
                print(tabulate(rows, headers=columns, tablefmt='psql', floatfmt=".2f"))
            else:
                print("Nenhum dado encontrado para Consulta 4.")

            # Consulta 5: Países com Maior Redução de Emissões entre 2010 e 2020
            print("\nConsulta 5: Países com Maior Redução de Emissões entre 2010 e 2020:")
            query5 = """
            WITH emissao_2010 AS (
                SELECT 
                    iso_code,
                    SUM(emissao) AS emissao_2010
                FROM public."EmissãoPoluentes"
                WHERE ano = 2010
                GROUP BY iso_code
            ),
            emissao_2020 AS (
                SELECT 
                    iso_code,
                    SUM(emissao) AS emissao_2020
                FROM public."EmissãoPoluentes"
                WHERE ano = 2020
                GROUP BY iso_code
            )
            SELECT 
                p.nome AS pais,
                e2010.emissao_2010,
                e2020.emissao_2020,
                (e2020.emissao_2020 - e2010.emissao_2010) AS reducao_emissao
            FROM emissao_2010 e2010
            JOIN emissao_2020 e2020 ON e2010.iso_code = e2020.iso_code
            JOIN public."Países" p ON e2010.iso_code = p.iso_code
            WHERE e2020.emissao_2020 < e2010.emissao_2010
            ORDER BY reducao_emissao ASC
            LIMIT 5;
            """
            cursor.execute(query5)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            if rows:
                print(tabulate(rows, headers=columns, tablefmt='psql', floatfmt=".2f"))
            else:
                print("Nenhum dado encontrado para Consulta 5.")
except Exception as e:
    print(f"Erro ao executar consultas ODS 13: {e}")
