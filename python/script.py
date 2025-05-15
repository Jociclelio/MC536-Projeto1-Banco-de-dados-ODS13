import pandas as pd
import psycopg2
from psycopg2 import sql
from tabulate import tabulate
import os

import parses

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
          
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
                DROP TABLE IF EXISTS "Região" CASCADE;
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
            with open(os.path.join(BASE_DIR, '../modelos/ModeloFisico.sql'), 'r', encoding='utf-8') as f:
                sql_script = f.read()
                # Executa o script SQL
                cursor.execute(sql_script)
                conn.commit()
                print("Tabelas criadas com sucesso")
        except Exception as e:
            conn.rollback()
            print(f"Erro: {e}")


# Carregar datasets
co2_df = pd.read_csv(os.path.join(BASE_DIR, "../datasets/owid-co2-data.csv"))
energy_df = pd.read_csv(os.path.join(BASE_DIR, "../datasets/owid-energy-data.csv"))
pip_df = pd.read_csv(os.path.join(BASE_DIR, "../datasets/pip.csv"))

# Guardar o nome e referencia dos csv gerados
tabelas_arquivos = {}

#pre-processa os dados nos dadasets
parses.regiao(pip_df,tabelas_arquivos)
parses.paises(co2_df,energy_df, pip_df,tabelas_arquivos)
parses.tipo_gases(tabelas_arquivos)
parses.fontes_poluente(tabelas_arquivos)
parses.fontes_energia(tabelas_arquivos)
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
def consultar_tabela(cursor, tabela):
    # Monta a consulta SQL dinamicamente
    query = sql.SQL('SELECT * FROM public.{} ORDER BY 1 LIMIT 10;').format(sql.Identifier(tabela))
    cursor.execute(query)
    
    # Obtém os resultados
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
   
    print("Mostrando a tabela", tabela)
    # Exibe os resultados
    if rows:
        print(tabulate(rows, headers=columns, tablefmt='psql', floatfmt=".2f"))

# Mostra as tabelas inseridas no banco
with conectar() as conn:
    with conn.cursor() as cursor:
        for tabela, _ in tabelas_arquivos.items():
            consultar_tabela(cursor, tabela)

# Executa e mostra as querys nao triviais em ../consultas
# salva o resultado em ../consultas/resultados
try:
    with conectar() as conn: 
        with conn.cursor() as cursor:
            # Diretórios
            consultas_dir = os.path.join(BASE_DIR, '../consultas')
            resultados_dir = os.path.join(BASE_DIR, '../consultas/resultados')
            os.makedirs(resultados_dir, exist_ok=True)
            
            # Executar consultas dinamicamente
            i = 1
            while True:
                query_file = f'{consultas_dir}/query{i}.sql'
                txt_file = f'{consultas_dir}/query{i}.txt'
                result_file = f'{resultados_dir}/query{i}.csv'
                
                # Verificar se o arquivo de consulta existe
                if not os.path.exists(query_file):
                    print(f"\nNenhuma query{i}.sql encontrada. Finalizando.")
                    break
                
                # Ler consulta
                try:
                    with open(query_file, 'r', encoding='utf-8') as f:
                        query = f.read()
                except Exception as e:
                    print(f"Erro ao ler {query_file}: {e}")
                    i += 1
                    continue
                
                # Executar consulta
                try:
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    
                    # Exibir resultado
                    print(f"\nConsulta {i}: ", end="")
                    # Ler e exibir query{i}.txt, se existir
                    if os.path.exists(txt_file):
                        try:
                            with open(txt_file, 'r', encoding='utf-8') as f:
                                conteudo = f.read()
                            print(conteudo)
                        except Exception as e:
                            print(f"Erro ao ler {txt_file}: {e}")
                    else:
                        print(f"\nNenhum {txt_file} encontrado.")
                    
                    print(tabulate(rows, headers=columns, tablefmt='psql', floatfmt=".2f"))
                    
                    # Salvar como CSV
                    df = pd.DataFrame(rows, columns=columns)
                    df.to_csv(result_file, index=False, encoding='utf-8')
                    print(f"Resultado salvo em {result_file}")
                    
                    # Commit para evitar transação abortada
                    conn.commit()
                    
                except Exception as e:
                    print(f"Erro ao executar consulta {i}: {e}")
                    # Rollback para limpar o erro
                    conn.rollback()
                
                i += 1
                
except Exception as e:
    print(f"Erro geral: {e}")
