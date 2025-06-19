import pymongo
import pandas as pd
import os
import psycopg2

# Passo 1: Conectar ao MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["ods13_db"]
client.drop_database("ods13_db")
if "ods13_db" not in client.list_database_names():
    print("Conexão com MongoDB estabelecida e banco ods13_db preparado com sucesso!")
else:
    print("Erro: Banco ods13_db não foi limpo corretamente!")

def conectar():
    print("Conectando...")
    return psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="myql",
        host="localhost",
        port="5432"
    )
# Passo 2: Conectar ao PostgreSQL
try:

    conn = conectar()
    cursor = conn.cursor()
    print("Conexão com PostgreSQL estabelecida com sucesso!")
except Exception as e:
    print(f"Erro ao conectar ao PostgreSQL: {e}")
    exit(1)

# Diretório para salvar CSVs
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(BASE_DIR, "../dados-pre-processados")
os.makedirs(data_dir, exist_ok=True)

# Função para executar consulta SQL e salvar CSV
def query_to_csv(query, csv_name):
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        csv_path = os.path.join(data_dir, csv_name)
        df.to_csv(csv_path, index=False, encoding="utf-8")
        print(f"CSV {csv_name} gerado com sucesso em {csv_path}")
        return df
    except Exception as e:
        print(f"Erro ao gerar CSV {csv_name}: {e}")
        return pd.DataFrame()

# Gerar CSVs a partir do PostgreSQL
query_regiao = 'SELECT regiao_code, nome FROM public."Região"'
regiao_df = query_to_csv(query_regiao, "regiao.csv")

query_paises = 'SELECT iso_code, nome, regiao_code FROM public."Países"'
paises_df = query_to_csv(query_paises, "paises.csv")

query_fontes_poluente = 'SELECT fonte_poluente_id, nome FROM public."FontesPoluente"'
fontes_poluente_df = query_to_csv(query_fontes_poluente, "fontes_poluente.csv")

query_fontes_energia = 'SELECT fonte_energia_id, nome FROM public."FontesEnergia"'
fontes_energia_df = query_to_csv(query_fontes_energia, "fontes_energia.csv")

query_emissao_poluentes = """
SELECT e.iso_code, e.ano, e.emissao, f.fonte_poluente_id, f.nome AS fonte_poluente
FROM public."EmissãoPoluentes" e
JOIN public."FontesPoluente" f ON e.fonte_poluente_id = f.fonte_poluente_id
"""
emissao_poluentes_df = query_to_csv(query_emissao_poluentes, "emissao_poluentes.csv")

query_atividades_energia = """
SELECT a.iso_code, a.ano, a.consumo, f.fonte_energia_id, f.nome AS fonte_energia
FROM public."AtividadesEnergia" a
JOIN public."FontesEnergia" f ON a.fonte_energia_id = f.fonte_energia_id
"""
atividades_energia_df = query_to_csv(query_atividades_energia, "atividades_energia.csv")

query_indicadores_economicos = 'SELECT * FROM public."IndicadoresEconômicos"'
indicadores_economicos_df = query_to_csv(query_indicadores_economicos, "indicadores_economicos.csv")

# Fechar conexão com PostgreSQL
cursor.close()
conn.close()

# Passo 3: Popular MongoDB a partir dos CSVs gerados
fontes_poluente_dict = [
    {"_id": row["fonte_poluente_id"], "nome": row["nome"]}
    for _, row in fontes_poluente_df.iterrows()
]
db.fontes_poluente.insert_many(fontes_poluente_dict)
print(f"Coleção fontes_poluente populada com {len(fontes_poluente_dict)} documentos.")

fontes_energia_dict = [
    {"_id": row["fonte_energia_id"], "nome": row["nome"]}
    for _, row in fontes_energia_df.iterrows()
]
db.fontes_energia.insert_many(fontes_energia_dict)
print(f"Coleção fontes_energia populada com {len(fontes_energia_dict)} documentos.")

# Inicializar Paises com base em paises.csv
paises_dict = {}
for _, row in paises_df.iterrows():
    iso_code = row["iso_code"]
    paises_dict[iso_code] = {
        "_id": iso_code,
        "nome": row["nome"],
        "regiao": {
            "regiao_code": row["regiao_code"],
            "nome": regiao_df[regiao_df["regiao_code"] == row["regiao_code"]]["nome"].iloc[0]
            if not regiao_df[regiao_df["regiao_code"] == row["regiao_code"]].empty
            else "World"
        },
        "emissoes": [],
        "energia": [],
        "indicadores_economicos": []
    }

# Popular array emissoes
for _, row in emissao_poluentes_df.iterrows():
    iso_code = row["iso_code"]
    if iso_code in paises_dict:
        paises_dict[iso_code]["emissoes"].append({
            "ano": int(row["ano"]),
            "fonte_poluente": row["fonte_poluente"],
            "emissao": float(row["emissao"])
        })

# Popular array energia
for _, row in atividades_energia_df.iterrows():
    iso_code = row["iso_code"]
    if iso_code in paises_dict:
        paises_dict[iso_code]["energia"].append({
            "ano": int(row["ano"]),
            "fonte_energia": row["fonte_energia"],
            "consumo": float(row["consumo"])
        })

# Popular array indicadores_economicos
indicadores_cols = [col for col in indicadores_economicos_df.columns if col not in ["iso_code", "ano"]]
for _, row in indicadores_economicos_df.iterrows():
    iso_code = row["iso_code"]
    if iso_code in paises_dict:
        indicadores = {"ano": int(row["ano"])}
        for col in indicadores_cols:
            if pd.notna(row[col]):
                indicadores[col] = row[col]
        paises_dict[iso_code]["indicadores_economicos"].append(indicadores)

# Inserir documentos na coleção Paises
db.paises.insert_many(list(paises_dict.values()))
print(f"Coleção paises populada com {len(paises_dict)} documentos.")

# Testar e Validar o Banco
print("\nValidação:")
print(f"Total de documentos em fontes_poluente: {db.fontes_poluente.count_documents({})}")
print(f"Total de documentos em fontes_energia: {db.fontes_energia.count_documents({})}")
print(f"Total de documentos em paises: {db.paises.count_documents({})}")
sample_pais = db.paises.find_one({"_id": "BRA"})
if sample_pais:
    print(f"\nExemplo de documento para Brasil (BRA):")
    print(f"  Nome: {sample_pais['nome']}")
    print(f"  Região: {sample_pais['regiao']['nome']}")
    print(f"  Emissões: {len(sample_pais['emissoes'])} registros")
    print(f"  Energia: {len(sample_pais['energia'])} registros")
    print(f"  Indicadores Econômicos: {len(sample_pais['indicadores_economicos'])} registros")
else:
    print("Documento para Brasil (BRA) não encontrado.")
sample_pais = db.paises.find_one({"_id": "USA"})
if sample_pais:
    print(f"\nExemplo de documento para Estados Unidos (USA):")
    print(f"  Nome: {sample_pais['nome']}")
    print(f"  Região: {sample_pais['regiao']['nome']}")
    print(f"  Emissões: {len(sample_pais['emissoes'])} registros")
    print(f"  Energia: {len(sample_pais['energia'])} registros")
    print(f"  Indicadores Econômicos: {len(sample_pais['indicadores_economicos'])} registros")
else:
    print("Documento para Estados Unidos (USA) não encontrado.")



# Fechar conexão com MongoDB
client.close()
print("Banco MongoDB populado com sucesso!")
