import pymongo
import pandas as pd
import os

# Conectar ao MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["ods13_db"]
db.drop_database()  # Limpar banco existente

# Diretório dos dados
data_dir = "../dados-pre-processados"

# Carregar datasets
pip_df = pd.read_csv(f"{data_dir}/dataset.csv")
co2_df = pd.read_csv(f"{data_dir}/co2_data.csv")
energy_df = pd.read_csv(f"{data_dir}/atividades_energia.csv")
poluentes_df = pd.read_csv(f"{data_dir}/emissao_poluentes.csv")
econ_df = pd.read_csv(f"{data_dir}/indicadores_economicos.csv")

# Populating FontesPoluente
fontes_poluente = poluentes_df[["fonte_poluente_id", "nome"]].drop_duplicates()
fontes_poluente_dict = [
    {"_id": row["fonte_poluente_id"], "nome": row["nome"]}
    for _, row in fontes_poluente.iterrows()
]
db.fontes_poluente.insert_many(fontes_poluente_dict)
print("FontesPoluente populada!")

# Populating FontesEnergia
fontes_energia = energy_df[["fonte_energia_id", "nome"]].drop_duplicates()
fontes_energia_dict = [
    {"_id": row["fonte_energia_id"], "nome": row["nome"]}
    for _, row in fontes_energia.iterrows()
]
db.fontes_energia.insert_many(fontes_energia_dict)
print("FontesEnergia populada!")

# Populating Paises
paises = pip_df[["country_code", "country_name", "region_code", "region_name"]].drop_duplicates()
paises = paises.rename(columns={"country_code": "iso_code", "country_name": "nome"})
paises_dict = {}
for _, pais in paises.iterrows():
    iso_code = pais["iso_code"]
    paises_dict[iso_code] = {
        "_id": iso_code,
        "nome": pais["nome"],
        "regiao": {
            "regiao_code": pais["region_code"] if pd.notna(pais["region_code"]) else "WLD",
            "nome": pais["region_name"] if pd.notna(pais["region_name"]) else "World"
        },
        "emissoes": [],
        "energia": [],
        "indicadores_economicos": []
    }

# Adicionar países de outros datasets
for df in [co2_df, energy_df, poluentes_df, econ_df]:
    for iso_code in df["iso_code"].drop_duplicates():
        if iso_code not in paises_dict:
            paises_dict[iso_code] = {
                "_id": iso_code,
                "nome": iso_code,
                "regiao": {"regiao_code": "WLD", "nome": "World"},
                "emissoes": [],
                "energia": [],
                "indicadores_economicos": []
            }

# Popular emissoes
for _, row in poluentes_df.iterrows():
    iso_code = row["iso_code"]
    if iso_code in paises_dict:
        fonte = fontes_poluente[fontes_poluente["fonte_poluente_id"] == row["fonte_poluente_id"]]["nome"].iloc[0]
        paises_dict[iso_code]["emissoes"].append({
            "ano": int(row["ano"]),
            "fonte_poluente": fonte,
            "emissao": float(row["emissao"])
        })

# Popular energia
for _, row in energy_df.iterrows():
    iso_code = row["iso_code"]
    if iso_code in paises_dict:
        fonte = fontes_energia[fontes_energia["fonte_energia_id"] == row["fonte_energia_id"]]["nome"].iloc[0]
        paises_dict[iso_code]["energia"].append({
            "ano": int(row["ano"]),
            "fonte_energia": fonte,
            "consumo": float(row["consumo"])
        })

# Popular indicadores econômicos
for _, row in econ_df.iterrows():
    iso_code = row["iso_code"]
    if iso_code in paises_dict:
        # Assumindo que pce existe; ajustar conforme colunas reais
        indicadores = {"ano": int(row["ano"])}
        if "pce" in econ_df.columns:
            indicadores["pce"] = float(row["pce"])
        paises_dict[iso_code]["indicadores_economicos"].append(indicadores)

# Inserir países
db.paises.insert_many(paises_dict.values())
print("Paises populada!")