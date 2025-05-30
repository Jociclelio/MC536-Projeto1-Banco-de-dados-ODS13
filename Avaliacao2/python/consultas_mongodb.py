import pymongo
from tabulate import tabulate
import pandas as pd

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["ods13_db"]

# Consulta 1: Tendência de Emissões por Fonte Poluente no Brasil (2000–2020)
query1 = [
    {"$match": {"_id": "BRA"}},
    {"$unwind": "$emissoes"},
    {"$match": {"emissoes.ano": {"$gte": 2000, "$lte": 2020}}},
    {
        "$group": {
            "_id": {"ano": "$emissoes.ano", "fonte_poluente": "$emissoes.fonte_poluente"},
            "total_emissao": {"$sum": "$emissoes.emissao"}
        }
    },
    {"$sort": {"_id.ano": 1, "_id.fonte_poluente": 1}},
    {"$project": {"ano": "$_id.ano", "fonte_poluente": "$_id.fonte_poluente", "total_emissao": 1, "_id": 0}}
]
print("\nConsulta 1: Tendência de Emissões por Fonte Poluente no Brasil (2000–2020)")
result1 = list(db.paises.aggregate(query1))
print(tabulate(result1, headers="keys", tablefmt="psql", floatfmt=".2f"))
pd.DataFrame(result1).to_csv("../consultas/resultados/query1.txt", index=False)

# Consulta 2: Top 5 Países por Emissões Totais em 2020
query2 = [
    {"$unwind": "$emissoes"},
    {"$match": {"emissoes.ano": 2020}},
    {
        "$group": {
            "_id": "$nome",
            "total_emissao": {"$sum": "$emissoes.emissao"}
        }
    },
    {"$sort": {"total_emissao": -1}},
    {"$limit": 5},
    {"$project": {"pais": "$_id", "total_emissao": 1, "_id": 0}}
]
print("\nConsulta 2: Top 5 Países por Emissões Totais em 2020")
result2 = list(db.paises.aggregate(query2))
print(tabulate(result2, headers="keys", tablefmt="psql", floatfmt=".2f"))
pd.DataFrame(result2).to_csv("../consultas/resultados/query2.txt", index=False)

# Consulta 3: Consumo de Energia Renovável vs. Não Renovável em 2020
query3 = [
    {"$unwind": "$energia"},
    {"$match": {"energia.ano": 2020}},
    {
        "$group": {
            "_id": "$nome",
            "consumo_renovavel": {
                "$sum": {
                    "$cond": [
                        {"$in": ["$energia.fonte_energia", ["Hydro", "Solar", "Wind", "Biofuel"]]},
                        "$energia.consumo",
                        0
                    ]
                }
            },
            "consumo_nao_renovavel": {
                "$sum": {
                    "$cond": [
                        {"$in": ["$energia.fonte_energia", ["Coal", "Oil", "Gas"]]},
                        "$energia.consumo",
                        0
                    ]
                }
            }
        }
    },
    {"$sort": {"consumo_renovavel": -1}},
    {"$limit": 10},
    {"$project": {"pais": "$_id", "consumo_renovavel": 1, "consumo_nao_renovavel": 1, "_id": 0}}
]
print("\nConsulta 3: Consumo de Energia Renovável vs. Não Renovável em 2020")
result3 = list(db.paises.aggregate(query3))
print(tabulate(result3, headers="keys", tablefmt="psql", floatfmt=".2f"))
pd.DataFrame(result3).to_csv("../consultas/resultados/query3.txt", index=False)

# Consulta 4: Emissões Totais por Região em 2020
query4 = [
    {"$unwind": "$emissoes"},
    {"$match": {"emissoes.ano": 2020}},
    {
        "$group": {
            "_id": "$regiao.nome",
            "total_emissao": {"$sum": "$emissoes.emissao"}
        }
    },
    {"$sort": {"total_emissao": -1}},
    {"$project": {"regiao": "$_id", "total_emissao": 1, "_id": 0}}
]
print("\nConsulta 4: Emissões Totais por Região em 2020")
result4 = list(db.paises.aggregate(query4))
print(tabulate(result4, headers="keys", tablefmt="psql", floatfmt=".2f"))
pd.DataFrame(result4).to_csv("../consultas/resultados/query4.txt", index=False)

# Consulta 5: Países com Maior Redução de Emissões (2010–2020)
query5 = [
    {"$unwind": "$emissoes"},
    {"$match": {"emissoes.ano": {"$in": [2010, 2020]}}},
    {
        "$group": {
            "_id": {"pais": "$nome", "ano": "$emissoes.ano"},
            "total_emissao": {"$sum": "$emissoes.emissao"}
        }
    },
    {
        "$group": {
            "_id": "$_id.pais",
            "emissoes": {
                "$push": {"ano": "$_id.ano", "total_emissao": "$total_emissao"}
            }
        }
    },
    {
        "$project": {
            "emissao_2010": {
                "$arrayElemAt": [
                    "$emissoes.total_emissao",
                    {"$indexOfArray": ["$emissoes.ano", 2010]}
                ]
            },
            "emissao_2020": {
                "$arrayElemAt": [
                    "$emissoes.total_emissao",
                    {"$indexOfArray": ["$emissoes.ano", 2020]}
                ]
            }
        }
    },
    {
        "$match": {
            "$expr": {"$lt": ["$emissao_2020", "$emissao_2010"]}
        }
    },
    {
        "$project": {
            "pais": "$_id",
            "emissao_2010": 1,
            "emissao_2020": 1,
            "reducao_emissao": {"$subtract": ["$emissao_2020", "$emissao_2010"]},
            "_id": 0
        }
    },
    {"$sort": {"reducao_emissao": 1}},
    {"$limit": 5}
]
print("\nConsulta 5: Países com Maior Redução de Emissões (2010–2020)")
result5 = list(db.paises.aggregate(query5))
print(tabulate(result5, headers="keys", tablefmt="psql", floatfmt=".2f"))
pd.DataFrame(result5).to_csv("../consultas/resultados/query5.txt", index=False)