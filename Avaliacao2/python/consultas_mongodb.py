import pymongo
import pandas as pd
from tabulate import tabulate
import os

try:
    # Conectar ao MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["ods13_db"]
    
    # Diretório para resultados
    resultados_dir = "../consultas/resultados"
    consultas_dir = "../consultas"
    os.makedirs(resultados_dir, exist_ok=True)
    
    # Consulta 1: Percentual de emissões por fonte poluente globalmente em 2020
    query1 = [
        {"$unwind": "$emissoes"},
        {"$match": {"emissoes.ano": 2020}},
        {
            "$group": {
                "_id": "$emissoes.fonte_poluente_id",
                "total_emissao": {"$sum": "$emissoes.emissao"}
            }
        },
        {
            "$group": {
                "_id": None,
                "emissao_global": {"$sum": "$total_emissao"},
                "fontes": {
                    "$push": {
                        "fonte_poluente_id": "$_id",
                        "total_emissao": "$total_emissao"
                    }
                }
            }
        },
        {
            "$unwind": "$fontes"
        },
        {
            "$project": {
                "fonte_poluente_id": "$fontes.fonte_poluente_id",
                "percentual": {
                    "$multiply": [
                        {"$divide": ["$fontes.total_emissao", "$emissao_global"]},
                        100
                    ]
                },
                "_id": 0
            }
        },
        {"$sort": {"percentual": -1}}
    ]
    print("\nConsulta 1: Percentual de Emissões por Fonte Poluente Globalmente em 2020")
    result1 = list(db.paises.aggregate(query1))
    print(tabulate(result1, headers="keys", tablefmt="psql", floatfmt=".2f"))
    pd.DataFrame(result1).to_csv(f"{resultados_dir}/query1.txt", index=False)
    
    # Escrever relevância ODS 13
    with open(f"{consultas_dir}/query1.txt", "w", encoding="utf-8") as f:
        f.write("ODS 13.3 (Conscientização): Revela a contribuição percentual de cada fonte poluente para as emissões globais em 2020, informando ações de mitigação.")
    
    # Consulta 2: Países com maior crescimento percentual de emissões (2015–2020)
    query2 = [
        {"$unwind": "$emissoes"},
        {"$match": {"emissoes.ano": {"$in": [2015, 2020]}}},
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
                "emissao_2015": {
                    "$arrayElemAt": [
                        "$emissoes.total_emissao",
                        {"$indexOfArray": ["$emissoes.ano", 2015]}
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
                "$and": [
                    {"emissao_2015": {"$gt": 0}},
                    {"emissao_2020": {"$gt": 0}}
                ]
            }
        },
        {
            "$project": {
                "pais": "$_id",
                "crescimento_percentual": {
                    "$multiply": [
                        {"$divide": [
                            {"$subtract": ["$emissao_2020", "$emissao_2015"]},
                            "$emissao_2015"
                        ]},
                        100
                    ]
                },
                "_id": 0
            }
        },
        {"$sort": {"crescimento_percentual": -1}},
        {"$limit": 5}
    ]
    print("\nConsulta 2: Países com Maior Crescimento Percentual de Emissões (2015–2020)")
    result2 = list(db.paises.aggregate(query2))
    print(tabulate(result2, headers="keys", tablefmt="psql", floatfmt=".2f"))
    pd.DataFrame(result2).to_csv(f"{resultados_dir}/query2.txt", index=False)
    
    with open(f"{consultas_dir}/query2.txt", "w", encoding="utf-8") as f:
        f.write("ODS 13.2 (Mitigação): Identifica países com maior aumento percentual de emissões entre 2015 e 2020, priorizando intervenções climáticas.")
    
    # Consulta 3: Top 10 países com maior proporção de energia renovável em 2020
    query3 = [
        {"$unwind": "$energia"},
        {"$match": {"energia.ano": 2020}},
        {
            "$group": {
                "_id": "$nome",
                "consumo_renovavel": {
                    "$sum": {
                        "$cond": [
                            {"$in": ["$energia.fonte_energia_id", [1, 2, 3, 4]]},  # Assumindo IDs para Hydro, Solar, Wind, Biofuel
                            "$energia.consumo",
                            0
                        ]
                    }
                },
                "consumo_total": {"$sum": "$energia.consumo"}
            }
        },
        {
            "$match": {"consumo_total": {"$gt": 0}}
        },
        {
            "$project": {
                "pais": "$_id",
                "proporcao_renovavel": {
                    "$multiply": [
                        {"$divide": ["$consumo_renovavel", "$consumo_total"]},
                        100
                    ]
                },
                "_id": 0
            }
        },
        {"$sort": {"proporcao_renovavel": -1}},
        {"$limit": 10}
    ]
    print("\nConsulta 3: Top 10 Países com Maior Proporção de Energia Renovável em 2020")
    result3 = list(db.paises.aggregate(query3))
    print(tabulate(result3, headers="keys", tablefmt="psql", floatfmt=".2f"))
    pd.DataFrame(result3).to_csv(f"{resultados_dir}/query3.txt", index=False)
    
    with open(f"{consultas_dir}/query3.txt", "w", encoding="utf-8") as f:
        f.write("ODS 13.2 (Transição Energética): Destaca países com maior proporção de energia renovável em 2020, incentivando a adoção de fontes limpas.")
    
    # Consulta 4: Regiões com maior variabilidade de emissões (2010–2020)
    query4 = [
        {"$unwind": "$emissoes"},
        {"$match": {"emissoes.ano": {"$gte": 2010, "$lte": 2020}}},
        {
            "$group": {
                "_id": {"regiao": "$regiao.nome", "ano": "$emissoes.ano"},
                "total_emissao": {"$sum": "$emissoes.emissao"}
            }
        },
        {
            "$group": {
                "_id": "$_id.regiao",
                "emissoes": {"$push": "$total_emissao"},
                "media_emissao": {"$avg": "$total_emissao"}
            }
        },
        {
            "$project": {
                "regiao": "$_id",
                "desvio_padrao": {
                    "$sqrt": {
                        "$avg": {
                            "$map": {
                                "input": "$emissoes",
                                "as": "emissao",
                                "in": {
                                    "$pow": [
                                        {"$subtract": ["$$emissao", "$media_emissao"]},
                                        2
                                    ]
                                }
                            }
                        }
                    }
                },
                "_id": 0
            }
        },
        {"$sort": {"desvio_padrao": -1}}
    ]
    print("\nConsulta 4: Regiões com Maior Variabilidade de Emissões (2010–2020)")
    result4 = list(db.paises.aggregate(query4))
    print(tabulate(result4, headers="keys", tablefmt="psql", floatfmt=".2f"))
    pd.DataFrame(result4).to_csv(f"{resultados_dir}/query4.txt", index=False)
    
    with open(f"{consultas_dir}/query4.txt", "w", encoding="utf-8") as f:
        f.write("ODS 13.b (Planejamento): Identifica regiões com maior variabilidade de emissões entre 2010 e 2020, apoiando políticas climáticas estáveis.")
    
    # Consulta 5: Correlação entre emissões e indicadores econômicos em 2020 (top 5 países)
    query5 = [
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
        {
            "$lookup": {
                "from": "paises",
                "localField": "_id",
                "foreignField": "nome",
                "as": "pais_data"
            }
        },
        {"$unwind": "$pais_data"},
        {"$unwind": "$pais_data.indicadores_economicos"},
        {"$match": {"pais_data.indicadores_economicos.ano": 2020}},
        {
            "$project": {
                "pais": "$_id",
                "total_emissao": 1,
                "indicador_economico": "$pais_data.indicadores_economicos.pce",  # Assumindo pce; ajustar se necessário
                "_id": 0
            }
        }
    ]
    print("\nConsulta 5: Correlação entre Emissões e Indicadores Econômicos em 2020 (Top 5 Países)")
    result5 = list(db.paises.aggregate(query5))
    print(tabulate(result5, headers="keys", tablefmt="psql", floatfmt=".2f"))
    pd.DataFrame(result5).to_csv(f"{resultados_dir}/query5.txt", index=False)
    
    with open(f"{consultas_dir}/query5.txt", "w", encoding="utf-8") as f:
        f.write("ODS 13.3 (Conscientização): Relaciona emissões com indicadores econômicos em 2020 para os maiores emissores, destacando impactos econômicos do clima.")

except Exception as e:
    print(f"Erro geral: {e}")
