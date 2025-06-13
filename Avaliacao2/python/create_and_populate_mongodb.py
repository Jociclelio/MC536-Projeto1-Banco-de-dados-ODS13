import pymongo
import pandas as pd
import os

try:
    # Passo 1: Conectar ao MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["ods13_db"]
    client.drop_database("ods13_db")
    if "ods13_db" not in client.list_database_names():
        print("Conexão com MongoDB estabelecida e banco ods13_db preparado com sucesso!")
    else:
        print("Erro: Banco ods13_db não foi limpo corretamente!")
    
    # Passo 2: Carregar os datasets
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(BASE_DIR, "../../Avaliacao1/dados-pre-processados")
    datasets = {
        "../datasets/pip.csv": None,
        "../datasets/owid-co2-data.csv": None,
        "atividades_energia.csv": None,
        "emissao_poluentes.csv": None,
        "indicadores_economicos.csv": None
    }
    
    for file_name in datasets:
        file_path = os.path.join(data_dir, file_name)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Arquivo {file_path} não encontrado!")
        try:
            datasets[file_name] = pd.read_csv(file_path)
            print(f"Arquivo {file_name} carregado com sucesso! ({len(datasets[file_name])} linhas)")
        except Exception as e:
            raise Exception(f"Erro ao carregar {file_name}: {e}")
    
    # Passo 3: Popular a coleção FontesPoluente
    poluentes_df = datasets["emissao_poluentes.csv"]
    fontes_poluente = poluentes_df[["fonte_poluente_id"]].drop_duplicates()
    fontes_poluente["nome"] = fontes_poluente["fonte_poluente_id"].astype(str)  # Usar ID como nome temporário
    fontes_poluente_dict = [
        {"_id": row["fonte_poluente_id"], "nome": row["nome"]}
        for _, row in fontes_poluente.iterrows()
    ]
    db.fontes_poluente.insert_many(fontes_poluente_dict)
    print(f"Coleção fontes_poluente populada com {len(fontes_poluente_dict)} documentos.")
    
    # Passo 4: Popular a coleção FontesEnergia
    energy_df = datasets["atividades_energia.csv"]
    fontes_energia = energy_df[["fonte_energia_id"]].drop_duplicates()
    fontes_energia["nome"] = fontes_energia["fonte_energia_id"].astype(str)  # Usar ID como nome temporário
    fontes_energia_dict = [
        {"_id": row["fonte_energia_id"], "nome": row["nome"]}
        for _, row in fontes_energia.iterrows()
    ]
    db.fontes_energia.insert_many(fontes_energia_dict)
    print(f"Coleção fontes_energia populada com {len(fontes_energia_dict)} documentos.")
    
    # Passo 5: Inicializar a coleção Paises a partir de pip.csv
    pip_df = datasets["../datasets/pip.csv"]
    paises = pip_df[["country_code", "country_name", "region_code", "region_name"]].drop_duplicates()
    paises_dict = {}
    for _, row in paises.iterrows():
        iso_code = row["country_code"]
        paises_dict[iso_code] = {
            "_id": iso_code,
            "nome": row["country_name"],
            "regiao": {
                "regiao_code": row["region_code"] if pd.notna(row["region_code"]) else "WLD",
                "nome": row["region_name"] if pd.notna(row["region_name"]) else "World"
            },
            "emissoes": [],
            "energia": [],
            "indicadores_economicos": []
        }
    print(f"Inicializado {len(paises_dict)} países a partir de pip.csv.")
    
    # Passo 6: Complementar Paises com outros datasets
    for df in [datasets["../datasets/owid-co2-data.csv"], energy_df, poluentes_df, datasets["indicadores_economicos.csv"]]:
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
    print(f"Total de países após complementação: {len(paises_dict)}.")
    
    # Passo 7: Popular array emissoes em Paises
    for _, row in poluentes_df.iterrows():
        iso_code = row["iso_code"]
        if iso_code in paises_dict:
            paises_dict[iso_code]["emissoes"].append({
                "ano": int(row["ano"]),
                "fonte_poluente_id": row["fonte_poluente_id"],
                "emissao": float(row["emissao"])
            })
    print("Array emissoes populado para os países.")
    
    # Passo 8: Popular array energia em Paises
    for _, row in energy_df.iterrows():
        iso_code = row["iso_code"]
        if iso_code in paises_dict:
            paises_dict[iso_code]["energia"].append({
                "ano": int(row["ano"]),
                "fonte_energia_id": row["fonte_energia_id"],
                "consumo": float(row["consumo"])
            })
    print("Array energia populado para os países.")
    
    # Passo 9: Popular array indicadores_economicos em Paises
    econ_df = datasets["indicadores_economicos.csv"]
    indicadores_cols = [col for col in econ_df.columns if col not in ["iso_code", "ano"]]
    for _, row in econ_df.iterrows():
        iso_code = row["iso_code"]
        if iso_code in paises_dict:
            indicadores = {"ano": int(row["ano"])}
            for col in indicadores_cols:
                if pd.notna(row[col]):
                    indicadores[col] = row[col]
            paises_dict[iso_code]["indicadores_economicos"].append(indicadores)
    print("Array indicadores_economicos populado para os países.")
    
    # Passo 10: Inserir documentos na coleção Paises
    db.paises.insert_many(list(paises_dict.values()))
    print(f"Coleção paises populada com {len(paises_dict)} documentos.")
    
    # Passo 11: Testar e Validar o Banco
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

except Exception as e:
    print(f"Erro geral: {e}")
