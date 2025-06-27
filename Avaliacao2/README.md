# Justificativa para a Escolha do MongoDB (Cenário B)

O **Cenário B** requer um sistema de banco de dados que suporte o armazenamento de dados semi-estruturados, ofereça alta escalabilidade, esquema flexível e operações CRUD com baixa latência. Após avaliar as opções (MongoDB, DuckDB e Neo4J), o **MongoDB** foi selecionado como a melhor escolha pelos seguintes motivos:

- **Forma de Armazenamento de Arquivos**:  
  O MongoDB utiliza documentos BSON, perfeitos para dados semi-estruturados e esquemas dinâmicos. Isso permite adicionar novos campos sem a necessidade de migrações complexas, diferentemente do DuckDB (otimizado para dados colunares e análises) e do Neo4J (focado em grafos e relações complexas).

- **Linguagem e Processamento de Consultas**:  
  Com o Aggregation Framework, o MongoDB suporta consultas complexas e agregações eficientes, como cálculos de emissões por região ou tendências de consumo de energia. Além disso, sua integração com Python via `pymongo` simplifica a adaptação de consultas SQL da Atividade 1.

- **Processamento e Controle de Transações**:  
  Embora suporte transações ACID em réplicas, o MongoDB é otimizado para operações CRUD rápidas, atendendo à exigência de baixa latência do Cenário B. O DuckDB não é ideal para alta concorrência, enquanto o Neo4J prioriza integridade relacional, menos relevante aqui.

- **Mecanismos de Recuperação e Segurança**:  
  Recursos como replica sets e sharding garantem alta disponibilidade, tolerância a falhas e escalabilidade horizontal. Autenticação e criptografia TLS também atendem aos requisitos de segurança.

**Conclusão**: O MongoDB é a escolha ideal para o Cenário B devido à sua flexibilidade de esquema, alta performance em operações CRUD e escalabilidade robusta, superando DuckDB (mais adequado para análises, Cenário A) e Neo4J (focado em relações, Cenário C).

---

# Modelo Lógico (MongoDB)

O modelo lógico foi projetado para aproveitar a flexibilidade do MongoDB e otimizar operações CRUD, agregando dados relacionados em documentos completos.
![Texto alternativo](modelos/"Modelo Lógico".jpg)

## Coleções

1. **Paises**  
   - Armazena informações de países com dados embutidos de emissões, energia e indicadores econômicos em arrays.  
   - Exemplo:  
     ```json
     {
       "_id": "BRA",
       "nome": "Brazil",
       "regiao": { "regiao_code": "LAC", "nome": "Latin America and Caribbean" },
       "emissoes": [
         { "ano": 2020, "fonte_poluente": "Oil", "emissao": 123.45 },
         ...
       ],
       "energia": [
         { "ano": 2020, "fonte_energia": "Hydro", "consumo": 456.78 },
         ...
       ],
       "indicadores_economicos": [
         { "ano": 2020, "pce": 789.01 },
         ...
       ]
     }
     ```

2. **FontesPoluente**  
   - Contém informações sobre fontes poluentes.  
   - Exemplo:  
     ```json
     { "_id": 1, "nome": "Oil" }
     ```

3. **FontesEnergia**  
   - Armazena dados sobre fontes de energia.  
   - Exemplo:  
     ```json
     { "_id": 1, "nome": "Hydro" }
     ```

## Observações
- A coleção `Paises` reduz a necessidade de joins ao embutir dados relacionados em arrays, otimizando operações de leitura.  
- O esquema dinâmico do MongoDB permite adicionar novos campos (ex.: `populacao`) sem alterar a estrutura existente, garantindo a flexibilidade exigida pelo Cenário B.
