# Olist Dataset Ingestion and Curation

## Descrição

Pipeline de dados automatizado para ingestão, limpeza e modelagem do histórico de vendas da Olist, seguindo a arquitetura Medalhão (Bronze, Silver, Gold) com Apache Airflow e PostgreSQL.

## Estrutura do Projeto

```
├── dags/
│   ├── ingestao_bronze.py
│   ├── processamento_silver.py
│   └── carga_gold.py
├── data/
│   └── [arquivos CSV extraídos do dataset Olist]
├── docker-compose.yaml
├── requirements.txt
└── README.md
```
## Estrutura das tabelas 

<img width="820" height="570" alt="image" src="https://github.com/user-attachments/assets/4d3bd242-82c3-4760-b40f-075f96443249" />

## Pré-requisitos

- Docker e Docker Compose
- Git
- Editor de código (VS Code recomendado)

## Setup do Ambiente

1. **Clone o repositório**
   ```bash
   git clone https://github.com/eduardo-eleuterio/Olist-dataset-ingestion-and-curation.git
   cd Olist-dataset-ingestion-and-curation
   ```

2. **Coloque os arquivos CSV na pasta `data/`**
   - Baixe o dataset Olist do Kaggle e extraia os 9 arquivos CSV para a pasta `data`.

3. **Configure o ambiente Docker**
   - O arquivo `docker-compose.yaml` já está pronto para subir Airflow e PostgreSQL.
   - Certifique-se que o volume da pasta `data` está mapeado:
     ```
     - ./data:/data
     ```

4. **Suba os containers**
   ```bash
   docker compose up
   ```

5. **Acesse o Airflow**
   - Interface: [http://localhost:8080](http://localhost:8080)
   - Login: airflow | Senha: airflow

## Execução do Pipeline

1. **Camada Bronze**
   - Execute a DAG `ingestao_bronze` no Airflow.
   - Ela irá ler os CSVs e criar tabelas brutas no schema `bronze` do PostgreSQL.

2. **Camada Silver**
   - A DAG `processamento_silver` será acionada automaticamente após a bronze.
   - Realiza limpeza, padronização e deduplicação, salvando no schema `silver`.

3. **Camada Gold**
   - Execute a DAG `carga_gold` para consolidar o Data Mart analítico no schema `gold`.

## Decisões e Justificativas

- **Idempotência:** As DAGs droppam e recriam tabelas para evitar duplicidade e garantir reprocessamento seguro.
- **Tratamento de Nulos:** Na silver, mantemos nulo em `order_delivered_customer_date` para pedidos não entregues, permitindo cálculos precisos de tempo de entrega apenas para pedidos concluídos.
- **Sanitização de CEP:** Usamos regex para garantir apenas 5 dígitos numéricos, evitando inconsistências.
- **Normalização de Categorias:** Padronização para caixa baixa e remoção de espaços, facilitando agrupamentos e análises.
- **Deduplicação:** Remoção de duplicatas em `olist_order_items_dataset` por `order_id` e `order_item_id` para garantir integridade.
- **Tipos de Dados:** Conversão explícita de datas e timestamps para garantir consistência e facilitar análises temporais.
- **Testes Automatizados:** Notebooks de teste para cada camada validam schema, nulos, duplicados e regras de negócio.

## Testes Automatizados e Análise Final

A pasta `test` contém notebooks de testes para as três camadas do pipeline:

- `test_bronze.ipynb`: Validação da camada Bronze (schema, nulos, duplicados).
- `test_silver.ipynb`: Testes de qualidade e integridade na camada Silver.
- `test_gold.ipynb`: Testes de regras de negócio e integridade na camada Gold.
- `analise_final.ipynb`: Notebook de análise SQL respondendo à pergunta de negócio sobre os clientes que mais gastaram em SP e suas categorias preferidas.

Esses notebooks garantem a qualidade dos dados em cada etapa e facilitam a validação dos resultados do pipeline.

## Como acessar o banco de dados PostgreSQL

Você pode acessar o banco de dados diretamente pelo terminal usando o comando abaixo:

1. Abra um terminal e execute o seguinte comando para acessar o container do PostgreSQL:
    ```bash
    docker compose exec postgres psql -U airflow -d airflow
    ```
    Isso abrirá o prompt do PostgreSQL dentro do container.

2. Para listar os schemas e tabelas:
    - Listar schemas:
       ```sql
       \dn
       ```
    - Listar tabelas de um schema (exemplo: bronze):
       ```sql
       \dt bronze.*
       ```
    - Sair do prompt:
       ```sql
       \q
       ```

## Resultado 
   - Query final:
       ```sql
       SELECT  customer_unique_id, total_gasto, categoria_mais_comprada FROM gold.dm_vendas_clientes WHERE estado_cliente = 'SP' ORDER BY total_gasto DESC LIMIT 10;
       ```
  <img width="566" height="234" alt="image" src="https://github.com/user-attachments/assets/ac856f4c-532d-488f-ba7c-e55e7f0da0a0" />


## Referências

- [Dataset Olist no Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- [Documentação Apache Airflow](https://airflow.apache.org/docs/)
- [Documentação PostgreSQL](https://www.postgresql.org/docs/)
