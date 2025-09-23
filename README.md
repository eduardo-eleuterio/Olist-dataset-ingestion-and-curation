# Olist Dataset Ingestion and Curation

## Descrição

Pipeline de dados automatizado para ingestão, limpeza e modelagem do histórico de vendas da Olist, seguindo a arquitetura Medalhão (Bronze, Silver, Gold) com Apache Airflow e PostgreSQL.

## Estrutura do Projeto

```
├── dags/
│   ├── ingestao_bronze.py
│   └── processamento_silver.py
├── data/
│   └── [arquivos CSV extraídos do dataset Olist]
├── docker-compose.yaml
├── requirements.txt
└── README.md
```

## Pré-requisitos

- Docker e Docker Compose
- Git
- Editor de código (VS Code recomendado)

## Setup do Ambiente

1. **Clone o repositório**
   ```bash
   git clone <seu-repo>
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
   - (Implemente a DAG `carga_gold.py` conforme o desafio para consolidar o Data Mart analítico.)

## Decisões e Justificativas

- **Idempotência:** As DAGs droppam e recriam tabelas para evitar duplicidade.
- **Tratamento de Nulos:** Na silver, mantemos nulo em `order_delivered_customer_date` para pedidos não entregues, permitindo cálculos precisos de tempo de entrega apenas para pedidos concluídos.
- **Sanitização de CEP:** Usamos regex para garantir apenas 5 dígitos numéricos.
- **Normalização de Categorias:** Padronização para caixa baixa e remoção de espaços.
- **Deduplicação:** Remoção de duplicatas em `olist_order_items_dataset` por `order_id` e `order_item_id`.


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

## Como validar

- Verifique as tabelas nos schemas `bronze` e `silver` do PostgreSQL.
- Consulte os logs das DAGs no Airflow para eventuais erros.

## Referências

- [Dataset Olist no Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- [Documentação Apache Airflow](https://airflow.apache.org/docs/)
- [Documentação PostgreSQL](https://www.postgresql.org/docs/)
