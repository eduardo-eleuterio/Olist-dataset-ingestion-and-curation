# Análise Final – Fase 4

## Query SQL utilizada

```sql
SELECT  customer_unique_id, 
        total_gasto, 
        categoria_mais_comprada
FROM gold.dm_vendas_clientes
WHERE estado_cliente = 'SP'
ORDER BY total_gasto DESC
LIMIT 10;
```

## Explicação dos Resultados

Esta query retorna os 10 clientes que mais gastaram no estado de São Paulo (SP), mostrando o valor total gasto e a categoria de produto preferida de cada um. O resultado é extraído da tabela analítica `dm_vendas_clientes` criada na camada Gold do pipeline, consolidando as métricas de valor e preferência por categoria.

Os dados podem ser utilizados pelo time de BI para identificar perfis de clientes, oportunidades de negócio e estratégias de marketing focadas nos consumidores de maior valor em SP.
