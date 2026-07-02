# DOCUMENTO DE ESPECIFICAÇÃO DE REGRAS DE NEGÓCIO

**Projeto:** Pipeline de Dados Mercado Livre Analytics
**Tecnologias:** Databricks Lakehouse, Apache Spark, Delta Lake
**Versão:** 1.0
**Classificação:** Uso Interno / Portfólio Técnico

---

## SUMÁRIO
1. Objetivo do Documento e Visão Geral
2. Catálogo de Tabelas do Sistema
3. Especificação de Atributos e Linhagem (Camada Gold)
   A. Tabela: gold.fato_vendas
   B. Tabela: gold.dim_clientes
   C. Tabela: gold.dim_produtos
4. Regras de Engenharia para Criação de Chaves (Surrogate Keys)
   A. Algoritmo Utilizado e Implementação Técnica

---

## 1. OBJETIVO DO DOCUMENTO E VISÃO GERAL

### Objetivo
[cite_start]O objetivo deste documento é estabelecer a governança de dados e a rastreabilidade do Data Lakehouse, servindo como a única fonte de verdade técnica e analítica do projeto[cite: 12]. Ele visa:
* [cite_start]**Garantir a Linhagem (Data Lineage):** Mapear com precisão o ciclo de vida do dado, desde a sua captura bruta na Landing Zone até a sua consolidação estruturada no modelo dimensional da Gold[cite: 14].
* [cite_start]**Facilitar o Consumo Analítico:** Documentar os tipos, chaves e significados das colunas para reduzir o tempo de descoberta (*Time-to-Discovery*) de analistas de BI, cientistas de dados e engenheiros[cite: 15].
* [cite_start]**Assegurar a Qualidade:** Servir como especificação de engenharia para auditorias, validação de esquemas e evolução controlada das tabelas Delta[cite: 16].

### Visão Geral
[cite_start]Este projeto demonstra a construção de um pipeline completo de Engenharia de Dados utilizando Databricks, Apache Spark e Delta Lake, realizando ingestão via API, tratamento sequencial nas camadas Bronze, Silver e Gold, modelagem dimensional (*Star Schema*) e disponibilização para consumo analítico de alta performance[cite: 18]. [cite_start]A adoção da arquitetura Medallion garante rastreabilidade, governança e evolução controlada de todo o ciclo de vida dos dados[cite: 22].

---

## 2. CATÁLOGO DE TABELAS DO SISTEMA

| Camada | Nome Físico da Tabela | Tipo de Armazenamento | Descrição / Objetivo de Negócio |
| :--- | :--- | :--- | :--- |
| **Bronze** | `bronze.mercado_livre_vendas` | Delta Lake | [cite_start]Armazena o payload bruto transacional de vendas com metadados de ingestão. |
| **Bronze** | `bronze.mercado_livre_produtos` | Delta Lake | [cite_start]Armazena o histórico cru de produtos capturados na API. |
| **Silver** | `silver.trusted_clientes` | Delta Lake | [cite_start]Tabela de transição com dados cadastrais higienizados e aplicação de hashes para chaves substitutas. |
| **Silver** | `silver.trusted_produtos` | Delta Lake | [cite_start]Catálogo limpo, desduplicado e com normalização de strings. |
| **Silver** | `silver.trusted_status_venda` | Delta Lake | [cite_start]Entidade isolada contendo o mapeamento de status únicos extraídos do pipeline. |
| **Silver** | `silver.trusted_calendario` | Delta Lake | [cite_start]Tabela de mapeamento cronológico e chaves temporais para análise de sazonalidade. |
| **Silver** | `silver.evento_vendas` | Delta Lake | [cite_start]Dados de vendas granularizados por item após o processo de EXPLODE. |
| **Gold** | `gold.dim_clientes` | Delta Lake | [cite_start]Dimensão mestre de compradores estruturada para relatórios analíticos (Star Schema). |
| **Gold** | `gold.dim_produtos` | Delta Lake | [cite_start]Dimensão mestre do catálogo de itens e preços unificados (Star Schema). |
| **Gold** | `gold.dim_status_venda` | Delta Lake | [cite_start]Dimensão categórica para isolamento e filtragem de status de pedidos (Star Schema). |
| **Gold** | `gold.dim_calendario` | Delta Lake | [cite_start]Dimensão tempo estruturada para suporte a inteligência temporal (Star Schema). |
| **Gold** | `gold.fato_vendas` | Delta Lake | [cite_start]Tabela fato centralizadora de métricas financeiras e volumétricas (Star Schema). |

---

## 3. ESPECIFICAÇÃO DE ATRIBUTOS E LINHAGEM (CAMADA GOLD)

### A. Tabela: `gold.fato_vendas`
> **Nota de Complexidade Técnica:** A granularidade original desta tabela na camada Bronze era de uma linha por transação unificada (pedido cheio). Com a aplicação do operador SQL `LATERAL VIEW explode` sobre o array de itens estruturados, a granularidade foi expandida de forma atômica para **uma linha por item contido no pedido**, garantindo a precisão dos cálculos de volumetria de vendas.

* **`id_venda`** (STRING)
  * *Descrição:* ID original da transação gerado pelo sistema transacional de origem[cite: 29].
  * [cite_start]*Linhagem:* `bronze.mercado_livre_vendas.id` [cite: 30]
* **`sk_calendario`** (INT)
  * *Descrição:* Chave estrangeira de relacionamento com a dimensão tempo (`dim_calendario`)[cite: 32].
  * [cite_start]*Linhagem:* Gerado na camada Silver a partir do tratamento da string `date_created`[cite: 33].
* **`id_produto`** (STRING)
  * *Descrição:* ID natural do produto vendido, limpo de impurezas de aspas e caracteres especiais[cite: 35].
  * [cite_start]*Linhagem:* Extraído do array interno `order_items` após desaninhamento estrutural[cite: 36].
* **`id_cliente`** (STRING)
  * *Descrição:* ID natural de identificação do comprador[cite: 38].
  * [cite_start]*Linhagem:* Extraído via função `get_json_object` mapeando o nó JSON `buyer.id`[cite: 39].
* **`sk_cliente`** (STRING)
  * *Descrição:* Surrogate Key do cliente usada para o relacionamento na Gold[cite: 41].
  * [cite_start]*Linhagem:* Conecta-se diretamente à chave primária de `gold.dim_clientes`[cite: 42].
* **`sk_status_venda`** (STRING)
  * [cite_start]*Descrição:* Surrogate Key de relacionamento com a dimensão de status[cite: 44].
  * [cite_start]*Linhagem:* Conecta-se diretamente à chave primária de `gold.dim_status_venda`[cite: 45].
* **`sk_produto`** (STRING)
  * *Descrição:* Surrogate Key de relacionamento com a dimensão de produtos[cite: 47].
  * [cite_start]*Linhagem:* Conecta-se diretamente à chave primária de `gold.dim_produtos`[cite: 48].
* **`quantidade_itens`** (INT)
  * *Descrição:* Quantidade volumétrica do produto específico adquirido na venda[cite: 50].
  * [cite_start]*Linhagem:* Mapeado do campo interno `quantity` dentro do JSON de itens da transação[cite: 51].
* **`valor_unitario`** (DOUBLE)
  * *Descrição:* Preço cobrado por uma unidade do produto no momento da compra[cite: 53].
  * [cite_start]*Linhagem:* Mapeado do campo interno `unit_price` dentro do JSON de itens[cite: 54].
* **`valor_total_venda`** (DOUBLE)
  * [cite_start]*Descrição:* Valor bruto consolidado da transação completa (pedido cheio)[cite: 56].
  * [cite_start]*Linhagem:* `bronze.mercado_livre_vendas.total_amount` [cite: 57]
* **`dh_insercao_gold`** (TIMESTAMP)
  * [cite_start]*Descrição:* Registro de auditoria técnica indicando o carimbo de data/hora da consolidação na Gold[cite: 59].
  * *Linhagem:* Gerado via função nativa do sistema `current_timestamp()`[cite: 60].

### B. Tabela: `gold.dim_clientes`
> **Nota de Complexidade Técnica:** Para simular conformidade com as diretrizes de privacidade de dados (LGPD) e tratar a integridade de payloads dinâmicos provenientes do simulador de API, foi aplicada uma camada de validação e mascaramento condicional de strings sensíveis antes da inserção dos registros na tabela final.

* [cite_start]**`sk_cliente`** (STRING): Chave substituta (Surrogate Key) para relacionamento do modelo estrela[cite: 63].
* **`id_cliente`** (STRING): ID natural do cliente originário do sistema transacional[cite: 64].
* **`nm_cliente`** (STRING): Nome ou Nickname do comprador, higienizado contra valores nulos[cite: 65].
* **`tipo_documento`** (STRING): Identificador fiscal cadastrado na plataforma (Ex: CPF/CNPJ)[cite: 66].
* **`estado_entrega`** (STRING): Unidade Federativa (UF) mapeada a partir do endereço de entrega do pedido[cite: 67].
* **`cidade_entrega`** (STRING): Município mapeado a partir do endereço de entrega do pedido[cite: 68].
* **`dh_insercao_gold`** (TIMESTAMP): Carimbo de data/hora indicando o momento da carga na Gold[cite: 69].

### C. Tabela: `gold.dim_produtos`
> **Nota de Complexidade Técnica:** Strings textuais extraídas diretamente da API externa continham aspas duplas embutidas nos identificadores primitivos (ex: `\"MLB148\"`). Foi aplicada uma rotina utilizando a função `REPLACE` combinada com funções de padronização de caixa (`UPPER` e `TRIM`) para normalizar os IDs de junção e evitar falhas de integridade referencial (*data mismatches*).

* [cite_start]**`sk_produto`** (STRING): Chave substituta (Surrogate Key) gerada via técnica de hash para o produto[cite: 72].
* **`id_produto`** (STRING): ID limpo, normalizado e padronizado do item (Ex: Sem aspas residuais)[cite: 73].
* **`preco_unitario`** (DOUBLE): Preço base ou de tabela capturado na extração[cite: 74].
* **`quantidade_itens`** (INT): Volume total do lote de inventário mapeado[cite: 75].
* **`origem_dados`** (STRING): Tag de auditoria que identifica qual API ou Microsserviço gerou a informação original[cite: 76].
* **`dh_ingestao_etl`** (TIMESTAMP): Data e hora exata em que o produto foi registrado na camada Bronze[cite: 77].
* [cite_start]**`dh_insercao_gold`** (TIMESTAMP): Data e hora exata do processamento e consolidação do modelo analítico na Gold[cite: 78].

---

## 4. REGRAS DE ENGENHARIA PARA CRIAÇÃO DE CHAVES (SURROGATE KEYS)

[cite_start]Na modelagem dimensional tradicional (Kimball), as Chaves Substitutas (*Surrogate Keys* ou `sk_`) eram geradas de forma incremental e sequencial utilizando indexadores numéricos do banco de dados relacional[cite: 80]. 

No entanto, em arquiteturas modernas de **Data Lakehouse distribuído (como o Databricks operando com Delta Lake)**, chaves auto-incrementais atuam como um ponto centralizado de bloqueio, exigindo comunicação constante entre os nós trabalhadores (*workers*) para decidir a próxima sequência. [cite_start]Esse comportamento gera gargalos graves de concorrência e quebra o paralelismo nativo do ecossistema Apache Spark[cite: 81].

[cite_start]Para solucionar essa limitação arquitetural e assegurar escalabilidade massiva, este projeto implementa a estratégia de **Deterministic Hash-based Surrogate Keys** (Chaves Substitutas Baseadas em Hash Determinístico)[cite: 82].

### A. Algoritmo Utilizado e Implementação Técnica
[cite_start]As chaves substitutas das tabelas dimensionais são construídas na camada Silver aplicando funções de hash criptográfico (como `MD5` ou `SHA2`) sobre a Chave Natural padronizada de cada entidade[cite: 84]. Essa abordagem assegura a **idempotência do pipeline**, permitindo cargas paralelas ou reprocessamentos completos sem o risco de gerar duplicidades, colisões estruturais ou chaves órfãs.

**Exemplo Técnico de Implementação em Spark SQL:**
```sql
-- Geração da SK de produto tratando impurezas textuais da origem
SELECT 
    MD5(UPPER(TRIM(REPLACE(id_item_referencia, '"', '')))) AS sk_produto
FROM bronze.mercado_livre_produtos;