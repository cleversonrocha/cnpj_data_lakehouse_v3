# CNPJ Data Lakehouse v3

Projeto de engenharia de dados para ingestão e modelagem dos dados públicos de cadastro nacional de pessoas jurídicas da Receita Federal no Databricks.

O repositório usa **Databricks Asset Bundles**, **Lakeflow Declarative Pipelines**, **Databricks Workflows**, **PySpark**, **SQL** e **Pytest** para entregar uma arquitetura em camadas com modelo dimensional na camada Gold.

## Visão geral

```mermaid
flowchart LR
    A[Arquivos públicos da Receita Federal] --> B[Volume UC - CSV]
    B --> C[Bronze]
    C --> D[Silver]
    D --> E[Testes Pytest]
    E --> F[Intermediate]
    F --> G[Gold - Star Schema]
```

O fluxo completo é orquestrado pelo job `Pipeline Job`, definido em [resources/pipeline_job.yml](resources/pipeline_job.yml).

## Arquitetura do pipeline

| Etapa | Implementação | Resultado |
|---|---|---|
| Download | [src/download_data.ipynb](src/download_data.ipynb) | Download dos `.zip`, extração e gravação dos CSV em um Volume gerenciado |
| Bronze | [src/transformations/ingest_bronze_data.py](src/transformations/ingest_bronze_data.py) | Leitura incremental dos arquivos brutos com Auto Loader |
| Qualidade | [tests/test_silver_quality.py](tests/test_silver_quality.py) | Testes de unicidade e integridade referencial após a Silver |
| Silver | [src/transformations/ingest_silver_data.py](src/transformations/ingest_silver_data.py) | Limpeza, tipagem, normalização de datas e validações DLT |
| Intermediate | [src/transformations/ingest_intermediate_data.py](src/transformations/ingest_intermediate_data.py) | Tabelas intermediárias, dimensões auxiliares e chaves substitutas |
| Gold | [src/transformations/ingest_gold_data.sql](src/transformations/ingest_gold_data.sql) | Views materializadas, fato, dimensões, bridges e agregações |

### Fluxo de execução

```text
download_data
    -> ingest_bronze_data
    -> ingest_silver_data
    -> run_silver_tests
    -> ingest_intermediate_data
    -> ingest_gold_data
```

Os testes são executados depois da Silver e antes das tabelas Intermediate e Gold.

### Modelo analítico da camada Gold

- **Fato:** `fact_estabelecimentos`
- **Dimensões:** `dim_cnaes`, `dim_localidades`, `dim_naturezas_juridicas`, `dim_situacoes`, `dim_socios`, `dim_tempo_inicio_atividades`, `dim_tempo_situacoes_cadastrais` e `dim_tempo_situacoes_especiais`
- **Tabelas bridge:** `bridge_estabelecimentos_cnaes` e `bridge_estabelecimentos_socios`
- **Agregações:** `agg_fact_estabelecimentos` e `agg_fact_estabelecimentos_cnaes`

## Estrutura do repositório

```text
.
├── databricks.yml                 # Bundle, variáveis e targets
├── resources/
│   └── pipeline_job.yml           # Pipelines declarativos e workflow
├── src/
│   ├── download_data.ipynb        # Download e extração dos dados
│   └── transformations/
│       ├── ingest_bronze_data.py
│       ├── ingest_silver_data.py
│       ├── ingest_intermediate_data.py
│       └── ingest_gold_data.sql
├── tests/
│   └── test_silver_quality.py     # Unicidade e integridade referencial
└── .github/workflows/
    └── ci-cd.yml                  # Validação e deploy dos bundles
```

## Pré-requisitos

- Conta e workspace no Databricks
- Unity Catalog habilitado
- Databricks CLI configurada
- Python `3.12`
- [uv](https://docs.astral.sh/uv/) para gerenciamento do ambiente local
- Extensão Databricks para VS Code, opcional

### Recursos Unity Catalog

O projeto foi testado no Databricks Free Edition. Nesse ambiente, catálogos, schemas e volumes gerenciados precisam ser criados manualmente antes do primeiro deploy. Execute os comandos abaixo no SQL Editor do Databricks com uma identidade que tenha permissão para criar esses recursos.

| Target do bundle | Catálogo Unity Catalog | Schemas | Volume |
|---|---|---|---|
| `dev` | `dev_cnpj_data_lakehouse_v3` | `bronze`, `silver`, `gold` | `bronze.raw` |
| `stg` | `stg_cnpj_data_lakehouse_v3` | `bronze`, `silver`, `gold` | `bronze.raw` |
| `prd` | `prd_cnpj_data_lakehouse_v3` | `bronze`, `silver`, `gold` | `bronze.raw` |

Os nomes dos catálogos vêm da variável `catalog` de cada target em [databricks.yml](databricks.yml). O mesmo catálogo é usado pelos pipelines e pelos caminhos dos volumes, por exemplo `/Volumes/stg_cnpj_data_lakehouse_v3/bronze/raw/2026_10` no target `stg`.

Crie os recursos Unity Catalog para os três targets:

```sql
CREATE CATALOG IF NOT EXISTS dev_cnpj_data_lakehouse_v3;
CREATE SCHEMA IF NOT EXISTS dev_cnpj_data_lakehouse_v3.bronze;
CREATE SCHEMA IF NOT EXISTS dev_cnpj_data_lakehouse_v3.silver;
CREATE SCHEMA IF NOT EXISTS dev_cnpj_data_lakehouse_v3.gold;
CREATE VOLUME IF NOT EXISTS dev_cnpj_data_lakehouse_v3.bronze.raw;

CREATE CATALOG IF NOT EXISTS stg_cnpj_data_lakehouse_v3;
CREATE SCHEMA IF NOT EXISTS stg_cnpj_data_lakehouse_v3.bronze;
CREATE SCHEMA IF NOT EXISTS stg_cnpj_data_lakehouse_v3.silver;
CREATE SCHEMA IF NOT EXISTS stg_cnpj_data_lakehouse_v3.gold;
CREATE VOLUME IF NOT EXISTS stg_cnpj_data_lakehouse_v3.bronze.raw;

CREATE CATALOG IF NOT EXISTS prd_cnpj_data_lakehouse_v3;
CREATE SCHEMA IF NOT EXISTS prd_cnpj_data_lakehouse_v3.bronze;
CREATE SCHEMA IF NOT EXISTS prd_cnpj_data_lakehouse_v3.silver;
CREATE SCHEMA IF NOT EXISTS prd_cnpj_data_lakehouse_v3.gold;
CREATE VOLUME IF NOT EXISTS prd_cnpj_data_lakehouse_v3.bronze.raw;
```

## Configuração local

```bash
uv sync --dev
databricks configure
```

Para validar o ambiente sem alterar os recursos no workspace:

```bash
databricks bundle validate --target dev
```

## Deploy

O target padrão é `dev`. Os targets disponíveis são `dev`, `stg` e `prd`; os valores de catálogo e schema estão definidos em [databricks.yml](databricks.yml).

```bash
databricks bundle deploy --target dev
databricks bundle deploy --target stg
databricks bundle deploy --target prd
```

Para processar outra referência mensal da base:

```bash
databricks bundle deploy --target dev --var="year_month=2026_11" --var="target_dir=/2026-11"
```

## Executando o pipeline

```bash
databricks bundle run pipeline_job --target dev
```

Também é possível iniciar o `Pipeline Job` pela interface de Jobs & Pipelines no Databricks.

## Variáveis do bundle

| Variável | Descrição | Padrão |
|---|---|---|
| `catalog` | Catálogo Unity Catalog alvo | Definido pelo target |
| `schema` | Schema inicial | `bronze` |
| `year_month` | Nome da pasta mensal no Volume | `2026_10` |
| `target_dir` | Diretório mensal da fonte da Receita Federal | `/2026-10` |

## Testes

O arquivo [tests/test_silver_quality.py](tests/test_silver_quality.py) contém verificações de unicidade de chaves nas tabelas Bronze e Silver e de integridade referencial entre tabelas Silver, incluindo CNAEs secundários.

No Databricks, a task `run_silver_tests` usa `python_wheel_task` para chamar diretamente o entry point `pytest`, passando `tests/test_silver_quality.py` como arquivo de teste. Ela funciona como quality gate: Intermediate e Gold só executam depois que os testes passam.

Os testes montam o catálogo a partir da configuração Spark `spark.bundle.target`, formando `<target>_cnpj_data_lakehouse_v3`. O valor padrão no código é `dev`; para executar os testes nos targets `stg` ou `prd`, essa configuração precisa corresponder ao target usado.

```text
pytest --rootdir=. tests/test_silver_quality.py
```

## CI/CD

A esteira em [.github/workflows/ci-cd.yml](.github/workflows/ci-cd.yml) valida os três targets do bundle (`dev`, `stg` e `prd`) nos eventos configurados. Nomes de branches do Git e targets do bundle são diferentes:

- **Pull requests** direcionados a `develop`, `databricks` ou `main`: executam a validação do bundle.
- **Push em `develop`**: valida o bundle, faz deploy no target `dev` e abre um PR de `develop` para `staging` caso ainda não exista um PR aberto.
- **Push em `staging`** (incluindo merge do PR de promoção): valida o bundle e faz deploy no target `stg`.
- **Push em `main`**: valida o bundle e faz deploy no target `prd`.

No workflow atual, o gatilho `pull_request` não inclui a branch `staging`; assim, o PR automático de `develop` para `staging` não dispara essa validação até que `staging` seja adicionada à lista de branches do gatilho.

Para deploy manual, use os targets do bundle, não os nomes das branches:

```bash
databricks bundle deploy --target dev
databricks bundle deploy --target stg
databricks bundle deploy --target prd
```

A esteira precisa dos secrets `DATABRICKS_HOST` e `DATABRICKS_TOKEN` configurados no repositório GitHub.
Como o workflow abre o PR de promoção automaticamente, habilite também **Settings → Actions → General → Allow GitHub Actions to create and approve pull requests**.

### Transparência sobre o uso de IA

Ferramentas de IA generativa foram utilizadas como apoio na revisão e atualização desta documentação. As descrições foram conferidas com os arquivos de configuração e código do repositório; as decisões e alterações do projeto permanecem sob responsabilidade dos mantenedores.

### Bloquear push direto em `main` e `staging`

O workflow promove código para `staging` por meio de um pull request; ele não faz push direto nessa branch. Para exigir PRs também de pessoas e impedir bypass:

1. Abra o repositório no GitHub e vá para **Settings → Rules → Rulesets** (ou **Settings → Branches** para regras clássicas).
2. Crie uma regra para `main` e outra para `staging` (ou uma ruleset que tenha ambas como alvo).
3. Ative **Require a pull request before merging**. Para proteção adicional, exija pelo menos uma aprovação e o check `ci-validation`.
4. Ative a aplicação da regra a administradores / **Do not allow bypassing the above settings** e deixe a lista de bypass vazia.
5. Mantenha bloqueados force-push e exclusão da branch.

Com isso, atualizações de `main` e `staging` devem entrar por PR aprovado e com o check obrigatório concluído. A criação do PR de promoção para `staging` usa o `GITHUB_TOKEN` padrão da Action, com permissão `pull-requests: write`; o merge continua sujeito à regra configurada no GitHub. A conta que executa o merge precisa ter permissão de escrita no repositório.

## Solução de problemas

- **Catálogo ou schema não existe:** confira a correspondência entre target e catálogo na tabela acima e crie os recursos do target antes do deploy.
- **Volume não encontrado:** confirme que `bronze.raw` existe no catálogo associado ao target utilizado.
- **Nenhum arquivo baixado:** confirme se `target_dir` corresponde a uma pasta válida da fonte pública.
- **Arquivos antigos são reutilizados:** remova ou ajuste o conteúdo mensal no Volume quando houver necessidade de reprocessar a mesma referência.
- **Falha nos testes de qualidade:** verifique o resultado da task `run_silver_tests` e os exemplos de chaves duplicadas ou referências órfãs no log.
