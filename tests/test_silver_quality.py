import logging
import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import pytest

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


@pytest.fixture(scope="session")
def catalog(spark) -> str:
    target = spark.conf.get("spark.bundle.target", "dev")
    return f"{target}_cnpj_data_lakehouse_v3"


# ==============================================================================
# 1. TESTES DE UNICIDADE (BRONZE E SILVER)
# ==============================================================================


@pytest.mark.parametrize(
    "tabela,colunas_pk",
    [
        # Bronze
        ("bronze.estabelecimentos", ["_c0", "_c1", "_c2"]),
        # Silver - Tabelas de Domínio
        ("silver.motivos", ["codigo"]),
        ("silver.qualificacoes", ["codigo"]),
        ("silver.naturezas", ["codigo"]),
        ("silver.paises", ["codigo"]),
        ("silver.cnaes", ["codigo"]),
        ("silver.municipios", ["codigo"]),
        # Silver - Tabelas Principais
        ("silver.empresas", ["cnpj_basico"]),
        ("silver.simples", ["cnpj_basico"]),
        ("silver.estabelecimentos", ["cnpj_basico", "cnpj_ordem", "cnpj_dv"]),
    ],
)
def test_unicidade_chaves_primarias(spark, catalog, tabela, colunas_pk):
    full_table = f"{catalog}.{tabela}"
    df = spark.read.table(full_table)

    duplicados = df.groupBy(*colunas_pk).agg(F.count("*").alias("qtd")).filter(F.col("qtd") > 1).limit(5)
    amostra_duplicados = [row.asDict() for row in duplicados.collect()]

    logger.info(f"[Unicidade] Verificação concluída em {full_table} para chave {colunas_pk}")

    assert len(amostra_duplicados) == 0, (
        f"Existem chaves duplicadas na tabela {full_table} ({colunas_pk}). Exemplos: {amostra_duplicados}"
    )


# ==============================================================================
# 2. TESTES DE INTEGRIDADE REFERENCIAL (SILVER)
# ==============================================================================


@pytest.mark.parametrize(
    "tabela_filha,coluna_fk,tabela_pai,coluna_pk",
    [
        # Relacionamentos de Estabelecimentos
        ("silver.estabelecimentos", "motivo_situacao_cadastral", "silver.motivos", "codigo"),
        ("silver.estabelecimentos", "pais", "silver.paises", "codigo"),
        ("silver.estabelecimentos", "cnae_fiscal_principal", "silver.cnaes", "codigo"),
        ("silver.estabelecimentos", "municipio", "silver.municipios", "codigo"),
        ("silver.estabelecimentos", "cnpj_basico", "silver.empresas", "cnpj_basico"),
        # Relacionamentos de Empresas
        ("silver.empresas", "natureza_juridica", "silver.naturezas", "codigo"),
        ("silver.empresas", "qualificacao_responsavel", "silver.qualificacoes", "codigo"),
        # Relacionamentos de Sócios
        ("silver.socios", "cnpj_basico", "silver.empresas", "cnpj_basico"),
        ("silver.socios", "qualificacao", "silver.qualificacoes", "codigo"),
        ("silver.socios", "pais", "silver.paises", "codigo"),
        ("silver.socios", "qualificacao_representante", "silver.qualificacoes", "codigo"),
        # Relacionamentos de Simples
        ("silver.simples", "cnpj_basico", "silver.empresas", "cnpj_basico"),
    ],
)
def test_integridade_referencial_silver(spark, catalog, tabela_filha, coluna_fk, tabela_pai, coluna_pk):
    full_filha = f"{catalog}.{tabela_filha}"
    full_pai = f"{catalog}.{tabela_pai}"

    df_filha = (
        spark.read.table(full_filha).select(F.col(coluna_fk).alias("fk")).filter(F.col("fk").isNotNull()).distinct()
    )
    df_pai = spark.read.table(full_pai).select(F.col(coluna_pk).alias("pk"))

    orfaos = df_filha.join(df_pai, df_filha["fk"] == df_pai["pk"], how="left_anti").limit(5)
    lista_orfaos = [row["fk"] for row in orfaos.collect()]

    logger.info(f"[Integridade Referencial] {full_filha}.{coluna_fk} -> {full_pai}.{coluna_pk} concluída")

    assert len(lista_orfaos) == 0, (
        f"Chaves órfãs encontradas em {full_filha}.{coluna_fk} sem correspondência em "
        f"{full_pai}.{coluna_pk}. Exemplos: {lista_orfaos}"
    )


def test_integridade_cnae_secundaria(spark, catalog):
    full_estab = f"{catalog}.silver.estabelecimentos"
    full_cnaes = f"{catalog}.silver.cnaes"

    df_cnaes_explodidos = (
        spark.read.table(full_estab)
        .select("cnae_fiscal_secundaria")
        .filter(F.col("cnae_fiscal_secundaria").isNotNull())
        .select(F.explode(F.split(F.col("cnae_fiscal_secundaria"), ",")).alias("cnae"))
        .distinct()
    )
    df_cnaes_dominio = spark.read.table(full_cnaes).select(F.col("codigo").alias("pk"))

    orfaos = df_cnaes_explodidos.join(
        df_cnaes_dominio, df_cnaes_explodidos["cnae"] == df_cnaes_dominio["pk"], how="left_anti"
    ).limit(5)
    lista_orfaos = [row["cnae"] for row in orfaos.collect()]

    logger.info(f"[Integridade Referencial] {full_estab}.cnae_fiscal_secundaria -> {full_cnaes}.codigo concluída")

    assert len(lista_orfaos) == 0, f"CNAEs secundários órfãos encontrados em {full_estab}: {lista_orfaos}"
