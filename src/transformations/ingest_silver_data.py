from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="motivos",
    comment="Tabela de domínio - Motivos",
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("codigo", "codigo IS NOT NULL")
@dp.expect_or_fail("descricao", "descricao IS NOT NULL")
def motivos():
    return spark.read.table("bronze.motivos").select(
        F.nullif(F.trim(F.col("_c0")), F.lit("")).cast("tinyint").alias("codigo"),
        F.nullif(F.trim(F.col("_c1")), F.lit("")).cast("string").alias("descricao"),
    )


@dp.table(
    name="qualificacoes",
    comment="Tabela de domínio - Qualificacoes",
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("codigo", "codigo IS NOT NULL")
@dp.expect_or_fail("descricao", "descricao IS NOT NULL")
def qualificacoes():
    return spark.read.table("bronze.qualificacoes").select(
        F.nullif(F.trim(F.col("_c0")), F.lit("")).cast("tinyint").alias("codigo"),
        F.nullif(F.trim(F.col("_c1")), F.lit("")).cast("string").alias("descricao"),
    )


@dp.table(
    name="naturezas",
    comment="Tabela de domínio - Naturezas",
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("codigo", "codigo IS NOT NULL")
@dp.expect_or_fail("descricao", "descricao IS NOT NULL")
def naturezas():
    return spark.read.table("bronze.naturezas").select(
        F.nullif(F.trim(F.col("_c0")), F.lit("")).cast("smallint").alias("codigo"),
        F.nullif(F.trim(F.col("_c1")), F.lit("")).cast("string").alias("descricao"),
    )


@dp.table(
    name="paises",
    comment="Tabela de domínio - Paises",
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("codigo", "codigo IS NOT NULL")
@dp.expect_or_fail("descricao", "descricao IS NOT NULL")
def paises():
    return spark.read.table("bronze.paises").select(
        F.nullif(F.trim(F.col("_c0")), F.lit("")).cast("smallint").alias("codigo"),
        F.nullif(F.trim(F.col("_c1")), F.lit("")).cast("string").alias("descricao"),
    )


@dp.table(
    name="cnaes",
    comment="Tabela de domínio - CNAEs",
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("codigo", "codigo IS NOT NULL")
@dp.expect_or_fail("descricao", "descricao IS NOT NULL")
def cnaes():
    return spark.read.table("bronze.cnaes").select(
        F.nullif(F.trim(F.col("_c0")), F.lit("")).cast("string").alias("codigo"),
        F.nullif(F.trim(F.col("_c1")), F.lit("")).cast("string").alias("descricao"),
    )


@dp.table(
    name="municipios",
    comment="Tabela de domínio - Municipios",
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("codigo", "codigo IS NOT NULL")
@dp.expect_or_fail("descricao", "descricao IS NOT NULL")
def municipios():
    return spark.read.table("bronze.municipios").select(
        F.nullif(F.trim(F.col("_c0")), F.lit("")).cast("smallint").alias("codigo"),
        F.nullif(F.trim(F.col("_c1")), F.lit("")).cast("string").alias("descricao"),
    )


@dp.table(
    name="socios",
    comment="Tabela de domínio - Socios",
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("cnpj_basico", "cnpj_basico IS NOT NULL")
@dp.expect_or_fail("identificador", "identificador IS NOT NULL")
@dp.expect("nome_razao_social", "nome_razao_social IS NOT NULL")
@dp.expect("cpf_cnpj", "cpf_cnpj IS NOT NULL")
@dp.expect_or_fail("qualificacao", "qualificacao IS NOT NULL")
@dp.expect_or_fail("data_entrada_sociedade", "data_entrada_sociedade IS NOT NULL")
@dp.expect("pais", "pais IS NOT NULL")
@dp.expect_or_fail("representante_legal", "representante_legal IS NOT NULL")
@dp.expect("nome_do_representante", "nome_do_representante IS NOT NULL")
@dp.expect_or_fail("qualificacao_representante", "qualificacao_representante IS NOT NULL")
@dp.expect_or_fail("faixa_etaria_socio", "faixa_etaria_socio IS NOT NULL")
def socios():
    return spark.read.table("bronze.socios").select(
        F.nullif(F.trim(F.col("_c0")), F.lit("")).cast("string").alias("cnpj_basico"),
        F.nullif(F.trim(F.col("_c1")), F.lit("")).cast("tinyint").alias("identificador"),
        F.nullif(F.trim(F.col("_c2")), F.lit("")).cast("string").alias("nome_razao_social"),
        F.nullif(F.trim(F.col("_c3")), F.lit("")).cast("string").alias("cpf_cnpj"),
        F.nullif(F.trim(F.col("_c4")), F.lit("")).cast("tinyint").alias("qualificacao"),
        F.try_to_date(F.nullif(F.trim(F.col("_c5")), F.lit("")), "yyyyMMdd").alias("data_entrada_sociedade"),
        F.nullif(F.trim(F.col("_c6")), F.lit("")).cast("smallint").alias("pais"),
        F.nullif(F.trim(F.col("_c7")), F.lit("")).cast("string").alias("representante_legal"),
        F.nullif(F.trim(F.col("_c8")), F.lit("")).cast("string").alias("nome_do_representante"),
        F.nullif(F.trim(F.col("_c9")), F.lit("")).cast("tinyint").alias("qualificacao_representante"),
        F.nullif(F.trim(F.col("_c10")), F.lit("")).cast("tinyint").alias("faixa_etaria_socio"),
    )


@dp.table(
    name="simples",
    comment="Tabela de domínio - Simples",
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("cnpj_basico", "cnpj_basico IS NOT NULL")
@dp.expect_or_fail("opcao_simples", "opcao_simples IS NOT NULL")
@dp.expect_or_fail("data_opcao_simples", "data_opcao_simples IS NOT NULL")
@dp.expect("data_exclusao_simples", "data_exclusao_simples IS NOT NULL")
@dp.expect_or_fail("opcao_mei", "opcao_mei IS NOT NULL")
@dp.expect("data_opcao_mei", "data_opcao_mei IS NOT NULL")
@dp.expect("data_exclusao_mei", "data_exclusao_mei IS NOT NULL")
def simples():
    return spark.read.table("bronze.simples").select(
        F.nullif(F.trim(F.col("_c0")), F.lit("")).cast("string").alias("cnpj_basico"),
        F.nullif(F.trim(F.col("_c1")), F.lit("")).cast("string").alias("opcao_simples"),
        F.try_to_date(F.nullif(F.trim(F.col("_c2")), F.lit("")), "yyyyMMdd").alias("data_opcao_simples"),
        F.try_to_date(F.nullif(F.trim(F.col("_c3")), F.lit("")), "yyyyMMdd").alias("data_exclusao_simples"),
        F.nullif(F.trim(F.col("_c4")), F.lit("")).cast("string").alias("opcao_mei"),
        F.try_to_date(F.nullif(F.trim(F.col("_c5")), F.lit("")), "yyyyMMdd").alias("data_opcao_mei"),
        F.try_to_date(F.nullif(F.trim(F.col("_c6")), F.lit("")), "yyyyMMdd").alias("data_exclusao_mei"),
    )


@dp.table(
    name="empresas",
    comment="Tabela de domínio - Empresas",
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("cnpj_basico", "cnpj_basico IS NOT NULL")
@dp.expect("razao_social", "razao_social IS NOT NULL")
@dp.expect_or_fail("natureza_juridica", "natureza_juridica IS NOT NULL")
@dp.expect_or_fail("qualificacao_responsavel", "qualificacao_responsavel IS NOT NULL")
@dp.expect_or_fail("capital_social", "capital_social IS NOT NULL")
@dp.expect("porte_empresa", "porte_empresa IS NOT NULL")
@dp.expect("ente_federativo_responsavel", "ente_federativo_responsavel IS NOT NULL")
def empresas():
    return spark.read.table("bronze.empresas").select(
        F.nullif(F.trim(F.col("_c0")), F.lit("")).cast("string").alias("cnpj_basico"),
        F.nullif(F.trim(F.col("_c1")), F.lit("")).cast("string").alias("razao_social"),
        F.nullif(F.trim(F.col("_c2")), F.lit("")).cast("smallint").alias("natureza_juridica"),
        F.nullif(F.trim(F.col("_c3")), F.lit("")).cast("tinyint").alias("qualificacao_responsavel"),
        F.expr("try_cast(nullif(replace(trim(_c4), ',', '.'), '') as decimal(15, 2))").alias("capital_social"),
        F.nullif(F.trim(F.col("_c5")), F.lit("")).cast("tinyint").alias("porte_empresa"),
        F.nullif(F.trim(F.col("_c6")), F.lit("")).cast("string").alias("ente_federativo_responsavel"),
    )


@dp.table(
    name="estabelecimentos", comment="Tabela de domínio - Estabelecimentos", table_properties={"quality": "silver"}
)
@dp.expect_or_fail("cnpj_basico", "cnpj_basico IS NOT NULL")
@dp.expect_or_fail("cnpj_ordem", "cnpj_ordem IS NOT NULL")
@dp.expect_or_fail("cnpj_dv", "cnpj_dv IS NOT NULL")
@dp.expect_or_fail("identificador_matriz_filial", "identificador_matriz_filial IS NOT NULL")
@dp.expect("nome_fantasia", "nome_fantasia IS NOT NULL")
@dp.expect_or_fail("situacao_cadastral", "situacao_cadastral IS NOT NULL")
@dp.expect("data_situacao_cadastral", "data_situacao_cadastral IS NOT NULL")
@dp.expect_or_fail("motivo_situacao_cadastral", "motivo_situacao_cadastral IS NOT NULL")
@dp.expect("cidade_exterior", "cidade_exterior IS NOT NULL")
@dp.expect("pais", "pais IS NOT NULL")
@dp.expect_or_fail("data_inicio_atividade", "data_inicio_atividade IS NOT NULL")
@dp.expect_or_fail("cnae_fiscal_principal", "cnae_fiscal_principal IS NOT NULL")
@dp.expect("tipo_logradouro", "tipo_logradouro IS NOT NULL")
@dp.expect_or_fail("logradouro", "logradouro IS NOT NULL")
@dp.expect_or_fail("numero", "numero IS NOT NULL")
@dp.expect("complemento", "complemento IS NOT NULL")
@dp.expect_or_fail("bairro", "bairro IS NOT NULL")
@dp.expect("cep", "cep IS NOT NULL")
@dp.expect_or_fail("uf", "uf IS NOT NULL")
@dp.expect_or_fail("municipio", "municipio IS NOT NULL")
@dp.expect("ddd_1", "ddd_1 IS NOT NULL")
@dp.expect("telefone_1", "telefone_1 IS NOT NULL")
@dp.expect("ddd_2", "ddd_2 IS NOT NULL")
@dp.expect("telefone_2", "telefone_2 IS NOT NULL")
@dp.expect("ddd_fax", "ddd_fax IS NOT NULL")
@dp.expect("fax", "fax IS NOT NULL")
@dp.expect("email", "email IS NOT NULL")
@dp.expect("situacao_especial", "situacao_especial IS NOT NULL")
@dp.expect("data_situacao_especial", "data_situacao_especial IS NOT NULL")
def estabelecimentos():
    return spark.read.table("bronze.estabelecimentos").select(
        F.nullif(F.trim(F.col("_c0")), F.lit("")).cast("string").alias("cnpj_basico"),
        F.nullif(F.trim(F.col("_c1")), F.lit("")).cast("string").alias("cnpj_ordem"),
        F.nullif(F.trim(F.col("_c2")), F.lit("")).cast("string").alias("cnpj_dv"),
        F.nullif(F.trim(F.col("_c3")), F.lit("")).cast("tinyint").alias("identificador_matriz_filial"),
        F.nullif(F.trim(F.col("_c4")), F.lit("")).cast("string").alias("nome_fantasia"),
        F.expr("try_cast(nullif(trim(_c5), '') as tinyint)").alias("situacao_cadastral"),
        F.expr("try_to_date(nullif(trim(_c6), ''), 'yyyyMMdd')").alias("data_situacao_cadastral"),
        F.expr("try_cast(nullif(trim(_c7), '') as tinyint)").alias("motivo_situacao_cadastral"),
        F.nullif(F.trim(F.col("_c8")), F.lit("")).cast("string").alias("cidade_exterior"),
        F.expr("try_cast(nullif(trim(_c9), '') as smallint)").alias("pais"),
        F.expr("try_to_date(nullif(trim(_c10), ''), 'yyyyMMdd')").alias("data_inicio_atividade"),
        F.nullif(F.trim(F.col("_c11")), F.lit("")).cast("string").alias("cnae_fiscal_principal"),
        F.nullif(
            F.array_join(
                F.array_distinct(
                    F.filter(F.transform(F.split(F.col("_c12"), ","), lambda x: F.trim(x)), lambda x: x != "")
                ),
                ",",
            ),
            F.lit(""),
        ).alias("cnae_fiscal_secundaria"),
        F.nullif(F.trim(F.col("_c13")), F.lit("")).cast("string").alias("tipo_logradouro"),
        F.nullif(F.trim(F.col("_c14")), F.lit("")).cast("string").alias("logradouro"),
        F.nullif(F.trim(F.col("_c15")), F.lit("")).cast("string").alias("numero"),
        F.nullif(F.trim(F.col("_c16")), F.lit("")).cast("string").alias("complemento"),
        F.nullif(F.trim(F.col("_c17")), F.lit("")).cast("string").alias("bairro"),
        F.nullif(F.regexp_replace(F.col("_c18"), "[^0-9]", ""), F.lit("")).alias("cep"),
        F.nullif(F.trim(F.col("_c19")), F.lit("")).cast("string").alias("uf"),
        F.expr("try_cast(nullif(trim(_c20), '') as smallint)").alias("municipio"),
        F.nullif(F.regexp_replace(F.col("_c21"), "[^0-9]", ""), F.lit("")).alias("ddd_1"),
        F.nullif(F.regexp_replace(F.col("_c22"), "[^0-9]", ""), F.lit("")).alias("telefone_1"),
        F.nullif(F.regexp_replace(F.col("_c23"), "[^0-9]", ""), F.lit("")).alias("ddd_2"),
        F.nullif(F.regexp_replace(F.col("_c24"), "[^0-9]", ""), F.lit("")).alias("telefone_2"),
        F.nullif(F.regexp_replace(F.col("_c25"), "[^0-9]", ""), F.lit("")).alias("ddd_fax"),
        F.nullif(F.regexp_replace(F.col("_c26"), "[^0-9]", ""), F.lit("")).alias("fax"),
        F.nullif(F.trim(F.col("_c27")), F.lit("")).cast("string").alias("email"),
        F.nullif(F.trim(F.col("_c28")), F.lit("")).cast("string").alias("situacao_especial"),
        F.expr("try_to_date(nullif(trim(_c29), ''), 'yyyyMMdd')").alias("data_situacao_especial"),
    )
