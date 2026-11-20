from pyspark import pipelines as dp


@dp.materialized_view(name="int_ufs")
def int_ufs():
    return spark.sql("""
        WITH ufs_distintas AS (
            SELECT DISTINCT uf
            FROM silver.estabelecimentos
            WHERE uf IS NOT NULL
        ),
        ufs_codificadas AS (
            SELECT
                ROW_NUMBER() OVER (ORDER BY uf) AS codigo,
                uf AS sigla,
                CASE uf
                    WHEN 'AC' THEN 'Acre'
                    WHEN 'AP' THEN 'Amapá'
                    WHEN 'AM' THEN 'Amazonas'
                    WHEN 'PA' THEN 'Pará'
                    WHEN 'RO' THEN 'Rondônia'
                    WHEN 'RR' THEN 'Roraima'
                    WHEN 'TO' THEN 'Tocantins'
                    WHEN 'AL' THEN 'Alagoas'
                    WHEN 'BA' THEN 'Bahia'
                    WHEN 'CE' THEN 'Ceará'
                    WHEN 'MA' THEN 'Maranhão'
                    WHEN 'PB' THEN 'Paraíba'
                    WHEN 'PE' THEN 'Pernambuco'
                    WHEN 'PI' THEN 'Piauí'
                    WHEN 'RN' THEN 'Rio Grande do Norte'
                    WHEN 'SE' THEN 'Sergipe'
                    WHEN 'DF' THEN 'Distrito Federal'
                    WHEN 'GO' THEN 'Goiás'
                    WHEN 'MT' THEN 'Mato Grosso'
                    WHEN 'MS' THEN 'Mato Grosso do Sul'
                    WHEN 'ES' THEN 'Espírito Santo'
                    WHEN 'MG' THEN 'Minas Gerais'
                    WHEN 'RJ' THEN 'Rio de Janeiro'
                    WHEN 'SP' THEN 'São Paulo'
                    WHEN 'PR' THEN 'Paraná'
                    WHEN 'RS' THEN 'Rio Grande do Sul'
                    WHEN 'SC' THEN 'Santa Catarina'
                    WHEN 'EX' THEN 'Exterior'
                END AS descricao,
                CASE
                    WHEN uf IN ('AC','AP','AM','PA','RO','RR','TO') THEN 'NORTE'
                    WHEN uf IN ('AL','BA','CE','MA','PB','PE','PI','RN','SE') THEN 'NORDESTE'
                    WHEN uf IN ('DF','GO','MT','MS') THEN 'CENTRO-OESTE'
                    WHEN uf IN ('ES','MG','RJ','SP') THEN 'SUDESTE'
                    WHEN uf IN ('PR','RS','SC') THEN 'SUL'
                    WHEN uf = 'EX' THEN 'EXTERIOR'
                END AS regiao
            FROM ufs_distintas

            UNION ALL
            SELECT -1, 'NÃO INF.', 'NÃO INFORMADO', 'NÃO INFORMADO'
            UNION ALL
            SELECT -2, 'NÃO IDENT.', 'NÃO IDENTIFICADO', 'NÃO IDENTIFICADO'
        )
        SELECT
            CAST(codigo AS TINYINT) AS codigo,
            sigla,
            descricao,
            regiao,
            CURRENT_TIMESTAMP() AS data_processamento
        FROM ufs_codificadas
    """)


@dp.materialized_view(name="int_tipos_pessoas")
def int_tipos_pessoas():
    return spark.sql("""
        WITH tipos_pessoas_distintas AS (
            SELECT DISTINCT
                identificador AS codigo,
                CASE identificador
                    WHEN 1 THEN 'PESSOA FÍSICA'
                    WHEN 2 THEN 'PESSOA JURÍDICA'
                    WHEN 3 THEN 'ESTRANGEIRO'
                END AS descricao
            FROM silver.socios
            WHERE identificador IS NOT NULL

            UNION ALL
            SELECT -1 AS codigo, 'NÃO INFORMADO' AS descricao

            UNION ALL
            SELECT -2 AS codigo, 'NÃO IDENTIFICADO' AS descricao
        )
        SELECT
            CAST(codigo AS TINYINT) AS codigo,
            descricao,
            CURRENT_TIMESTAMP() AS data_processamento
        FROM tipos_pessoas_distintas
    """)


@dp.materialized_view(name="int_tipos_estabelecimentos")
def int_tipos_estabelecimentos():
    return spark.sql("""
        WITH tipos_estabelecimentos_distintos AS (
            SELECT DISTINCT identificador_matriz_filial
            FROM silver.estabelecimentos
            WHERE identificador_matriz_filial IS NOT NULL
        ),
        tipos_estabelecimentos_codificada AS (
            SELECT
                identificador_matriz_filial AS codigo,
                CASE
                    WHEN identificador_matriz_filial = 1 THEN 'MATRIZ'
                    WHEN identificador_matriz_filial = 2 THEN 'FILIAL'
                END AS descricao
            FROM tipos_estabelecimentos_distintos

            UNION ALL
            SELECT -1 AS codigo, 'NÃO INFORMADO' AS descricao

            UNION ALL
            SELECT -2 AS codigo, 'NÃO IDENTIFICADO' AS descricao
        )
        SELECT
            CAST(codigo AS TINYINT) AS codigo,
            descricao,
            CURRENT_TIMESTAMP() AS data_processamento
        FROM tipos_estabelecimentos_codificada
    """)


@dp.materialized_view(name="int_tempo_situacoes_especiais")
def int_tempo_situacoes_especiais():
    return spark.sql("""
        WITH limites AS (
            SELECT
                MIN(data_situacao_especial) AS data_min,
                MAX(data_situacao_especial) AS data_max
            FROM silver.int_estabelecimentos
            WHERE data_situacao_especial IS NOT NULL
        ),

        date_spine AS (
            SELECT EXPLODE(SEQUENCE(data_min, data_max, INTERVAL 1 DAY)) AS data_referencia
            FROM limites
            WHERE data_min IS NOT NULL
        ),

        dim_tempo_enriquecida AS (
            SELECT
                CAST(DATE_FORMAT(data_referencia, 'yyyyMMdd') AS INT) AS sk_id,
                data_referencia,
                CAST(YEAR(data_referencia) AS SMALLINT) AS ano,
                CAST(MONTH(data_referencia) AS TINYINT) AS mes,
                CAST(DAY(data_referencia) AS TINYINT) AS dia,
                CAST(QUARTER(data_referencia) AS TINYINT) AS trimestre,

                CAST(
                    CASE
                        WHEN MONTH(data_referencia) <= 6 THEN 1
                        ELSE 2
                    END AS TINYINT
                ) AS semestre,

                -- No Spark, WEEKDAY() retorna 0 = Segunda-feira até 6 = Domingo. Somando +1 equivale ao ISODOW (1 a 7)
                CAST(WEEKDAY(data_referencia) + 1 AS TINYINT) AS dia_semana_numero,

                CASE MONTH(data_referencia)
                    WHEN 1 THEN 'Janeiro' WHEN 2 THEN 'Fevereiro' WHEN 3 THEN 'Março'
                    WHEN 4 THEN 'Abril' WHEN 5 THEN 'Maio' WHEN 6 THEN 'Junho'
                    WHEN 7 THEN 'Julho' WHEN 8 THEN 'Agosto' WHEN 9 THEN 'Setembro'
                    WHEN 10 THEN 'Outubro' WHEN 11 THEN 'Novembro' WHEN 12 THEN 'Dezembro'
                END AS nome_mes,

                CASE WEEKDAY(data_referencia) + 1
                    WHEN 1 THEN 'Segunda-feira'
                    WHEN 2 THEN 'Terça-feira'
                    WHEN 3 THEN 'Quarta-feira'
                    WHEN 4 THEN 'Quinta-feira'
                    WHEN 5 THEN 'Sexta-feira'
                    WHEN 6 THEN 'Sábado'
                    WHEN 7 THEN 'Domingo'
                END AS nome_dia_semana
            FROM date_spine
        )

        SELECT
            sk_id,
            data_referencia,
            ano,
            mes,
            dia,
            trimestre,
            semestre,
            dia_semana_numero,
            nome_mes,
            nome_dia_semana,
            CURRENT_TIMESTAMP() AS data_processamento
        FROM dim_tempo_enriquecida

        UNION ALL

        SELECT
            -1 AS sk_id,
            CAST(NULL AS DATE) AS data_referencia,
            CAST(NULL AS SMALLINT) AS ano,
            CAST(NULL AS TINYINT) AS mes,
            CAST(NULL AS TINYINT) AS dia,
            CAST(NULL AS TINYINT) AS trimestre,
            CAST(NULL AS TINYINT) AS semestre,
            CAST(NULL AS TINYINT) AS dia_semana_numero,
            'Não informado' AS nome_mes,
            'Não informado' AS nome_dia_semana,
            CURRENT_TIMESTAMP() AS data_processamento
    """)


@dp.materialized_view(name="int_tempo_situacoes_cadastrais")
def int_tempo_situacoes_cadastrais():
    return spark.sql("""
        WITH limites AS (
            SELECT
                MIN(data_situacao_cadastral) AS data_min,
                MAX(data_situacao_cadastral) AS data_max
            FROM silver.int_estabelecimentos
            WHERE data_situacao_cadastral IS NOT NULL
        ),

        date_spine AS (
            SELECT EXPLODE(SEQUENCE(data_min, data_max, INTERVAL 1 DAY)) AS data_referencia
            FROM limites
            WHERE data_min IS NOT NULL
        ),

        dim_tempo_enriquecida AS (
            SELECT
                CAST(DATE_FORMAT(data_referencia, 'yyyyMMdd') AS INT) AS sk_id,
                data_referencia,
                CAST(YEAR(data_referencia) AS SMALLINT) AS ano,
                CAST(MONTH(data_referencia) AS TINYINT) AS mes,
                CAST(DAY(data_referencia) AS TINYINT) AS dia,
                CAST(QUARTER(data_referencia) AS TINYINT) AS trimestre,

                CAST(
                    CASE
                        WHEN MONTH(data_referencia) <= 6 THEN 1
                        ELSE 2
                    END AS TINYINT
                ) AS semestre,

                CAST(WEEKDAY(data_referencia) + 1 AS TINYINT) AS dia_semana_numero,

                CASE MONTH(data_referencia)
                    WHEN 1 THEN 'Janeiro' WHEN 2 THEN 'Fevereiro' WHEN 3 THEN 'Março'
                    WHEN 4 THEN 'Abril' WHEN 5 THEN 'Maio' WHEN 6 THEN 'Junho'
                    WHEN 7 THEN 'Julho' WHEN 8 THEN 'Agosto' WHEN 9 THEN 'Setembro'
                    WHEN 10 THEN 'Outubro' WHEN 11 THEN 'Novembro' WHEN 12 THEN 'Dezembro'
                END AS nome_mes,

                CASE WEEKDAY(data_referencia) + 1
                    WHEN 1 THEN 'Segunda-feira'
                    WHEN 2 THEN 'Terça-feira'
                    WHEN 3 THEN 'Quarta-feira'
                    WHEN 4 THEN 'Quinta-feira'
                    WHEN 5 THEN 'Sexta-feira'
                    WHEN 6 THEN 'Sábado'
                    WHEN 7 THEN 'Domingo'
                END AS nome_dia_semana
            FROM date_spine
        )

        SELECT
            sk_id,
            data_referencia,
            ano,
            mes,
            dia,
            trimestre,
            semestre,
            dia_semana_numero,
            nome_mes,
            nome_dia_semana,
            CURRENT_TIMESTAMP() AS data_processamento
        FROM dim_tempo_enriquecida

        UNION ALL

        SELECT
            -1 AS sk_id,
            CAST(NULL AS DATE) AS data_referencia,
            CAST(NULL AS SMALLINT) AS ano,
            CAST(NULL AS TINYINT) AS mes,
            CAST(NULL AS TINYINT) AS dia,
            CAST(NULL AS TINYINT) AS trimestre,
            CAST(NULL AS TINYINT) AS semestre,
            CAST(NULL AS TINYINT) AS dia_semana_numero,
            'Não informado' AS nome_mes,
            'Não informado' AS nome_dia_semana,
            CURRENT_TIMESTAMP() AS data_processamento
    """)


@dp.materialized_view(name="int_tempo_inicio_atividades")
def int_tempo_inicio_atividades():
    return spark.sql("""
        WITH limites AS (
            SELECT
                MIN(data_inicio_atividade) AS data_min,
                MAX(data_inicio_atividade) AS data_max
            FROM silver.int_estabelecimentos
            WHERE data_inicio_atividade IS NOT NULL
        ),

        date_spine AS (
            SELECT EXPLODE(SEQUENCE(data_min, data_max, INTERVAL 1 DAY)) AS data_referencia
            FROM limites
            WHERE data_min IS NOT NULL
        ),

        dim_tempo_enriquecida AS (
            SELECT
                CAST(DATE_FORMAT(data_referencia, 'yyyyMMdd') AS INT) AS sk_id,
                data_referencia,
                CAST(YEAR(data_referencia) AS SMALLINT) AS ano,
                CAST(MONTH(data_referencia) AS TINYINT) AS mes,
                CAST(DAY(data_referencia) AS TINYINT) AS dia,
                CAST(QUARTER(data_referencia) AS TINYINT) AS trimestre,

                CAST(
                    CASE
                        WHEN MONTH(data_referencia) <= 6 THEN 1
                        ELSE 2
                    END AS TINYINT
                ) AS semestre,

                CAST(WEEKDAY(data_referencia) + 1 AS TINYINT) AS dia_semana_numero,

                CASE MONTH(data_referencia)
                    WHEN 1 THEN 'Janeiro' WHEN 2 THEN 'Fevereiro' WHEN 3 THEN 'Março'
                    WHEN 4 THEN 'Abril' WHEN 5 THEN 'Maio' WHEN 6 THEN 'Junho'
                    WHEN 7 THEN 'Julho' WHEN 8 THEN 'Agosto' WHEN 9 THEN 'Setembro'
                    WHEN 10 THEN 'Outubro' WHEN 11 THEN 'Novembro' WHEN 12 THEN 'Dezembro'
                END AS nome_mes,

                CASE WEEKDAY(data_referencia) + 1
                    WHEN 1 THEN 'Segunda-feira'
                    WHEN 2 THEN 'Terça-feira'
                    WHEN 3 THEN 'Quarta-feira'
                    WHEN 4 THEN 'Quinta-feira'
                    WHEN 5 THEN 'Sexta-feira'
                    WHEN 6 THEN 'Sábado'
                    WHEN 7 THEN 'Domingo'
                END AS nome_dia_semana
            FROM date_spine
        )

        SELECT
            sk_id,
            data_referencia,
            ano,
            mes,
            dia,
            trimestre,
            semestre,
            dia_semana_numero,
            nome_mes,
            nome_dia_semana,
            CURRENT_TIMESTAMP() AS data_processamento
        FROM dim_tempo_enriquecida

        UNION ALL

        SELECT
            -1 AS sk_id,
            CAST(NULL AS DATE) AS data_referencia,
            CAST(NULL AS SMALLINT) AS ano,
            CAST(NULL AS TINYINT) AS mes,
            CAST(NULL AS TINYINT) AS dia,
            CAST(NULL AS TINYINT) AS trimestre,
            CAST(NULL AS TINYINT) AS semestre,
            CAST(NULL AS TINYINT) AS dia_semana_numero,
            'Não informado' AS nome_mes,
            'Não informado' AS nome_dia_semana,
            CURRENT_TIMESTAMP() AS data_processamento
    """)


@dp.materialized_view(name="int_tempo_entrada_sociedade")
def int_tempo_entrada_sociedade():
    return spark.sql("""
        WITH limites AS (
            SELECT
                MIN(data_entrada_sociedade) AS data_min,
                MAX(data_entrada_sociedade) AS data_max
            FROM silver.socios
            WHERE data_entrada_sociedade IS NOT NULL
        ),

        date_spine AS (
            SELECT EXPLODE(SEQUENCE(data_min, data_max, INTERVAL 1 DAY)) AS data_referencia
            FROM limites
            WHERE data_min IS NOT NULL
        ),

        dim_tempo_enriquecida AS (
            SELECT
                CAST(DATE_FORMAT(data_referencia, 'yyyyMMdd') AS INT) AS sk_id,
                data_referencia,
                CAST(YEAR(data_referencia) AS SMALLINT) AS ano,
                CAST(MONTH(data_referencia) AS TINYINT) AS mes,
                CAST(DAY(data_referencia) AS TINYINT) AS dia,
                CAST(QUARTER(data_referencia) AS TINYINT) AS trimestre,

                CAST(
                    CASE
                        WHEN MONTH(data_referencia) <= 6 THEN 1
                        ELSE 2
                    END AS TINYINT
                ) AS semestre,

                CAST(WEEKDAY(data_referencia) + 1 AS TINYINT) AS dia_semana_numero,

                CASE MONTH(data_referencia)
                    WHEN 1 THEN 'Janeiro' WHEN 2 THEN 'Fevereiro' WHEN 3 THEN 'Março'
                    WHEN 4 THEN 'Abril' WHEN 5 THEN 'Maio' WHEN 6 THEN 'Junho'
                    WHEN 7 THEN 'Julho' WHEN 8 THEN 'Agosto' WHEN 9 THEN 'Setembro'
                    WHEN 10 THEN 'Outubro' WHEN 11 THEN 'Novembro' WHEN 12 THEN 'Dezembro'
                END AS nome_mes,

                CASE WEEKDAY(data_referencia) + 1
                    WHEN 1 THEN 'Segunda-feira'
                    WHEN 2 THEN 'Terça-feira'
                    WHEN 3 THEN 'Quarta-feira'
                    WHEN 4 THEN 'Quinta-feira'
                    WHEN 5 THEN 'Sexta-feira'
                    WHEN 6 THEN 'Sábado'
                    WHEN 7 THEN 'Domingo'
                END AS nome_dia_semana
            FROM date_spine
        )

        SELECT
            sk_id,
            data_referencia,
            ano,
            mes,
            dia,
            trimestre,
            semestre,
            dia_semana_numero,
            nome_mes,
            nome_dia_semana,
            CURRENT_TIMESTAMP() AS data_processamento
        FROM dim_tempo_enriquecida

        UNION ALL

        SELECT
            -1 AS sk_id,
            CAST(NULL AS DATE) AS data_referencia,
            CAST(NULL AS SMALLINT) AS ano,
            CAST(NULL AS TINYINT) AS mes,
            CAST(NULL AS TINYINT) AS dia,
            CAST(NULL AS TINYINT) AS trimestre,
            CAST(NULL AS TINYINT) AS semestre,
            CAST(NULL AS TINYINT) AS dia_semana_numero,
            'Não informado' AS nome_mes,
            'Não informado' AS nome_dia_semana,
            CURRENT_TIMESTAMP() AS data_processamento
    """)


@dp.materialized_view(name="int_situacoes_especiais")
def int_situacoes_especiais():
    return spark.sql("""
        WITH situacoes_especiais_distintas AS (
            SELECT DISTINCT situacao_especial
            FROM silver.estabelecimentos
            WHERE situacao_especial IS NOT NULL
        ),

        situacoes_especiais_codificada AS (
            SELECT
                ROW_NUMBER() OVER (ORDER BY situacao_especial) AS codigo,
                situacao_especial AS descricao
            FROM situacoes_especiais_distintas

            UNION ALL
            SELECT -1 AS codigo, 'NÃO INFORMADO' AS descricao

            UNION ALL
            SELECT -2 AS codigo, 'NÃO IDENTIFICADO' AS descricao
        )

        SELECT
            CAST(codigo AS TINYINT) AS codigo,
            descricao,
            CURRENT_TIMESTAMP() AS data_processamento
        FROM situacoes_especiais_codificada
    """)


@dp.materialized_view(name="int_situacoes_cadastrais_motivos")
def int_situacoes_cadastrais_motivos():
    return spark.sql("""
        WITH motivos AS (
            SELECT
                codigo,
                descricao
            FROM silver.motivos

            UNION ALL
            SELECT -1 AS codigo, 'NÃO INFORMADO' AS descricao

            UNION ALL
            SELECT -2 AS codigo, 'NÃO IDENTIFICADO' AS descricao
        )

        SELECT
            CAST(codigo AS SMALLINT) AS codigo,
            descricao,
            CURRENT_TIMESTAMP() AS data_processamento
        FROM motivos
    """)


@dp.materialized_view(name="int_situacoes_cadastrais")
def int_situacoes_cadastrais():
    return spark.sql("""
        WITH situacao_cadastral_distintas AS (
            SELECT DISTINCT situacao_cadastral
            FROM silver.estabelecimentos
            WHERE situacao_cadastral IS NOT NULL
        ),

        situacao_cadastral_codificada AS (
            SELECT
                situacao_cadastral AS codigo,
                CASE
                    WHEN situacao_cadastral = 1 THEN 'NULA'
                    WHEN situacao_cadastral = 2 THEN 'ATIVA'
                    WHEN situacao_cadastral = 3 THEN 'SUSPENSA'
                    WHEN situacao_cadastral = 4 THEN 'INAPTA'
                    WHEN situacao_cadastral = 8 THEN 'BAIXADA'
                END AS descricao
            FROM situacao_cadastral_distintas

            UNION ALL
            SELECT -1 AS codigo, 'NÃO INFORMADO' AS descricao

            UNION ALL
            SELECT -2 AS codigo, 'NÃO IDENTIFICADO' AS descricao
        )

        SELECT
            CAST(codigo AS TINYINT) AS codigo,
            descricao,
            CURRENT_TIMESTAMP() AS data_processamento
        FROM situacao_cadastral_codificada
    """)


@dp.materialized_view(name="int_simples")
def int_simples():
    return spark.sql("""
        SELECT
            cnpj_basico,
            opcao_simples,
            data_opcao_simples,
            data_exclusao_simples,
            opcao_mei,
            data_opcao_mei,
            data_exclusao_mei,
            CURRENT_TIMESTAMP() AS data_processamento
        FROM silver.simples
    """)


@dp.materialized_view(name="int_qualificacoes")
def int_qualificacoes():
    return spark.sql("""
        WITH qualificacoes AS (
            SELECT
                codigo,
                descricao
            FROM silver.qualificacoes

            UNION ALL
            SELECT
                -1 AS codigo,
                'NÃO INFORMADO' AS descricao

            UNION ALL
            SELECT
                -2 AS codigo,
                'NÃO IDENTIFICADO' AS descricao
        )

        SELECT
            CAST(codigo AS TINYINT) AS codigo,
            descricao,
            CURRENT_TIMESTAMP() AS data_processamento
        FROM qualificacoes
    """)


@dp.materialized_view(name="int_portes_empresas")
def int_portes_empresas():
    return spark.sql("""
        WITH portes_empresas_distintas AS (
            SELECT DISTINCT
                porte_empresa AS codigo,
                CASE
                    WHEN porte_empresa = 1 THEN 'ME'
                    WHEN porte_empresa = 3 THEN 'EPP'
                    WHEN porte_empresa = 5 THEN 'DEMAIS'
                END AS sigla,
                CASE
                    WHEN porte_empresa = 1 THEN 'MICRO EMPRESA'
                    WHEN porte_empresa = 3 THEN 'EMPRESA DE PEQUENO PORTE'
                    WHEN porte_empresa = 5 THEN 'DEMAIS'
                END AS descricao
            FROM silver.empresas
            WHERE porte_empresa IS NOT NULL

            UNION ALL
            SELECT -1 AS codigo, 'NÃO INF.' AS sigla, 'NÃO INFORMADO' AS descricao

            UNION ALL
            SELECT -2 AS codigo, 'NÃO IDENT.' AS sigla, 'NÃO IDENTIFICADO' AS descricao
        )

        SELECT
            CAST(codigo AS TINYINT) AS codigo,
            sigla,
            descricao,
            CURRENT_TIMESTAMP() AS data_processamento
        FROM portes_empresas_distintas
    """)


@dp.materialized_view(name="int_paises")
def int_paises():
    return spark.sql("""
        WITH paises AS (
            SELECT
                codigo,
                descricao
            FROM silver.paises

            UNION ALL
            SELECT
                -1 AS codigo,
                'NÃO INFORMADO' AS descricao

            UNION ALL
            SELECT
                -2 AS codigo,
                'NÃO IDENTIFICADO' AS descricao
        )

        SELECT
            CAST(codigo AS SMALLINT) AS codigo,
            descricao,
            CURRENT_TIMESTAMP() AS data_processamento
        FROM paises
    """)


@dp.materialized_view(name="int_naturezas")
def int_naturezas():
    return spark.sql("""
        WITH naturezas_juridicas AS (
            SELECT
                codigo,
                CONCAT(SUBSTR(CAST(codigo AS STRING), 1, 3), '-', SUBSTR(CAST(codigo AS STRING), 4, 1)) AS codigo_formatado,
                descricao
            FROM silver.naturezas

            UNION ALL
            SELECT -1 AS codigo, 'NÃO INFORMADO' AS codigo_formatado, 'NÃO INFORMADO' AS descricao

            UNION ALL
            SELECT -2 AS codigo, 'NÃO IDENTIFICADO' AS codigo_formatado, 'NÃO IDENTIFICADO' AS descricao
        )

        SELECT
            CAST(codigo AS SMALLINT) AS codigo,
            codigo_formatado,
            descricao,
            CURRENT_TIMESTAMP() AS data_processamento
        FROM naturezas_juridicas
    """)


@dp.materialized_view(name="int_municipios")
def int_municipios():
    return spark.sql("""
        WITH municipios AS (
            SELECT
                codigo,
                descricao
            FROM silver.municipios

            UNION ALL
            SELECT -1 AS codigo, 'NÃO INFORMADO' AS descricao

            UNION ALL
            SELECT -2 AS codigo, 'NÃO IDENTIFICADO' AS descricao
        )

        SELECT
            CAST(codigo AS SMALLINT) AS codigo,
            descricao,
            CURRENT_TIMESTAMP() AS data_processamento
        FROM municipios
    """)


@dp.materialized_view(name="int_faixas_etarias_socios")
def int_faixas_etarias_socios():
    return spark.sql("""
        WITH faixas_etarias_distintas AS (
            SELECT DISTINCT
                faixa_etaria_socio AS codigo,
                CASE
                    WHEN faixa_etaria_socio = 1 THEN '0 a 12 anos'
                    WHEN faixa_etaria_socio = 2 THEN '13 a 20 anos'
                    WHEN faixa_etaria_socio = 3 THEN '21 a 30 anos'
                    WHEN faixa_etaria_socio = 4 THEN '31 a 40 anos'
                    WHEN faixa_etaria_socio = 5 THEN '41 a 50 anos'
                    WHEN faixa_etaria_socio = 6 THEN '51 a 60 anos'
                    WHEN faixa_etaria_socio = 7 THEN '61 a 70 anos'
                    WHEN faixa_etaria_socio = 8 THEN '71 a 80 anos'
                    WHEN faixa_etaria_socio = 9 THEN 'maiores de 80 anos'
                    WHEN faixa_etaria_socio = 0 THEN 'não se aplica'
                END AS descricao
            FROM silver.socios
            WHERE faixa_etaria_socio IS NOT NULL

            UNION ALL
            SELECT -1 AS codigo, 'não informado' AS descricao

            UNION ALL
            SELECT -2 AS codigo, 'não identificado' AS descricao
        )

        SELECT
            CAST(codigo AS TINYINT) AS codigo,
            descricao,
            CURRENT_TIMESTAMP() AS data_processamento
        FROM faixas_etarias_distintas
    """)


@dp.materialized_view(name="int_estabelecimentos")
def int_estabelecimentos():
    return spark.sql("""
        SELECT
            CAST(ROW_NUMBER() OVER (ORDER BY e.cnpj_basico, e.cnpj_ordem, e.cnpj_dv) AS INT) AS sk_id,
            e.cnpj_basico,
            e.cnpj_ordem,
            e.cnpj_dv,
            CASE
                WHEN e.identificador_matriz_filial IS NULL THEN CAST(-1 AS TINYINT) -- NÃO INFORMADO
                WHEN te.codigo IS NULL THEN CAST(-2 AS TINYINT) -- NÃO IDENTIFICADO
                ELSE te.codigo
            END AS identificador_matriz_filial,
            e.nome_fantasia,
            CASE
                WHEN e.situacao_cadastral IS NULL THEN CAST(-1 AS TINYINT) -- NÃO INFORMADO
                WHEN sc.codigo IS NULL THEN CAST(-2 AS TINYINT) -- NÃO IDENTIFICADO
                ELSE sc.codigo
            END AS situacao_cadastral,
            e.data_situacao_cadastral,
            CASE
                WHEN e.motivo_situacao_cadastral IS NULL THEN CAST(-1 AS TINYINT) -- NÃO INFORMADO
                WHEN scm.codigo IS NULL THEN CAST(-2 AS TINYINT) -- NÃO IDENTIFICADO
                ELSE scm.codigo
            END AS motivo_situacao_cadastral,
            CASE
                WHEN e.cidade_exterior IS NULL THEN CAST(-1 AS SMALLINT) -- NÃO INFORMADO
                WHEN me.codigo IS NULL THEN CAST(-2 AS SMALLINT) -- NÃO IDENTIFICADO
                ELSE me.codigo
            END AS cidade_exterior,
            CASE
                -- Se identificou o país na tabela países
                WHEN p.codigo IS NOT NULL THEN CAST(e.pais AS SMALLINT)
                -- Se não identificou o país e a cidade não é EXTERIOR (9707)
                WHEN p.codigo IS NULL AND mu.codigo <> 9707 THEN CAST(105 AS SMALLINT) -- BRASIL
                -- Se não identificou o país e está como EXTERIOR
                WHEN p.codigo IS NULL AND mu.codigo = 9707 THEN CAST(-2 AS SMALLINT) -- NÃO IDENTIFICADO
                ELSE CAST(-1 AS SMALLINT) -- NÃO INFORMADO
            END AS pais,
            e.data_inicio_atividade,
            CASE
                WHEN e.cnae_fiscal_principal IS NULL THEN '-1' -- NÃO INFORMADO
                WHEN c.codigo IS NULL THEN '-2' -- NÃO IDENTIFICADO
                ELSE c.codigo
            END AS cnae_fiscal_principal,
            e.cnae_fiscal_secundaria,
            e.tipo_logradouro,
            e.logradouro,
            e.numero,
            e.complemento,
            e.bairro,
            e.cep,
            CASE
                WHEN e.uf IS NULL THEN CAST(-1 AS TINYINT) -- NÃO INFORMADO
                WHEN u.codigo IS NULL THEN CAST(-2 AS TINYINT) -- NÃO IDENTIFICADO
                ELSE u.codigo
            END AS uf,
            CASE
                WHEN e.municipio IS NULL THEN CAST(-1 AS SMALLINT) -- NÃO INFORMADO
                WHEN mu.codigo IS NULL THEN CAST(-2 AS SMALLINT) -- NÃO IDENTIFICADO
                ELSE mu.codigo
            END AS municipio,
            e.ddd_1,
            e.telefone_1,
            e.ddd_2,
            e.telefone_2,
            e.ddd_fax,
            e.fax,
            e.email,
            CASE
                WHEN e.situacao_especial IS NULL THEN CAST(-1 AS TINYINT) -- NÃO INFORMADO
                WHEN se.codigo IS NULL THEN CAST(-2 AS TINYINT) -- NÃO IDENTIFICADO
                ELSE se.codigo
            END AS situacao_especial,
            e.data_situacao_especial,
            CURRENT_TIMESTAMP() AS data_processamento
        FROM silver.estabelecimentos e
        LEFT JOIN silver.int_tipos_estabelecimentos te ON te.codigo = e.identificador_matriz_filial
        LEFT JOIN silver.int_situacoes_cadastrais sc ON sc.codigo = e.situacao_cadastral
        LEFT JOIN silver.int_situacoes_cadastrais_motivos scm ON scm.codigo = e.motivo_situacao_cadastral
        LEFT JOIN silver.int_cidades_exterior me ON me.descricao = e.cidade_exterior
        LEFT JOIN silver.int_paises p ON p.codigo = e.pais
        LEFT JOIN silver.int_cnaes c ON c.codigo = e.cnae_fiscal_principal
        LEFT JOIN silver.int_ufs u ON u.sigla = e.uf
        LEFT JOIN silver.int_municipios mu ON mu.codigo = e.municipio
        LEFT JOIN silver.int_situacoes_especiais se ON se.descricao = e.situacao_especial
    """)


@dp.materialized_view(name="int_entes_federativos")
def int_entes_federativos():
    return spark.sql("""
        WITH entes_federativos AS (
            SELECT DISTINCT ente_federativo_responsavel
            FROM silver.empresas
            WHERE ente_federativo_responsavel IS NOT NULL
        ),

        entes_federativos_codificada AS (
            SELECT
                ROW_NUMBER() OVER (ORDER BY ente_federativo_responsavel) AS codigo,
                ente_federativo_responsavel AS descricao
            FROM entes_federativos

            UNION ALL
            SELECT -1 AS codigo, 'NÃO INFORMADO' AS descricao

            UNION ALL
            SELECT -2 AS codigo, 'NÃO IDENTIFICADO' AS descricao
        )

        SELECT
            CAST(codigo AS SMALLINT) AS codigo,
            descricao,
            CURRENT_TIMESTAMP() AS data_processamento
        FROM entes_federativos_codificada
    """)


@dp.materialized_view(name="int_empresas")
def int_empresas():
    return spark.sql("""
        WITH empresas_qualificadas AS (
            SELECT
                cnpj_basico,
                razao_social,
                natureza_juridica,
                qualificacao_responsavel,
                capital_social,
                porte_empresa,
                ente_federativo_responsavel,
                CAST(
                    ROW_NUMBER() OVER (
                        PARTITION BY cnpj_basico
                        ORDER BY
                            (CASE WHEN razao_social IS NOT NULL THEN 1 ELSE 0 END
                            + CASE WHEN natureza_juridica IS NOT NULL THEN 1 ELSE 0 END
                            + CASE WHEN qualificacao_responsavel IS NOT NULL THEN 1 ELSE 0 END
                            + CASE WHEN capital_social IS NOT NULL THEN 1 ELSE 0 END
                            + CASE WHEN porte_empresa IS NOT NULL THEN 1 ELSE 0 END
                            + CASE WHEN ente_federativo_responsavel IS NOT NULL THEN 1 ELSE 0 END) DESC,
                            razao_social DESC NULLS LAST -- desempate determinístico se completude empatar
                    ) AS INT
                ) AS rn
            FROM silver.empresas
            WHERE cnpj_basico IS NOT NULL
            QUALIFY rn = 1
        )

        SELECT
            em.cnpj_basico,
            em.razao_social,
            CASE
                WHEN em.natureza_juridica IS NULL THEN CAST(-1 AS SMALLINT) -- NÃO INFORMADO
                WHEN nj.codigo IS NULL THEN CAST(-2 AS SMALLINT) -- NÃO IDENTIFICADO
                ELSE CAST(nj.codigo AS SMALLINT)
            END AS natureza_juridica,
            CASE
                WHEN em.qualificacao_responsavel IS NULL THEN CAST(-1 AS TINYINT) -- NÃO INFORMADO
                WHEN q.codigo IS NULL THEN CAST(-2 AS TINYINT) -- NÃO IDENTIFICADO
                ELSE CAST(q.codigo AS TINYINT)
            END AS qualificacao_responsavel,
            em.capital_social,
            CASE
                WHEN em.porte_empresa IS NULL THEN CAST(-1 AS TINYINT) -- NÃO INFORMADO
                WHEN pe.codigo IS NULL THEN CAST(-2 AS TINYINT) -- NÃO IDENTIFICADO
                ELSE pe.codigo
            END AS porte_empresa,
            CASE
                WHEN em.ente_federativo_responsavel IS NULL THEN CAST(-1 AS SMALLINT) -- NÃO INFORMADO
                WHEN ef.codigo IS NULL THEN CAST(-2 AS SMALLINT) -- NÃO IDENTIFICADO
                ELSE ef.codigo
            END AS ente_federativo_responsavel,
            CURRENT_TIMESTAMP() AS data_processamento
        FROM empresas_qualificadas em
        LEFT JOIN silver.naturezas nj ON nj.codigo = em.natureza_juridica
        LEFT JOIN silver.qualificacoes q ON q.codigo = em.qualificacao_responsavel
        LEFT JOIN silver.int_portes_empresas pe ON pe.codigo = em.porte_empresa
        LEFT JOIN silver.int_entes_federativos ef ON ef.descricao = em.ente_federativo_responsavel
    """)


@dp.materialized_view(name="int_cnaes_secundarios")
def int_cnaes_secundarios():
    return spark.sql("""
        SELECT
            cs.sk_estabelecimento,
            cs.fk_cnae,
            'SECUNDÁRIO' AS tipo_cnae,
            CURRENT_TIMESTAMP() AS data_processamento
        FROM (
            SELECT
                sk_id AS sk_estabelecimento,
                EXPLODE(ARRAY_DISTINCT(SPLIT(cnae_fiscal_secundaria, ','))) AS fk_cnae
            FROM silver.int_estabelecimentos
            WHERE cnae_fiscal_secundaria IS NOT NULL
        ) AS cs
        JOIN int_cnaes c ON c.codigo = cs.fk_cnae
        LEFT JOIN silver.int_cnaes_principais cp
            ON cp.sk_estabelecimento = cs.sk_estabelecimento
            AND cp.fk_cnae = cs.fk_cnae
        WHERE cp.sk_estabelecimento IS NULL
    """)


@dp.materialized_view(name="int_cnaes_principais")
def int_cnaes_principais():
    return spark.sql("""
        SELECT
            e.sk_id AS sk_estabelecimento,
            c.codigo AS fk_cnae,
            'PRINCIPAL' AS tipo_cnae,
            CURRENT_TIMESTAMP() AS data_processamento
        FROM silver.int_estabelecimentos e
        JOIN silver.int_cnaes c ON c.codigo = e.cnae_fiscal_principal
    """)


@dp.materialized_view(name="int_cnaes")
def int_cnaes():
    return spark.sql("""
        WITH cnaes AS (
            SELECT
                CAST(codigo AS STRING) AS codigo,
                CASE
                    WHEN LENGTH(CAST(codigo AS STRING)) = 7 THEN
                        CONCAT(
                            SUBSTR(CAST(codigo AS STRING), 1, 2), '.',
                            SUBSTR(CAST(codigo AS STRING), 3, 2), '-',
                            SUBSTR(CAST(codigo AS STRING), 5, 1), '-',
                            SUBSTR(CAST(codigo AS STRING), 6, 2)
                        )
                    ELSE CAST(codigo AS STRING)
                END AS codigo_formatado,
                descricao
            FROM silver.cnaes

            UNION ALL
            SELECT '-1' AS codigo, '--1' AS codigo_formatado, 'NÃO INFORMADO' AS descricao

            UNION ALL
            SELECT '-2' AS codigo, '--2' AS codigo_formatado, 'NÃO IDENTIFICADO' AS descricao
        )

        SELECT
            codigo,
            codigo_formatado,
            descricao,
            CURRENT_TIMESTAMP() AS data_processamento
        FROM cnaes
    """)


@dp.materialized_view(name="int_cidades_exterior")
def int_cidades_exterior():
    return spark.sql("""
        WITH cidade_exterior_distintas AS (
            SELECT DISTINCT cidade_exterior
            FROM silver.estabelecimentos
            WHERE cidade_exterior IS NOT NULL
        ),

        cidade_exterior_codificada AS (
            SELECT
                ROW_NUMBER() OVER (ORDER BY cidade_exterior) AS codigo,
                cidade_exterior AS descricao
            FROM cidade_exterior_distintas

            UNION ALL
            SELECT -1 AS codigo, 'NÃO INFORMADO' AS descricao

            UNION ALL
            SELECT -2 AS codigo, 'NÃO IDENTIFICADO' AS descricao
        )

        SELECT
            CAST(codigo AS SMALLINT) AS codigo,
            descricao,
            CURRENT_TIMESTAMP() AS data_processamento
        FROM cidade_exterior_codificada
    """)


@dp.materialized_view(name="int_socios")
def int_socios():
    return spark.sql("""
        WITH socios AS (
            SELECT        
                s.cnpj_basico,        
                CASE
                    WHEN s.identificador IS NULL THEN CAST(-1 AS TINYINT)
                    WHEN tp.codigo IS NULL THEN CAST(-2 AS TINYINT)
                    ELSE tp.codigo
                END AS identificador,
                s.nome_razao_social,
                s.cpf_cnpj,
                CASE
                    WHEN s.qualificacao IS NULL THEN CAST(-1 AS TINYINT)
                    WHEN q.codigo IS NULL THEN CAST(-2 AS TINYINT)
                    ELSE q.codigo
                END AS qualificacao,
                s.data_entrada_sociedade,
                CASE
                    WHEN s.pais IS NULL THEN CAST(-1 AS TINYINT)
                    WHEN p.codigo IS NULL THEN CAST(-2 AS TINYINT)
                    ELSE p.codigo
                END AS pais,
                s.representante_legal,
                s.nome_do_representante,
                CASE
                    WHEN s.qualificacao_representante IS NULL THEN CAST(-1 AS TINYINT)
                    WHEN qr.codigo IS NULL THEN CAST(-2 AS TINYINT)
                    ELSE qr.codigo
                END AS qualificacao_representante,
                CASE
                    WHEN s.faixa_etaria_socio IS NULL THEN CAST(-1 AS TINYINT)
                    WHEN fe.codigo IS NULL THEN CAST(-2 AS TINYINT)
                    ELSE fe.codigo
                END AS faixa_etaria_socio
            FROM silver.socios s
            LEFT JOIN silver.int_tipos_pessoas tp ON tp.codigo = s.identificador
            LEFT JOIN silver.int_qualificacoes q ON q.codigo = s.qualificacao
            LEFT JOIN silver.int_paises p ON p.codigo = s.pais
            LEFT JOIN silver.int_qualificacoes qr ON qr.codigo = s.qualificacao_representante
            LEFT JOIN silver.int_faixas_etarias_socios fe ON fe.codigo = s.faixa_etaria_socio
        )
        SELECT
            CAST(ROW_NUMBER() OVER (ORDER BY cnpj_basico, cpf_cnpj, nome_razao_social) AS INT) AS sk_id,
            cnpj_basico,
            identificador,
            nome_razao_social,
            cpf_cnpj,
            qualificacao,
            data_entrada_sociedade,
            pais,
            representante_legal,
            nome_do_representante,
            qualificacao_representante,
            faixa_etaria_socio,    
            current_timestamp() AS data_processamento
        FROM socios
    """)
