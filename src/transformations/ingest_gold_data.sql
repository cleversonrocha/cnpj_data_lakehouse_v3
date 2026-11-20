CREATE OR REFRESH MATERIALIZED VIEW gold.agg_fact_estabelecimentos AS
SELECT
    e.sk_tempo_inicio_atividade,    
    e.sk_situacoes,
    e.sk_localidades,
    e.sk_naturezas_juridicas,    
    CAST(SUM(is_matriz) AS SMALLINT) AS qtd_matrizes,
    CAST(SUM(is_filial) AS SMALLINT) AS qtd_filiais,
    CAST(SUM(is_nula) AS SMALLINT) AS qtd_nulas,
    CAST(SUM(is_ativa) AS SMALLINT) AS qtd_ativas,
    CAST(SUM(is_suspensa) AS SMALLINT) AS qtd_suspensas,
    CAST(SUM(is_inapta) AS SMALLINT) AS qtd_inaptas,
    CAST(SUM(is_baixada) AS SMALLINT) AS qtd_baixadas,    
    CAST(SUM(is_epp) AS SMALLINT) AS qtd_epp,
    CAST(SUM(is_me) AS SMALLINT) AS qtd_me,
    CAST(SUM(is_demais) AS SMALLINT) AS qtd_demais,
    CAST(SUM(is_mei) AS SMALLINT) AS qtd_mei,
    CAST(COUNT(sk_estabelecimento) AS SMALLINT) AS qtd_estabelecimentos
FROM gold.fact_estabelecimentos e
GROUP BY
    e.sk_tempo_inicio_atividade,    
    e.sk_situacoes,
    e.sk_localidades,
    e.sk_naturezas_juridicas;

CREATE OR REFRESH MATERIALIZED VIEW gold.agg_fact_estabelecimentos_cnaes AS
SELECT
    bec.sk_cnae,
    e.sk_tempo_inicio_atividade,    
    e.sk_situacoes,
    e.sk_localidades,    
    bec.tipo_cnae, 
    CAST(COUNT(bec.sk_cnae) AS SMALLINT) AS qtd_cnaes
FROM gold.fact_estabelecimentos e
JOIN gold.bridge_estabelecimentos_cnaes bec 
    ON e.sk_estabelecimento = bec.sk_estabelecimento
GROUP BY
    e.sk_tempo_inicio_atividade,    
    e.sk_situacoes,
    e.sk_localidades,
    bec.sk_cnae,    
    bec.tipo_cnae;

CREATE OR REFRESH MATERIALIZED VIEW gold.bridge_estabelecimentos_cnaes AS
WITH cnaes AS (
    SELECT sk_estabelecimento, fk_cnae, tipo_cnae FROM silver.int_cnaes_principais
    UNION ALL
    SELECT sk_estabelecimento, fk_cnae, tipo_cnae FROM silver.int_cnaes_secundarios
)

SELECT 
    sk_estabelecimento,
    dc.sk_id AS sk_cnae,
    tipo_cnae,
    current_timestamp() AS data_processamento
FROM cnaes c
JOIN gold.dim_cnaes dc ON dc.codigo = c.fk_cnae;

CREATE OR REFRESH MATERIALIZED VIEW gold.bridge_estabelecimentos_socios AS
SELECT        
    e.sk_id AS sk_estabelecimento,
    s.sk_id AS sk_socio,        
    current_timestamp() AS data_processamento
FROM silver.int_estabelecimentos e
JOIN silver.int_empresas em ON em.cnpj_basico = e.cnpj_basico
JOIN silver.int_socios s ON s.cnpj_basico = em.cnpj_basico;

CREATE OR REFRESH MATERIALIZED VIEW gold.dim_cnaes AS
SELECT 
    CAST(ROW_NUMBER() OVER(ORDER BY codigo) AS SMALLINT) AS sk_id,
    codigo,
    codigo_formatado,
    descricao,
    current_timestamp() AS data_processamento 
FROM silver.int_cnaes;

CREATE OR REFRESH MATERIALIZED VIEW gold.dim_localidades AS
WITH localidades AS (
    SELECT
        DISTINCT
        CAST(DENSE_RANK() OVER(ORDER BY municipio,cidade_exterior,uf,pais) AS INTEGER) AS sk_id,
        municipio,
        cidade_exterior,
        uf,
        pais        
    FROM silver.int_estabelecimentos
)

SELECT
    l.sk_id,
    m.codigo AS codigo_municipio,
    m.descricao AS municipio,    
    ce.codigo AS codigo_cidade_exterior,
    ce.descricao AS cidade_exterior,    
    u.codigo AS codigo_uf,
    u.sigla AS uf,    
    u.regiao AS regiao,    
    p.codigo AS codigo_pais,
    p.descricao AS pais,
    current_timestamp() AS data_processamento
FROM localidades l
JOIN silver.int_municipios m ON m.codigo = l.municipio
JOIN silver.int_cidades_exterior ce ON ce.codigo = l.cidade_exterior
JOIN silver.int_ufs u ON u.codigo = l.uf
JOIN silver.int_paises p ON p.codigo = l.pais;

CREATE OR REFRESH MATERIALIZED VIEW gold.dim_naturezas_juridicas AS
SELECT 
    CAST(ROW_NUMBER() OVER(ORDER BY codigo) AS SMALLINT) AS sk_id,
    codigo,
    codigo_formatado,
    descricao,
    current_timestamp() AS data_processamento 
FROM silver.int_naturezas;

CREATE OR REFRESH MATERIALIZED VIEW gold.dim_situacoes AS
SELECT
    DISTINCT
    CAST(DENSE_RANK() OVER(ORDER BY e.identificador_matriz_filial,e.situacao_cadastral,e.motivo_situacao_cadastral,e.situacao_especial) AS INTEGER) AS sk_id,
    te.codigo AS codigo_identificador_matriz_filial,
    te.descricao AS identificador_matriz_filial,
    sc.codigo AS codigo_situacao_cadastral,    
    sc.descricao AS situacao_cadastral,    
    scm.codigo AS codigo_motivo_situacao_cadastral,
    scm.descricao AS motivo_situacao_cadastral,    
    se.codigo AS codigo_situacao_especial,
    se.descricao AS situacao_especial,        
    current_timestamp() AS data_processamento
FROM silver.int_estabelecimentos e
JOIN silver.int_tipos_estabelecimentos te ON te.codigo = e.identificador_matriz_filial
JOIN silver.int_situacoes_cadastrais sc ON sc.codigo = e.situacao_cadastral
JOIN silver.int_situacoes_cadastrais_motivos scm ON scm.codigo = e.motivo_situacao_cadastral
JOIN silver.int_situacoes_especiais se ON se.codigo = e.situacao_especial;

CREATE OR REFRESH MATERIALIZED VIEW gold.dim_socios AS
SELECT
    s.sk_id,
    s.cnpj_basico,
    s.identificador,
    tp.descricao as tipo_pessoa,
    s.nome_razao_social,
    s.cpf_cnpj,
    s.qualificacao,
    q.descricao AS descricao_qualificacao,
    s.data_entrada_sociedade,
    s.pais,
    p.descricao AS descricao_pais,
    representante_legal,
    s.nome_do_representante,
    s.qualificacao_representante,
    qr.descricao AS descricao_qualificacao_representante,
    s.faixa_etaria_socio,
    fe.descricao AS descricao_faixa_etaria_socio,
    current_timestamp() AS data_processamento 
FROM silver.int_socios s
JOIN silver.int_tipos_pessoas tp ON tp.codigo = s.identificador
JOIN silver.int_qualificacoes q ON q.codigo = s.qualificacao
JOIN silver.int_paises p ON p.codigo = s.pais
JOIN silver.int_qualificacoes qr ON qr.codigo = s.qualificacao_representante
JOIN silver.int_faixas_etarias_socios fe ON fe.codigo = s.faixa_etaria_socio;

CREATE OR REFRESH MATERIALIZED VIEW gold.dim_tempo_inicio_atividades AS
SELECT 
    sk_id,
    data_referencia,
    ano,
    mes,
    nome_mes,
    current_timestamp() AS data_processamento 
FROM silver.int_tempo_inicio_atividades;

CREATE OR REFRESH MATERIALIZED VIEW gold.dim_tempo_situacoes_cadastrais AS
SELECT 
    sk_id,
    data_referencia,
    ano,
    mes,
    nome_mes,
    current_timestamp() AS data_processamento 
FROM silver.int_tempo_situacoes_cadastrais;

CREATE OR REFRESH MATERIALIZED VIEW gold.dim_tempo_situacoes_especiais AS
SELECT 
    sk_id,
    data_referencia,
    ano,
    mes,
    nome_mes,
    current_timestamp() AS data_processamento 
FROM silver.int_tempo_situacoes_especiais;

CREATE OR REFRESH MATERIALIZED VIEW gold.fact_estabelecimentos AS
SELECT
    e.sk_id AS sk_estabelecimento,        
    COALESCE(CAST(DATE_FORMAT(e.data_inicio_atividade, 'yyyyMMdd') AS INT), -1) AS sk_tempo_inicio_atividade,    
    COALESCE(CAST(DATE_FORMAT(e.data_situacao_especial, 'yyyyMMdd') AS INT), -1) AS sk_tempo_situacoes_especiais,
    COALESCE(CAST(DATE_FORMAT(e.data_situacao_cadastral, 'yyyyMMdd') AS INT), -1) AS sk_tempo_situacoes_cadastrais,    
    s.sk_id AS sk_situacoes,
    l.sk_id AS sk_localidades,    
    nj.sk_id AS sk_naturezas_juridicas,
    CAST(CASE WHEN e.identificador_matriz_filial = 1 THEN 1 ELSE 0 END AS TINYINT) AS is_matriz,
    CAST(CASE WHEN e.identificador_matriz_filial = 2 THEN 1 ELSE 0 END AS TINYINT) AS is_filial,    
    CAST(CASE WHEN dc.codigo_porte_empresa = 1 THEN 1 ELSE 0 END AS TINYINT) AS is_me,
    CAST(CASE WHEN dc.codigo_porte_empresa = 3 THEN 1 ELSE 0 END AS TINYINT) AS is_epp,
    CAST(CASE WHEN dc.codigo_porte_empresa = 5 THEN 1 ELSE 0 END AS TINYINT) AS is_demais,
    CAST(CASE WHEN dc.opcao_simples = 'S' THEN 1 ELSE 0 END AS TINYINT) AS is_simples,
    CAST(CASE WHEN dc.opcao_mei = 'S' THEN 1 ELSE 0 END AS TINYINT) AS is_mei,
    CAST(CASE WHEN e.situacao_cadastral = 1 THEN 1 ELSE 0 END AS TINYINT) AS is_nula,
    CAST(CASE WHEN e.situacao_cadastral = 2 THEN 1 ELSE 0 END AS TINYINT) AS is_ativa,
    CAST(CASE WHEN e.situacao_cadastral = 3 THEN 1 ELSE 0 END AS TINYINT) AS is_suspensa,
    CAST(CASE WHEN e.situacao_cadastral = 4 THEN 1 ELSE 0 END AS TINYINT) AS is_inapta,
    CAST(CASE WHEN e.situacao_cadastral = 8 THEN 1 ELSE 0 END AS TINYINT) AS is_baixada,
    current_timestamp() AS data_processamento
FROM silver.int_estabelecimentos e
JOIN gold.dim_cadastro dc ON dc.sk_id = e.sk_id
JOIN gold.dim_naturezas_juridicas nj ON nj.codigo = dc.codigo_natureza_juridica
JOIN gold.dim_situacoes s ON s.codigo_identificador_matriz_filial = e.identificador_matriz_filial
    AND s.codigo_situacao_cadastral = e.situacao_cadastral
    AND s.codigo_motivo_situacao_cadastral = e.motivo_situacao_cadastral
    AND s.codigo_situacao_especial = e.situacao_especial
JOIN gold.dim_localidades l ON l.codigo_municipio = e.municipio
    AND l.codigo_cidade_exterior = e.cidade_exterior
    AND l.codigo_uf = e.uf
    AND l.codigo_pais = e.pais;

CREATE OR REFRESH MATERIALIZED VIEW gold.dim_cadastro AS
SELECT
    e.sk_id,    
    SUBSTR(e.cnpj_basico, 1, 2) || '.' || SUBSTR(e.cnpj_basico, 3, 3) || '.' || SUBSTR(e.cnpj_basico, 6, 3) || '/' || e.cnpj_ordem || '-' || e.cnpj_dv AS cnpj_completo,    
    te.descricao AS identificador_matriz_filial,
    e.data_inicio_atividade AS data_abertura,                
    em.razao_social,
    e.nome_fantasia,
    pe.codigo AS codigo_porte_empresa,
    pe.descricao AS porte_empresa,
    em.capital_social,
    nj.codigo AS codigo_natureza_juridica,   
    nj.codigo_formatado || ' - ' || nj.descricao AS natureza_juridica,    
    e.tipo_logradouro || ' ' || e.logradouro AS logradouro,    
    e.numero,    
    e.complemento,    
    CASE 
        WHEN e.cep IS NULL THEN e.cep
        ELSE SUBSTR(e.cep, 1, 5) || '-' || SUBSTR(e.cep, 6, 3)
    END AS cep,
    e.bairro,
    mu.descricao AS municipio,
    u.sigla AS uf,
    u.regiao,
    p.descricao AS pais,   
    ce.descricao AS cidade_exterior,
    e.email,
    CASE 
        WHEN e.ddd_1 IS NULL OR e.telefone_1 IS NULL THEN NULL
        ELSE '(' || e.ddd_1 || ') ' || 
            CASE 
                WHEN LENGTH(REGEXP_REPLACE(e.telefone_1, '\D', '', 'g')) = 9            
                    THEN REGEXP_REPLACE(REGEXP_REPLACE(e.telefone_1, '\D', '', 'g'), '^(\d{5})(\d{4})$', '\1-\2')
                WHEN LENGTH(REGEXP_REPLACE(e.telefone_1, '\D', '', 'g')) = 8             
                    THEN REGEXP_REPLACE(REGEXP_REPLACE(e.telefone_1, '\D', '', 'g'), '^(\d{4})(\d{4})$', '\1-\2')
                ELSE e.telefone_1
            END
    END AS telefone_1,
    CASE 
        WHEN e.ddd_2 IS NULL OR e.telefone_2 IS NULL THEN NULL
        ELSE '(' || e.ddd_2 || ') ' || 
            CASE 
                WHEN LENGTH(REGEXP_REPLACE(e.telefone_2, '\D', '', 'g')) = 9            
                    THEN REGEXP_REPLACE(REGEXP_REPLACE(e.telefone_2, '\D', '', 'g'), '^(\d{5})(\d{4})$', '\1-\2')
                WHEN LENGTH(REGEXP_REPLACE(e.telefone_2, '\D', '', 'g')) = 8             
                    THEN REGEXP_REPLACE(REGEXP_REPLACE(e.telefone_2, '\D', '', 'g'), '^(\d{4})(\d{4})$', '\1-\2')
                ELSE e.telefone_2
            END
    END AS telefone_2,
    CASE 
        WHEN e.ddd_fax IS NULL OR e.fax IS NULL THEN NULL
        ELSE '(' || e.ddd_fax || ') ' || 
            CASE 
                WHEN LENGTH(REGEXP_REPLACE(e.fax, '\D', '', 'g')) = 9             
                    THEN REGEXP_REPLACE(REGEXP_REPLACE(e.fax, '\D', '', 'g'), '^(\d{5})(\d{4})$', '\1-\2')
                WHEN LENGTH(REGEXP_REPLACE(e.fax, '\D', '', 'g')) = 8             
                    THEN REGEXP_REPLACE(REGEXP_REPLACE(e.fax, '\D', '', 'g'), '^(\d{4})(\d{4})$', '\1-\2')
                ELSE e.fax
            END
    END AS fax,  
    ef.descricao AS ente_federativo_responsavel,
    sc.descricao AS situacao_cadastral,
    scm.descricao AS motivo_situacao_cadastral,
    e.data_situacao_cadastral,
    se.descricao AS situacao_especial,
    e.data_situacao_especial,
    s.opcao_simples,
    s.opcao_mei,
    NOW() AS data_processamento
FROM silver.int_estabelecimentos e
JOIN silver.int_tipos_estabelecimentos te ON te.codigo = e.identificador_matriz_filial
JOIN silver.int_situacoes_cadastrais sc ON sc.codigo = e.situacao_cadastral
JOIN silver.int_situacoes_cadastrais_motivos scm ON scm.codigo = e.motivo_situacao_cadastral
JOIN silver.int_cidades_exterior ce ON ce.codigo = e.cidade_exterior
JOIN silver.int_paises p ON p.codigo = e.pais
JOIN silver.int_ufs u ON u.codigo = e.uf
JOIN silver.int_municipios mu ON mu.codigo = e.municipio
JOIN silver.int_situacoes_especiais se ON se.codigo = e.situacao_especial
JOIN silver.int_empresas em ON em.cnpj_basico = e.cnpj_basico
JOIN silver.int_naturezas nj ON nj.codigo = em.natureza_juridica
JOIN silver.int_portes_empresas pe ON pe.codigo = em.porte_empresa
JOIN silver.int_entes_federativos ef ON ef.codigo = em.ente_federativo_responsavel
LEFT JOIN silver.int_simples s ON s.cnpj_basico = e.cnpj_basico;