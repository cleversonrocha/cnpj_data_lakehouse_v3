import os
from pyspark import pipelines as dp

VOLUME_PATH = spark.conf.get("OUTPUT_FOLDER")

if not VOLUME_PATH or not os.path.exists(VOLUME_PATH):
    raise FileNotFoundError(f"O diretório configurado em OUTPUT_FOLDER não existe ou está vazio: '{VOLUME_PATH}'")

COMENTARIOS_TABELAS = {
    "Empresas": "Empresas (Dados cadastrais básicos)",
    "Estabelecimentos": "Estabelecimentos (Filiais, endereços e situação cadastral)",
    "Socios": "Sócios (Quadro societário e administradores)",
    "Simples": "Simples Nacional e MEI",
    "Cnaes": "Tabela de domínio - CNAEs",
    "Motivos": "Tabela de domínio - Motivos de situação cadastral",
    "Municipios": "Tabela de domínio - Municípios",
    "Naturezas": "Tabela de domínio - Naturezas jurídicas",
    "Paises": "Tabela de domínio - Países",
    "Qualificacoes": "Tabela de domínio - Qualificações de sócios",
}


def registrar_tabela_bronze(prefixo: str) -> None:
    nome_tabela = prefixo.lower()
    comentario = COMENTARIOS_TABELAS.get(prefixo, f"Tabela bruta: {prefixo}")

    @dp.table(
        name=nome_tabela,
        comment=comentario,
        table_properties={"quality": "bronze"},
    )
    def _tabela():
        return (
            spark.read.format("csv")
            .option("header", "false")
            .option("sep", ";")
            .option("quote", '"')
            .option("escape", '"')
            .option("multiLine", "true")
            .option("encoding", "ISO-8859-1")
            .option("pathGlobFilter", f"{prefixo}*.csv")
            .load(VOLUME_PATH)
        )


for prefixo in sorted(COMENTARIOS_TABELAS.keys()):
    registrar_tabela_bronze(prefixo)
