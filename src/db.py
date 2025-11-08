import os
from pathlib import Path
from urllib.parse import unquote
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Carrega variáveis do arquivo .env
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

def get_engine_from_env(env_var_name: str):
    """Cria um SQLAlchemy engine a partir de uma variável do arquivo .env contendo a URL de conexão.

    A string pode ser driver-agnóstica (ex: mssql+pyodbc://... ou postgresql+psycopg2://...)
    """
    url = os.getenv(env_var_name)
    if not url:
        raise RuntimeError(f"Variável {env_var_name} não definida no arquivo .env")
    
    # Para pymssql, precisamos passar parâmetros adicionais para Azure SQL
    if "pymssql" in url:
        # Criar engine com parâmetros específicos para Azure SQL
        return create_engine(
            url,
            connect_args={
                "timeout": 30,
                "login_timeout": 30,
                "tds_version": "7.4"
            }
        )
    
    return create_engine(url)

def ensure_controle_table(engine, tenant_id: str):
    """Cria a tabela CONTROLE_MIGRACAO_LEGADO no banco destino se não existir.

    Usa um SQL compatível com SQL Server e Postgres (tenta ambos).
    """
    create_sql = f"""
IF OBJECT_ID(N'dbo.CONTROLE_MIGRACAO_LEGADO', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.CONTROLE_MIGRACAO_LEGADO (
        Id INT IDENTITY(1,1) PRIMARY KEY,
        sCdTenant UNIQUEIDENTIFIER NOT NULL,
        sTabelaOrigem NVARCHAR(200) NOT NULL,
        sCampoChaveOrigem NVARCHAR(200) NOT NULL,
        sValorChaveOrigem NVARCHAR(200) NOT NULL,
        sTabelaDestino NVARCHAR(200) NOT NULL,
        sCampoChaveDestino NVARCHAR(200) NOT NULL,
        sValorChaveDestino NVARCHAR(200) NOT NULL,
        dtMigracao DATETIME NOT NULL DEFAULT(GETDATE())
    );
END
"""
    # Alguns bancos (Postgres) não aceitam IF OBJECT_ID; tentar criar de forma genérica se falhar
    with engine.begin() as conn:
        try:
            conn.execute(text(create_sql))
        except Exception:
            # Fallback genérico: CREATE TABLE IF NOT EXISTS (Postgres)
            create_sql_pg = f"""
CREATE TABLE IF NOT EXISTS controle_migracao_legacy (
    id SERIAL PRIMARY KEY,
    scdtenant UUID NOT NULL,
    stabelaorigem TEXT NOT NULL,
    scampochaveorigem TEXT NOT NULL,
    svalorchaveorigem TEXT NOT NULL,
    stabeladestino TEXT NOT NULL,
    scampochavedestino TEXT NOT NULL,
    svalorchavedestino TEXT NOT NULL,
    dtmigracao TIMESTAMP NOT NULL DEFAULT now()
);
"""
            conn.execute(text(create_sql_pg))
