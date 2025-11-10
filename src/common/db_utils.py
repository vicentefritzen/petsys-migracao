import os
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Carrega variáveis do arquivo .env
env_path = Path(__file__).parent.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

DEFAULT_TENANT = os.getenv("DEFAULT_TENANT", "dfedd5f4-f30c-45ea-bc1e-695081d8415c")
DEFAULT_CITY_ID = os.getenv("DEFAULT_CITY_ID", "b6099443-d5c4-5e2c-8b53-4bd1c02b9793")
DEFAULT_VET_USER_ID = os.getenv("DEFAULT_VET_USER_ID", "f7cc3d41-12e9-4247-a828-69cfeeb52a74")


def get_engine_from_env(env_var_name: str):
    """Cria um SQLAlchemy engine a partir de uma variável do arquivo .env."""
    url = os.getenv(env_var_name)
    if not url:
        raise RuntimeError(f"Variável {env_var_name} não definida no arquivo .env")
    
    # Para pymssql, precisamos passar parâmetros adicionais para Azure SQL
    if "pymssql" in url:
        return create_engine(
            url,
            connect_args={
                "timeout": 300,  # 5 minutos timeout para operações pesadas
                "login_timeout": 30,
                "tds_version": "7.4"
            }
        )
    
    return create_engine(url)


def ensure_controle_table(engine, tenant_id: str):
    """Cria a tabela CONTROLE_MIGRACAO_LEGADO no banco destino se não existir."""
    create_sql = """
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
    with engine.begin() as conn:
        conn.execute(text(create_sql))


def insert_controle(dest_engine, tenant_id: str, origem_table: str, campo_chave_origem: str, 
                   valor_chave_origem: str, destino_table: str, campo_chave_destino: str, 
                   valor_chave_destino: str):
    """Registra mapeamento na tabela de controle."""
    insert_sql = text("""
INSERT INTO dbo.CONTROLE_MIGRACAO_LEGADO (
    sCdTenant, sTabelaOrigem, sCampoChaveOrigem, sValorChaveOrigem, 
    sTabelaDestino, sCampoChaveDestino, sValorChaveDestino
)
VALUES (:tenant, :torig, :corig, :vorig, :tdest, :cdest, :vdest)
""")

    params = {
        "tenant": tenant_id,
        "torig": origem_table,
        "corig": campo_chave_origem,
        "vorig": valor_chave_origem,
        "tdest": destino_table,
        "cdest": campo_chave_destino,
        "vdest": valor_chave_destino,
    }

    with dest_engine.begin() as conn:
        conn.execute(insert_sql, params)


def get_tenant_id():
    """Retorna o tenant ID padrão do .env."""
    return DEFAULT_TENANT


def get_default_city_id():
    """Retorna o ID da cidade padrão do .env."""
    return DEFAULT_CITY_ID


def get_default_vet_user_id():
    """Retorna o ID do usuário veterinário padrão do .env."""
    return DEFAULT_VET_USER_ID
