import os
import uuid
from datetime import datetime
import argparse
from pathlib import Path
from sqlalchemy import text
from dotenv import load_dotenv
from db import get_engine_from_env, ensure_controle_table

# Carrega variáveis do arquivo .env
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

DEFAULT_TENANT = os.getenv("DEFAULT_TENANT", "dfedd5f4-f30c-45ea-bc1e-695081d8415c")
DEFAULT_CITY_ID = os.getenv("DEFAULT_CITY_ID", "00000000-0000-0000-0000-000000000001")


def map_cliente_to_pessoa(row, tenant_id: str):
    """Mapeia um registro de PET_CLIENTE para PESSOA.

    row: um Mapping retornado pelo SQLAlchemy (colunas por nome).
    Retorna um dict com os campos do destino.
    """
    def safe(val, default=""):
        return default if val is None else val

    sCdPessoa = str(uuid.uuid4())
    tipo = row.get("Tipo")
    id_fj = "F" if tipo == 1 or str(tipo) == "1" else "J"

    # Campos obrigatórios do destino: sCdTenant, sCdPessoa, sNmPessoa, sNrDoc, sIdFisicaJuridica,
    # sDsEmail (não nulo), sDsEndereco, nNrEndereco, sNmBairro, nNrCep, sCdCidade, bFlAtivo, tDtCadastro
    sNmPessoa = safe(row.get("Nome"), "")
    sNrDoc = safe(row.get("Documento"), "")
    sDsEmail = safe(row.get("Email"), "")
    sNrTelefone1 = safe(row.get("Telefone1"), None)
    sNrTelefone2 = safe(row.get("Telefone2"), None)
    sDsEndereco = safe(row.get("Endereco"), "")
    numero = row.get("Numero")
    try:
        nNrEndereco = int(numero) if numero is not None else 0
    except Exception:
        nNrEndereco = 0
    sDsComplemento = safe(row.get("Complemento"), None)
    sNmBairro = safe(row.get("Bairro"), "")
    nNrCep = safe(row.get("CEP"), "")
    sCdCidade = os.getenv("DEFAULT_CITY_ID", DEFAULT_CITY_ID)
    sDsObservacoes = safe(row.get("Observacoes"), None)
    ativo = row.get("Ativo")
    try:
        bFlAtivo = bool(int(ativo))
    except Exception:
        bFlAtivo = True
    dt = row.get("DataCadastro") or row.get("DataNascimento")
    if isinstance(dt, datetime):
        tDtCadastro = dt
    else:
        try:
            tDtCadastro = datetime.fromisoformat(str(dt))
        except Exception:
            tDtCadastro = datetime.utcnow()

    return {
        "sCdTenant": tenant_id,
        "sCdPessoa": sCdPessoa,
        "sNmPessoa": sNmPessoa,
        "sNmFantasia": None,
        "sNrDoc": sNrDoc,
        "sIdFisicaJuridica": id_fj,
        "sDsEmail": sDsEmail,
        "sNrTelefone1": sNrTelefone1,
        "sNrTelefone2": sNrTelefone2,
        "sDsEndereco": sDsEndereco,
        "nNrEndereco": nNrEndereco,
        "sDsComplemento": sDsComplemento,
        "sNmBairro": sNmBairro,
        "nNrCep": nNrCep,
        "sCdCidade": sCdCidade,
        "sDsObservacoes": sDsObservacoes,
        "bFlAtivo": bFlAtivo,
        "tDtCadastro": tDtCadastro,
    }


def insert_pessoa(dest_engine, pessoa: dict, dry_run: bool = False):
    # Verificar se pessoa já existe (por documento + tenant)
    check_sql = text(
        """
SELECT sCdPessoa FROM PESSOA 
WHERE sCdTenant = :sCdTenant AND sNrDoc = :sNrDoc
"""
    )
    
    insert_sql = text(
        """
INSERT INTO PESSOA (
    sCdTenant, sCdPessoa, sNmPessoa, sNmFantasia, sNrDoc, sIdFisicaJuridica,
    sDsEmail, sNrTelefone1, sNrTelefone2, sDsEndereco, nNrEndereco, sDsComplemento,
    sNmBairro, nNrCep, sCdCidade, sDsObservacoes, bFlAtivo, tDtCadastro
)
VALUES (
    :sCdTenant, :sCdPessoa, :sNmPessoa, :sNmFantasia, :sNrDoc, :sIdFisicaJuridica,
    :sDsEmail, :sNrTelefone1, :sNrTelefone2, :sDsEndereco, :nNrEndereco, :sDsComplemento,
    :sNmBairro, :nNrCep, :sCdCidade, :sDsObservacoes, :bFlAtivo, :tDtCadastro
)
"""
    )
    
    insert_tipo_sql = text(
        """
INSERT INTO PESSOA_TIPO (sCdPessoaTipo, sCdPessoa, nCdTipo, tDtAssociacao, bFlAtivo)
VALUES (NEWID(), :sCdPessoa, 2, GETDATE(), 1)
"""
    )

    if dry_run:
        print("[dry-run] insert_pessoa:", pessoa)
        print(f"[dry-run] insert_pessoa_tipo: sCdPessoa={pessoa['sCdPessoa']}, nCdTipo=2")
        return pessoa["sCdPessoa"]

    with dest_engine.begin() as conn:
        # Verificar se já existe
        result = conn.execute(check_sql, {"sCdTenant": pessoa["sCdTenant"], "sNrDoc": pessoa["sNrDoc"]})
        existing = result.fetchone()
        
        if existing:
            # Pessoa já existe, retornar o ID existente
            print(f"  ⚠ Pessoa com doc {pessoa['sNrDoc']} já existe, pulando...")
            return str(existing[0])
        
        # Inserir nova pessoa
        conn.execute(insert_sql, pessoa)
        conn.execute(insert_tipo_sql, {"sCdPessoa": pessoa["sCdPessoa"]})
    
    return pessoa["sCdPessoa"]


def insert_controle(dest_engine, tenant_id: str, origem_table: str, campo_chave_origem: str, valor_chave_origem: str, destino_table: str, campo_chave_destino: str, valor_chave_destino: str, dry_run: bool = False):
    # Tentar inserir na tabela SQL Server nomeada; se falhar, tentar na versão postgres
    insert_sql_mssql = text(
        """
INSERT INTO dbo.CONTROLE_MIGRACAO_LEGADO (sCdTenant, sTabelaOrigem, sCampoChaveOrigem, sValorChaveOrigem, sTabelaDestino, sCampoChaveDestino, sValorChaveDestino)
VALUES (:tenant, :torig, :corig, :vorig, :tdest, :cdest, :vdest)
"""
    )
    insert_sql_pg = text(
        """
INSERT INTO controle_migracao_legacy (scdtenant, stabelaorigem, scampochaveorigem, svalorchaveorigem, stabeladestino, scampochavedestino, svalorchavedestino)
VALUES (:tenant, :torig, :corig, :vorig, :tdest, :cdest, :vdest)
"""
    )

    params = {
        "tenant": tenant_id,
        "torig": origem_table,
        "corig": campo_chave_origem,
        "vorig": valor_chave_origem,
        "tdest": destino_table,
        "cdest": campo_chave_destino,
        "vdest": valor_chave_destino,
    }

    if dry_run:
        print("[dry-run] insert_controle:", params)
        return

    with dest_engine.begin() as conn:
        try:
            conn.execute(insert_sql_mssql, params)
        except Exception:
            conn.execute(insert_sql_pg, params)


def migrate(args):
    legacy_engine = get_engine_from_env("LEGACY_DB_URL")
    dest_engine = get_engine_from_env("DEST_DB_URL")

    tenant_id = args.tenant or DEFAULT_TENANT

    # Garantir que a tabela de controle exista
    ensure_controle_table(dest_engine, tenant_id)

    # Ler clientes do legado
    select_sql = text("SELECT * FROM PET_CLIENTE ORDER BY Codigo")

    batch_size = args.batch_size or 500
    total = 0

    with legacy_engine.connect() as src_conn:
        result = src_conn.execution_options(stream_results=True).execute(select_sql)
        while True:
            rows = result.fetchmany(batch_size)
            if not rows:
                break
            for r in rows:
                # Converter row para dict usando _mapping (compatível com SQLAlchemy 1.4+)
                row = dict(r._mapping)
                pessoa = map_cliente_to_pessoa(row, tenant_id)
                sCdPessoa = insert_pessoa(dest_engine, pessoa, dry_run=args.dry_run)
                # registrar mapeamento: PET_CLIENTE.Codigo -> PESSOA.sCdPessoa
                chave_origem = str(row.get("Codigo"))
                insert_controle(dest_engine, tenant_id, "PET_CLIENTE", "Codigo", chave_origem, "PESSOA", "sCdPessoa", sCdPessoa, dry_run=args.dry_run)
                total += 1
            print(f"Migrados: {total}")

    print(f"Migração finalizada. Total de registros migrados: {total}")


def main():
    parser = argparse.ArgumentParser(description="Migrar PET_CLIENTE -> PESSOA")
    parser.add_argument("--tenant", help="sCdTenant a usar (UUID)")
    parser.add_argument("--batch-size", type=int, help="Tamanho do batch de leitura")
    parser.add_argument("--dry-run", action="store_true", help="Não insere, apenas mostra operações")
    args = parser.parse_args()
    migrate(args)


if __name__ == "__main__":
    main()
