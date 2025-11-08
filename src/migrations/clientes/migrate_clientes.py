"""
Migração de Clientes (PET_CLIENTE -> PESSOA)
"""
import uuid
from datetime import datetime
from sqlalchemy import text
from common.db_utils import get_engine_from_env, ensure_controle_table, insert_controle, get_tenant_id, get_default_city_id


def map_cliente_to_pessoa(row, tenant_id: str):
    """Mapeia um registro de PET_CLIENTE para PESSOA."""
    def safe(val, default=""):
        return default if val is None else val

    sCdPessoa = str(uuid.uuid4())
    tipo = row.get("Tipo")
    id_fj = "F" if tipo == 1 or str(tipo) == "1" else "J"

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
    sCdCidade = get_default_city_id()
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


def insert_or_update_pessoa(dest_engine, pessoa: dict):
    """Insere ou atualiza pessoa na tabela PESSOA e PESSOA_TIPO.
    
    Se o documento (CPF/CNPJ) já existir para o tenant, atualiza os dados.
    Caso contrário, insere novo registro.
    """
    check_sql = text(
        """
SELECT sCdPessoa FROM PESSOA 
WHERE sCdTenant = :sCdTenant AND sNrDoc = :sNrDoc
"""
    )
    
    update_sql = text(
        """
UPDATE PESSOA SET
    sNmPessoa = :sNmPessoa,
    sNmFantasia = :sNmFantasia,
    sIdFisicaJuridica = :sIdFisicaJuridica,
    sDsEmail = :sDsEmail,
    sNrTelefone1 = :sNrTelefone1,
    sNrTelefone2 = :sNrTelefone2,
    sDsEndereco = :sDsEndereco,
    nNrEndereco = :nNrEndereco,
    sDsComplemento = :sDsComplemento,
    sNmBairro = :sNmBairro,
    nNrCep = :nNrCep,
    sCdCidade = :sCdCidade,
    sDsObservacoes = :sDsObservacoes,
    bFlAtivo = :bFlAtivo,
    tDtCadastro = :tDtCadastro
WHERE sCdPessoa = :sCdPessoa
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
    
    check_tipo_sql = text(
        """
SELECT COUNT(*) FROM PESSOA_TIPO 
WHERE sCdPessoa = :sCdPessoa AND nCdTipo = 2
"""
    )

    with dest_engine.begin() as conn:
        # Verificar se já existe
        result = conn.execute(check_sql, {"sCdTenant": pessoa["sCdTenant"], "sNrDoc": pessoa["sNrDoc"]})
        existing = result.fetchone()
        
        if existing:
            # Atualizar registro existente
            scd_pessoa_existente = str(existing[0])
            pessoa_update = pessoa.copy()
            pessoa_update["sCdPessoa"] = scd_pessoa_existente
            
            conn.execute(update_sql, pessoa_update)
            
            # Verificar se já tem o tipo CLIENTE (nCdTipo=2)
            result_tipo = conn.execute(check_tipo_sql, {"sCdPessoa": scd_pessoa_existente})
            count_tipo = result_tipo.fetchone()[0]
            
            if count_tipo == 0:
                conn.execute(insert_tipo_sql, {"sCdPessoa": scd_pessoa_existente})
            
            return scd_pessoa_existente
        else:
            # Inserir nova pessoa
            conn.execute(insert_sql, pessoa)
            conn.execute(insert_tipo_sql, {"sCdPessoa": pessoa["sCdPessoa"]})
    
    return pessoa["sCdPessoa"]


def migrate_clientes(batch_size=500):
    """Executa a migração de clientes."""
    print("\n" + "="*60)
    print("MIGRAÇÃO: PET_CLIENTE -> PESSOA")
    print("="*60 + "\n")
    
    legacy_engine = get_engine_from_env("LEGACY_DB_URL")
    dest_engine = get_engine_from_env("DEST_DB_URL")
    tenant_id = get_tenant_id()

    # Garantir que a tabela de controle exista
    ensure_controle_table(dest_engine, tenant_id)

    # Ler clientes do legado
    select_sql = text("SELECT * FROM PET_CLIENTE ORDER BY Codigo")

    total = 0

    with legacy_engine.connect() as src_conn:
        result = src_conn.execution_options(stream_results=True).execute(select_sql)
        while True:
            rows = result.fetchmany(batch_size)
            if not rows:
                break
            for r in rows:
                row = dict(r._mapping)
                codigo = str(row.get("Codigo"))
                nome = row.get("Nome", "SEM NOME")
                
                print(f"[{total + 1}] {nome}...", end=" ", flush=True)
                
                pessoa = map_cliente_to_pessoa(row, tenant_id)
                sCdPessoa = insert_or_update_pessoa(dest_engine, pessoa)
                
                print("✓")
                
                # Registrar mapeamento
                insert_controle(dest_engine, tenant_id, "PET_CLIENTE", "Codigo", codigo, 
                              "PESSOA", "sCdPessoa", sCdPessoa)
                total += 1
            
            if total % 100 == 0:
                print(f"\n>>> Progresso: {total} clientes processados...")

    print("\n" + "="*60)
    print(f"✓ Migração finalizada!")
    print(f"  Total processado: {total}")
    print("="*60 + "\n")
    
    return total
