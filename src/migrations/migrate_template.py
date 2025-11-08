"""
Template para criar novas migrações

Copie este arquivo e adapte para sua entidade.
Exemplo: cp migrate_template.py migrate_pets.py
"""
import uuid
from datetime import datetime
from sqlalchemy import text
from common.db_utils import get_engine_from_env, ensure_controle_table, insert_controle, get_tenant_id


def map_origem_to_destino(row, tenant_id: str):
    """
    Mapeia um registro da tabela origem para a tabela destino.
    
    Args:
        row: Dicionário com os dados da tabela origem
        tenant_id: ID do tenant
    
    Returns:
        dict: Dados mapeados para inserção na tabela destino
    """
    def safe(val, default=""):
        return default if val is None else val
    
    # TODO: Implementar mapeamento específico
    # Exemplo:
    # sCdRegistro = str(uuid.uuid4())
    # sNomeCampo = safe(row.get("CampoOrigem"), "")
    
    return {
        # "campo_destino": valor_mapeado,
    }


def insert_or_update_destino(dest_engine, registro: dict, dry_run: bool = False):
    """
    Insere ou atualiza registro na tabela destino.
    
    Verifica se o registro já existe (por chave única como documento, código, etc).
    Se existir, atualiza. Caso contrário, insere novo.
    
    Args:
        dest_engine: Engine do banco destino
        registro: Dicionário com dados do registro
        dry_run: Se True, apenas simula (não insere/atualiza)
    
    Returns:
        str: ID do registro inserido/atualizado
    """
    # TODO: Implementar SQL de verificação (adapte conforme chave única da tabela)
    check_sql = text(
        """
SELECT sCdPrimary FROM TABELA_DESTINO 
WHERE sCdTenant = :sCdTenant AND sCampoUnico = :sCampoUnico
"""
    )
    
    # TODO: Implementar SQL de update
    update_sql = text(
        """
UPDATE TABELA_DESTINO SET
    campo1 = :campo1,
    campo2 = :campo2,
    campo3 = :campo3
WHERE sCdPrimary = :sCdPrimary
"""
    )
    
    # TODO: Implementar SQL de insert
    insert_sql = text(
        """
INSERT INTO TABELA_DESTINO (sCdPrimary, campo1, campo2, campo3)
VALUES (:sCdPrimary, :campo1, :campo2, :campo3)
"""
    )

    if dry_run:
        print(f"  [dry-run] DESTINO: {registro}")
        return registro.get("sCdPrimary", str(uuid.uuid4()))

    with dest_engine.begin() as conn:
        # TODO: Adaptar verificação conforme sua tabela
        # result = conn.execute(check_sql, {"sCdTenant": registro["sCdTenant"], "sCampoUnico": registro["sCampoUnico"]})
        # existing = result.fetchone()
        existing = None  # Remova esta linha e descomente acima
        
        if existing:
            # Atualizar registro existente
            scd_existente = str(existing[0])
            registro_update = registro.copy()
            registro_update["sCdPrimary"] = scd_existente
            
            conn.execute(update_sql, registro_update)
            print(f"  ✓ Atualizado: {registro.get('nome_campo_exibicao', scd_existente)}")
            
            return scd_existente
        else:
            # Inserir novo registro
            conn.execute(insert_sql, registro)
            print(f"  ✓ Inserido: {registro.get('nome_campo_exibicao', registro['sCdPrimary'])}")
    
    return registro["sCdPrimary"]


def migrate_entidade(batch_size=500, dry_run=False):
    """
    Executa a migração da entidade.
    
    Args:
        batch_size: Quantidade de registros por batch
        dry_run: Se True, apenas simula (não insere)
    
    Returns:
        int: Total de registros processados
    """
    print("\n" + "="*60)
    print("MIGRAÇÃO: TABELA_ORIGEM -> TABELA_DESTINO")
    print("="*60 + "\n")
    
    legacy_engine = get_engine_from_env("LEGACY_DB_URL")
    dest_engine = get_engine_from_env("DEST_DB_URL")
    tenant_id = get_tenant_id()

    # Garantir que a tabela de controle exista
    ensure_controle_table(dest_engine, tenant_id)

    # TODO: Ajustar SQL de leitura da origem
    select_sql = text("SELECT * FROM TABELA_ORIGEM ORDER BY CampoPK")

    total = 0

    with legacy_engine.connect() as src_conn:
        result = src_conn.execution_options(stream_results=True).execute(select_sql)
        while True:
            rows = result.fetchmany(batch_size)
            if not rows:
                break
            for r in rows:
                row = dict(r._mapping)
                
                # Mapear
                registro = map_origem_to_destino(row, tenant_id)
                
                # Inserir ou atualizar
                sCdDestino = insert_or_update_destino(dest_engine, registro, dry_run=dry_run)
                
                # Registrar mapeamento na tabela de controle
                chave_origem = str(row.get("CampoPK"))
                insert_controle(
                    dest_engine, tenant_id, 
                    "TABELA_ORIGEM", "CampoPK", chave_origem,
                    "TABELA_DESTINO", "sCdPrimary", sCdDestino,
                    dry_run=dry_run
                )
                total += 1
            
            if not dry_run:
                print(f"Migrados: {total}")

    print("\n" + "="*60)
    print(f"✓ Migração finalizada!")
    print(f"  Total processado: {total}")
    print("="*60 + "\n")
    
    return total
