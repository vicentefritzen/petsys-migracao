"""
Migração de Vacinas
PET_VACINA (origem) -> VACINA (destino)

Migra o cadastro de vacinas do sistema legado para o novo sistema.
"""
import sys
from pathlib import Path

# Adicionar src ao path para imports funcionarem
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import uuid
from datetime import datetime
from sqlalchemy import text
from common.db_utils import get_engine_from_env, ensure_controle_table, insert_controle, get_tenant_id


def map_origem_to_destino(row, tenant_id: str):
    """
    Mapeia um registro da tabela PET_VACINA (origem) para VACINA (destino).
    
    Mapeamento:
    - Codigo -> (controle)
    - Descricao -> sNmVacina
    - Frequencia -> nNrFrequencia
    - Periodo -> nCdPeriodicidade
    - PrecoCompra -> nVlPrecoCompra
    - PrecoVenda -> nVlPrecoVenda
    - (fixo) -> nCdEspecie = 1 (CANINA)
    - (fixo) -> nPcDescontoMensalista = 0
    - (fixo) -> bFlInclusoPlanoMensalista = 0 (False)
    - (fixo) -> bFlAtivo = 1 (True)
    
    Args:
        row: Dicionário com os dados da tabela origem
        tenant_id: ID do tenant
    
    Returns:
        dict: Dados mapeados para inserção na tabela destino
    """
    def safe(val, default=""):
        return default if val is None else val
    
    def safe_int(val, default=0):
        if val is None:
            return default
        try:
            return int(val)
        except (ValueError, TypeError):
            return default
    
    def safe_decimal(val, default=0.0):
        if val is None:
            return default
        try:
            return float(val)
        except (ValueError, TypeError):
            return default
    
    sCdVacina = str(uuid.uuid4())
    sNmVacina = safe(row.get("Descricao"), "").strip()
    
    # Frequência e Periodicidade
    nNrFrequencia = safe_int(row.get("Frequencia"), 1)
    nCdPeriodicidade = safe_int(row.get("Periodo"), 1)
    
    # Preços
    nVlPrecoCompra = safe_decimal(row.get("PrecoCompra"), 0.0)
    nVlPrecoVenda = safe_decimal(row.get("PrecoVenda"), 0.0)
    
    # Campos fixos conforme especificado
    nCdEspecie = 1  # CANINA (padrão)
    nPcDescontoMensalista = 0.0
    bFlInclusoPlanoMensalista = False
    bFlAtivo = True
    
    # Timestamps
    tDtCadastro = datetime.now()
    tDtUltimaAlteracao = datetime.now()
    
    return {
        "sCdTenant": tenant_id,
        "sCdVacina": sCdVacina,
        "sNmVacina": sNmVacina,
        "nCdEspecie": nCdEspecie,
        "nNrFrequencia": nNrFrequencia,
        "nCdPeriodicidade": nCdPeriodicidade,
        "nVlPrecoCompra": nVlPrecoCompra,
        "nVlPrecoVenda": nVlPrecoVenda,
        "nPcDescontoMensalista": nPcDescontoMensalista,
        "bFlInclusoPlanoMensalista": bFlInclusoPlanoMensalista,
        "bFlAtivo": bFlAtivo,
        "tDtCadastro": tDtCadastro,
        "tDtUltimaAlteracao": tDtUltimaAlteracao,
    }


def insert_or_update_vacina(dest_engine, registro: dict, dry_run: bool = False):
    """
    Insere ou atualiza registro na tabela VACINA.
    
    Verifica se a vacina já existe (por nome + tenant).
    Se existir, atualiza. Caso contrário, insere novo.
    
    Args:
        dest_engine: Engine do banco destino
        registro: Dicionário com dados do registro
        dry_run: Se True, apenas simula (não insere/atualiza)
    
    Returns:
        str: ID da vacina inserida/atualizada
    """
    check_sql = text(
        """
SELECT sCdVacina FROM VACINA 
WHERE sCdTenant = :sCdTenant 
  AND UPPER(LTRIM(RTRIM(sNmVacina))) = UPPER(LTRIM(RTRIM(:sNmVacina)))
"""
    )
    
    update_sql = text(
        """
UPDATE VACINA SET
    nCdEspecie = :nCdEspecie,
    nNrFrequencia = :nNrFrequencia,
    nCdPeriodicidade = :nCdPeriodicidade,
    nVlPrecoCompra = :nVlPrecoCompra,
    nVlPrecoVenda = :nVlPrecoVenda,
    nPcDescontoMensalista = :nPcDescontoMensalista,
    bFlInclusoPlanoMensalista = :bFlInclusoPlanoMensalista,
    bFlAtivo = :bFlAtivo,
    tDtUltimaAlteracao = :tDtUltimaAlteracao
WHERE sCdVacina = :sCdVacina
"""
    )
    
    insert_sql = text(
        """
INSERT INTO VACINA (
    sCdTenant, sCdVacina, sNmVacina, nCdEspecie, 
    nNrFrequencia, nCdPeriodicidade, nVlPrecoCompra, nVlPrecoVenda,
    nPcDescontoMensalista, bFlInclusoPlanoMensalista, bFlAtivo,
    tDtCadastro, tDtUltimaAlteracao
)
VALUES (
    :sCdTenant, :sCdVacina, :sNmVacina, :nCdEspecie,
    :nNrFrequencia, :nCdPeriodicidade, :nVlPrecoCompra, :nVlPrecoVenda,
    :nPcDescontoMensalista, :bFlInclusoPlanoMensalista, :bFlAtivo,
    :tDtCadastro, :tDtUltimaAlteracao
)
"""
    )

    if dry_run:
        print(f"  [dry-run] VACINA: {registro['sNmVacina']}")
        return registro["sCdVacina"]

    with dest_engine.begin() as conn:
        # Verificar se já existe
        result = conn.execute(check_sql, {
            "sCdTenant": registro["sCdTenant"],
            "sNmVacina": registro["sNmVacina"]
        })
        existing = result.fetchone()
        
        if existing:
            # Atualizar registro existente
            scd_existente = str(existing[0])
            registro_update = registro.copy()
            registro_update["sCdVacina"] = scd_existente
            
            conn.execute(update_sql, registro_update)
            print(f"  ✓ Atualizado: {registro['sNmVacina']}")
            
            return scd_existente
        else:
            # Inserir novo registro
            conn.execute(insert_sql, registro)
            print(f"  ✓ Inserido: {registro['sNmVacina']}")
    
    return registro["sCdVacina"]


def migrate_vacinas(batch_size=500, dry_run=False):
    """
    Executa a migração de vacinas.
    
    Args:
        batch_size: Quantidade de registros por batch
        dry_run: Se True, apenas simula (não insere)
    
    Returns:
        dict: Estatísticas da migração
    """
    print("\n" + "="*60)
    print("MIGRAÇÃO: PET_VACINA (origem) -> VACINA (destino)")
    print("="*60 + "\n")
    
    legacy_engine = get_engine_from_env("LEGACY_DB_URL")
    dest_engine = get_engine_from_env("DEST_DB_URL")
    tenant_id = get_tenant_id()

    # Garantir que a tabela de controle exista
    ensure_controle_table(dest_engine, tenant_id)

    # Ler vacinas da origem
    select_sql = text("SELECT * FROM PET_VACINA ORDER BY Codigo")

    total = 0
    inseridos = 0
    atualizados = 0

    with legacy_engine.connect() as src_conn:
        result = src_conn.execution_options(stream_results=True).execute(select_sql)
        
        while True:
            rows = result.fetchmany(batch_size)
            if not rows:
                break
            
            for r in rows:
                row = dict(r._mapping)
                codigo_origem = str(row.get("Codigo"))
                
                print(f"[{total + 1}] Processando: {row.get('Descricao')} (Código: {codigo_origem})")
                
                # Mapear
                registro = map_origem_to_destino(row, tenant_id)
                
                # Verificar se já existe (para estatísticas)
                if not dry_run:
                    with dest_engine.begin() as conn:
                        check = conn.execute(
                            text("SELECT sCdVacina FROM VACINA WHERE sCdTenant = :t AND UPPER(LTRIM(RTRIM(sNmVacina))) = UPPER(LTRIM(RTRIM(:n)))"),
                            {"t": tenant_id, "n": registro["sNmVacina"]}
                        )
                        exists = check.fetchone() is not None
                
                # Inserir ou atualizar
                sCdVacina = insert_or_update_vacina(dest_engine, registro, dry_run=dry_run)
                
                # Atualizar estatísticas
                if not dry_run:
                    if exists:
                        atualizados += 1
                    else:
                        inseridos += 1
                
                # Registrar mapeamento na tabela de controle (apenas se não for dry-run)
                if not dry_run:
                    insert_controle(
                        dest_engine, tenant_id,
                        "PET_VACINA", "Codigo", codigo_origem,
                        "VACINA", "sCdVacina", sCdVacina
                    )
                
                total += 1

    print("\n" + "="*60)
    print("✓ Migração finalizada!")
    print(f"  Total processado: {total}")
    if not dry_run:
        print(f"  Inseridos: {inseridos}")
        print(f"  Atualizados: {atualizados}")
    print("="*60 + "\n")
    
    return {
        "total": total,
        "inseridos": inseridos,
        "atualizados": atualizados
    }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Migração de Vacinas")
    parser.add_argument("--dry-run", action="store_true", help="Executar em modo simulação")
    parser.add_argument("--batch-size", type=int, default=500, help="Tamanho do batch")
    
    args = parser.parse_args()
    
    migrate_vacinas(batch_size=args.batch_size, dry_run=args.dry_run)
