"""
Script para exclusão de dados migrados

Exclui dados migrados na ordem correta para evitar problemas de foreign key:
1. Aplicações de Vacinas (PET_VACINA)
2. Pesos (PET_PESO)
3. Vacinas (VACINA)
4. Pets (PET)
5. Clientes (PESSOA)
6. Registros de controle (CONTROLE_MIGRACAO_LEGADO)
"""
import sys
from pathlib import Path

# Adicionar src ao path para imports funcionarem
sys.path.insert(0, str(Path(__file__).parent))

from sqlalchemy import text
from common.db_utils import get_engine_from_env, get_tenant_id


def get_counts(dest_engine, tenant_id: str):
    """
    Retorna a quantidade de registros de cada tabela.
    
    Returns:
        dict: Contagens de cada tabela
    """
    counts = {}
    
    with dest_engine.connect() as conn:
        # Aplicações de Vacinas
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM PET_VACINA 
            WHERE sCdTenant = '{tenant_id}'
        """))
        counts['aplicacoes_vacinas'] = result.fetchone()[0]
        
        # Pesos
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM PET_PESO 
            WHERE sCdTenant = '{tenant_id}'
        """))
        counts['pesos'] = result.fetchone()[0]
        
        # Vacinas
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM VACINA 
            WHERE sCdTenant = '{tenant_id}'
        """))
        counts['vacinas'] = result.fetchone()[0]
        
        # Pets
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM PET 
            WHERE sCdTenant = '{tenant_id}'
        """))
        counts['pets'] = result.fetchone()[0]
        
        # Clientes
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM PESSOA 
            WHERE sCdTenant = '{tenant_id}'
        """))
        counts['clientes'] = result.fetchone()[0]
        
        # Controle de migração
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM CONTROLE_MIGRACAO_LEGADO 
            WHERE sCdTenant = '{tenant_id}'
        """))
        counts['controle'] = result.fetchone()[0]
    
    return counts


def clear_aplicacoes_vacinas(dest_engine, tenant_id: str, dry_run: bool = False):
    """Exclui aplicações de vacinas."""
    print("\n1️⃣  Excluindo APLICAÇÕES DE VACINAS (PET_VACINA)...", end=" ", flush=True)
    
    if dry_run:
        print("[DRY-RUN]")
        return 0
    
    delete_sql = text(f"""
        DELETE FROM PET_VACINA 
        WHERE sCdTenant = '{tenant_id}'
    """)
    
    with dest_engine.begin() as conn:
        result = conn.execute(delete_sql)
        count = result.rowcount
    
    print(f"✓ {count} registros excluídos")
    return count


def clear_pesos(dest_engine, tenant_id: str, dry_run: bool = False):
    """Exclui pesos dos pets."""
    print("2️⃣  Excluindo PESOS (PET_PESO)...", end=" ", flush=True)
    
    if dry_run:
        print("[DRY-RUN]")
        return 0
    
    delete_sql = text(f"""
        DELETE FROM PET_PESO 
        WHERE sCdTenant = '{tenant_id}'
    """)
    
    with dest_engine.begin() as conn:
        result = conn.execute(delete_sql)
        count = result.rowcount
    
    print(f"✓ {count} registros excluídos")
    return count


def clear_vacinas(dest_engine, tenant_id: str, dry_run: bool = False):
    """Exclui vacinas."""
    print("3️⃣  Excluindo VACINAS (VACINA)...", end=" ", flush=True)
    
    if dry_run:
        print("[DRY-RUN]")
        return 0
    
    delete_sql = text(f"""
        DELETE FROM VACINA 
        WHERE sCdTenant = '{tenant_id}'
    """)
    
    with dest_engine.begin() as conn:
        result = conn.execute(delete_sql)
        count = result.rowcount
    
    print(f"✓ {count} registros excluídos")
    return count


def clear_pets(dest_engine, tenant_id: str, dry_run: bool = False):
    """Exclui pets."""
    print("4️⃣  Excluindo PETS (PET)...", end=" ", flush=True)
    
    if dry_run:
        print("[DRY-RUN]")
        return 0
    
    delete_sql = text(f"""
        DELETE FROM PET 
        WHERE sCdTenant = '{tenant_id}'
    """)
    
    with dest_engine.begin() as conn:
        result = conn.execute(delete_sql)
        count = result.rowcount
    
    print(f"✓ {count} registros excluídos")
    return count


def clear_clientes(dest_engine, tenant_id: str, dry_run: bool = False):
    """Exclui clientes."""
    print("5️⃣  Excluindo CLIENTES (PESSOA)...", end=" ", flush=True)
    
    if dry_run:
        print("[DRY-RUN]")
        return 0
    
    delete_sql = text(f"""
        DELETE FROM PESSOA 
        WHERE sCdTenant = '{tenant_id}'
    """)
    
    with dest_engine.begin() as conn:
        result = conn.execute(delete_sql)
        count = result.rowcount
    
    print(f"✓ {count} registros excluídos")
    return count


def clear_controle(dest_engine, tenant_id: str, dry_run: bool = False):
    """Exclui registros de controle de migração."""
    print("6️⃣  Excluindo CONTROLE DE MIGRAÇÃO (CONTROLE_MIGRACAO_LEGADO)...", end=" ", flush=True)
    
    if dry_run:
        print("[DRY-RUN]")
        return 0
    
    delete_sql = text(f"""
        DELETE FROM CONTROLE_MIGRACAO_LEGADO 
        WHERE sCdTenant = '{tenant_id}'
    """)
    
    with dest_engine.begin() as conn:
        result = conn.execute(delete_sql)
        count = result.rowcount
    
    print(f"✓ {count} registros excluídos")
    return count


def clear_all_data(dry_run: bool = False):
    """
    Exclui todos os dados migrados na ordem correta.
    
    Args:
        dry_run: Se True, apenas simula (não exclui)
    
    Returns:
        dict: Estatísticas da exclusão
    """
    print("\n" + "="*80)
    print("EXCLUSÃO DE DADOS MIGRADOS")
    print("="*80 + "\n")
    
    dest_engine = get_engine_from_env("DEST_DB_URL")
    tenant_id = get_tenant_id()
    
    print(f"🔑 Tenant ID: {tenant_id}")
    
    # Mostrar contagens antes
    print("\n📊 Contagem ANTES da exclusão:")
    counts_before = get_counts(dest_engine, tenant_id)
    print(f"  • Aplicações de Vacinas: {counts_before['aplicacoes_vacinas']:,}")
    print(f"  • Pesos: {counts_before['pesos']:,}")
    print(f"  • Vacinas: {counts_before['vacinas']:,}")
    print(f"  • Pets: {counts_before['pets']:,}")
    print(f"  • Clientes: {counts_before['clientes']:,}")
    print(f"  • Registros de Controle: {counts_before['controle']:,}")
    
    if dry_run:
        print("\n[DRY-RUN] Simulando exclusão...\n")
    else:
        print("\n⚠️  ATENÇÃO: Esta operação é IRREVERSÍVEL!\n")
        print("Excluindo dados na ordem correta (respeitando foreign keys):\n")
    
    # Executar exclusões na ordem correta
    stats = {}
    
    try:
        stats['aplicacoes_vacinas'] = clear_aplicacoes_vacinas(dest_engine, tenant_id, dry_run)
        stats['pesos'] = clear_pesos(dest_engine, tenant_id, dry_run)
        stats['vacinas'] = clear_vacinas(dest_engine, tenant_id, dry_run)
        stats['pets'] = clear_pets(dest_engine, tenant_id, dry_run)
        stats['clientes'] = clear_clientes(dest_engine, tenant_id, dry_run)
        stats['controle'] = clear_controle(dest_engine, tenant_id, dry_run)
        
    except Exception as e:
        print(f"\n✗ Erro durante exclusão: {e}")
        return None
    
    # Mostrar contagens depois
    if not dry_run:
        print("\n📊 Contagem APÓS a exclusão:")
        counts_after = get_counts(dest_engine, tenant_id)
        print(f"  • Aplicações de Vacinas: {counts_after['aplicacoes_vacinas']:,}")
        print(f"  • Pesos: {counts_after['pesos']:,}")
        print(f"  • Vacinas: {counts_after['vacinas']:,}")
        print(f"  • Pets: {counts_after['pets']:,}")
        print(f"  • Clientes: {counts_after['clientes']:,}")
        print(f"  • Registros de Controle: {counts_after['controle']:,}")
    
    print("\n" + "="*80)
    if dry_run:
        print("✓ Simulação concluída!")
    else:
        print("✓ Exclusão concluída!")
    
    total_deleted = sum(stats.values())
    print(f"  Total de registros excluídos: {total_deleted:,}")
    print("="*80 + "\n")
    
    return stats


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Exclusão de Dados Migrados")
    parser.add_argument("--dry-run", action="store_true", help="Simula exclusão sem deletar dados")
    parser.add_argument("--confirm", action="store_true", help="Confirma exclusão (obrigatório para executar)")
    
    args = parser.parse_args()
    
    if not args.dry_run and not args.confirm:
        print("\n⚠️  ATENÇÃO: Esta operação é IRREVERSÍVEL!")
        print("\nPara executar a exclusão, use: --confirm")
        print("Para simular, use: --dry-run")
        print("\nExemplo: python src/clear_migrated_data.py --dry-run")
        print("         python src/clear_migrated_data.py --confirm\n")
        sys.exit(1)
    
    clear_all_data(dry_run=args.dry_run)
