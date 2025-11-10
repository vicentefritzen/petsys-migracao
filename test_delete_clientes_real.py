"""
Teste direto de exclusão de clientes
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from common.db_utils import get_engine_from_env, get_tenant_id

# Importar a função diretamente
sys.path.insert(0, str(Path(__file__).parent / 'src'))
import importlib.util
spec = importlib.util.spec_from_file_location("clear_migrated_data", "src/clear_migrated_data.py")
clear_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(clear_module)

if __name__ == "__main__":
    print("Iniciando teste de exclusão de clientes...")
    print("="*60)
    
    dest_engine = get_engine_from_env("DEST_DB_URL")
    tenant_id = get_tenant_id()
    
    print(f"Tenant: {tenant_id}")
    print(f"Modo: EXECUÇÃO REAL (não dry-run)")
    print("="*60)
    
    input("\nPressione ENTER para continuar ou CTRL+C para cancelar...")
    
    try:
        total = clear_module.clear_clientes(dest_engine, tenant_id, dry_run=False)
        print(f"\n\n{'='*60}")
        print(f"✓ Exclusão concluída!")
        print(f"  Total de registros deletados: {total:,}")
        print(f"{'='*60}")
    except Exception as e:
        print(f"\n\n{'='*60}")
        print(f"✗ ERRO: {e}")
        print(f"{'='*60}")
        import traceback
        traceback.print_exc()
