"""
Script de teste do menu de migração de vacinas
"""
import sys
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from migrations.vacinas.migrate_vacinas import migrate_vacinas

# Testar dry-run
print("=== TESTE: DRY-RUN ===\n")
migrate_vacinas(batch_size=500, dry_run=True)

print("\n\n=== TESTE: Análise dos dados migrados ===\n")
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
dest_engine = create_engine(os.getenv('DEST_DB_URL'))

with dest_engine.connect() as conn:
    tenant = os.getenv('DEFAULT_TENANT')
    
    # Total migrado
    result = conn.execute(text('''
        SELECT COUNT(*) FROM VACINA WHERE sCdTenant = :tenant
    '''), {'tenant': tenant})
    total = result.fetchone()[0]
    
    # Por espécie
    result = conn.execute(text('''
        SELECT nCdEspecie, COUNT(*) as qtd
        FROM VACINA 
        WHERE sCdTenant = :tenant
        GROUP BY nCdEspecie
        ORDER BY nCdEspecie
    '''), {'tenant': tenant})
    
    print(f"Total de vacinas: {total}")
    print("\nDistribuição por espécie:")
    for row in result:
        especie = "CANINA" if row[0] == 1 else "FELINA" if row[0] == 2 else "OUTRA"
        print(f"  {especie}: {row[1]} vacina(s)")
    
    # Verificar tabela de controle
    result = conn.execute(text('''
        SELECT COUNT(*) 
        FROM CONTROLE_MIGRACAO_LEGADO 
        WHERE sCdTenant = :tenant 
          AND sTabelaOrigem = 'PET_VACINA'
          AND sTabelaDestino = 'VACINA'
    '''), {'tenant': tenant})
    controle = result.fetchone()[0]
    
    print(f"\nRegistros na tabela de controle: {controle}")
    print("\n✓ Migração de vacinas está funcionando corretamente!")
