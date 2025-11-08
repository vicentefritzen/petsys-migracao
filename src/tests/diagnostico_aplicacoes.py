"""
Diagnóstico completo da migração de aplicações de vacinas
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from common.db_utils import get_tenant_id

load_dotenv()

legacy_engine = create_engine(os.getenv('LEGACY_DB_URL'))
dest_engine = create_engine(os.getenv('DEST_DB_URL'))
tenant_id = get_tenant_id()

print("="*80)
print("DIAGNÓSTICO: APLICAÇÕES DE VACINAS")
print("="*80 + "\n")

# 1. Total na origem
with legacy_engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM PET_ANIMAL_VACINA"))
    total_origem = result.scalar()
    print(f"📊 Total na ORIGEM (PET_ANIMAL_VACINA): {total_origem:,}")

# 2. Total no destino
with dest_engine.connect() as conn:
    result = conn.execute(text(f"""
        SELECT COUNT(*) FROM PET_VACINA 
        WHERE sCdTenant = '{tenant_id}'
    """))
    total_destino = result.scalar()
    print(f"📊 Total no DESTINO (PET_VACINA): {total_destino:,}")

# 3. Pets migrados
with dest_engine.connect() as conn:
    result = conn.execute(text(f"""
        SELECT COUNT(DISTINCT sValorChaveOrigem)
        FROM CONTROLE_MIGRACAO_LEGADO
        WHERE sCdTenant = '{tenant_id}'
          AND sTabelaOrigem = 'PET_ANIMAL'
    """))
    pets_migrados = result.scalar()
    print(f"📊 Pets migrados: {pets_migrados:,}")

# 4. Vacinas migradas
with dest_engine.connect() as conn:
    result = conn.execute(text(f"""
        SELECT COUNT(DISTINCT sValorChaveOrigem)
        FROM CONTROLE_MIGRACAO_LEGADO
        WHERE sCdTenant = '{tenant_id}'
          AND sTabelaOrigem = 'PET_VACINA'
    """))
    vacinas_migradas = result.scalar()
    print(f"📊 Vacinas (cadastro) migradas: {vacinas_migradas:,}")

# 5. Total de pets na origem
with legacy_engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM PET_ANIMAL"))
    total_pets_origem = result.scalar()
    print(f"📊 Total de pets na origem: {total_pets_origem:,}")

# 6. Total de vacinas na origem
with legacy_engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM PET_VACINA"))
    total_vacinas_origem = result.scalar()
    print(f"📊 Total de vacinas (cadastro) na origem: {total_vacinas_origem:,}")

# 7. Aplicações que PODEM ser migradas (pet e vacina existem na origem)
with legacy_engine.connect() as conn:
    result = conn.execute(text("""
        SELECT COUNT(*) 
        FROM PET_ANIMAL_VACINA av
        WHERE EXISTS (SELECT 1 FROM PET_ANIMAL pa WHERE pa.Codigo = av.Animal)
          AND EXISTS (SELECT 1 FROM PET_VACINA pv WHERE pv.Codigo = av.Vacina)
    """))
    aplicacoes_valiidas = result.scalar()
    print(f"📊 Aplicações válidas na origem (pet+vacina existem): {aplicacoes_valiidas:,}")

# 8. Pets únicos nas aplicações
with legacy_engine.connect() as conn:
    result = conn.execute(text("""
        SELECT COUNT(DISTINCT Animal) FROM PET_ANIMAL_VACINA
    """))
    pets_unicos = result.scalar()
    print(f"📊 Pets únicos nas aplicações: {pets_unicos:,}")

# 9. Vacinas únicas nas aplicações
with legacy_engine.connect() as conn:
    result = conn.execute(text("""
        SELECT COUNT(DISTINCT Vacina) FROM PET_ANIMAL_VACINA
    """))
    vacinas_unicas = result.scalar()
    print(f"📊 Vacinas únicas nas aplicações: {vacinas_unicas:,}")

print("\n" + "="*80)
print("ANÁLISE DETALHADA")
print("="*80 + "\n")

# 10. Verificar sample de pets das aplicações
print("Verificando se os primeiros 10 pets das aplicações foram migrados...\n")
with legacy_engine.connect() as conn:
    result = conn.execute(text("""
        SELECT DISTINCT TOP 10 av.Animal, pa.Nome
        FROM PET_ANIMAL_VACINA av
        LEFT JOIN PET_ANIMAL pa ON pa.Codigo = av.Animal
        ORDER BY av.Animal
    """))
    
    pets_sample = result.fetchall()
    migrados_count = 0
    nao_migrados_count = 0
    
    for codigo, nome in pets_sample:
        with dest_engine.connect() as dest_conn:
            check = dest_conn.execute(text(f"""
                SELECT sValorChaveDestino
                FROM CONTROLE_MIGRACAO_LEGADO
                WHERE sCdTenant = '{tenant_id}'
                  AND sTabelaOrigem = 'PET_ANIMAL'
                  AND sValorChaveOrigem = '{codigo}'
            """))
            migrado = check.fetchone()
            
            if migrado:
                status = "✓ MIGRADO"
                migrados_count += 1
            else:
                status = "✗ NÃO MIGRADO"
                nao_migrados_count += 1
            
            print(f"  Pet {codigo} ({nome or 'SEM NOME'}): {status}")

print(f"\nResultado: {migrados_count}/10 migrados, {nao_migrados_count}/10 não migrados")

# 11. Verificar sample de vacinas das aplicações
print("\n" + "-"*80)
print("Verificando se as primeiras 10 vacinas das aplicações foram migradas...\n")
with legacy_engine.connect() as conn:
    result = conn.execute(text("""
        SELECT DISTINCT TOP 10 av.Vacina, pv.Descricao
        FROM PET_ANIMAL_VACINA av
        LEFT JOIN PET_VACINA pv ON pv.Codigo = av.Vacina
        ORDER BY av.Vacina
    """))
    
    vacinas_sample = result.fetchall()
    migrados_count = 0
    nao_migrados_count = 0
    
    for codigo, descricao in vacinas_sample:
        with dest_engine.connect() as dest_conn:
            check = dest_conn.execute(text(f"""
                SELECT sValorChaveDestino
                FROM CONTROLE_MIGRACAO_LEGADO
                WHERE sCdTenant = '{tenant_id}'
                  AND sTabelaOrigem = 'PET_VACINA'
                  AND sValorChaveOrigem = '{codigo}'
            """))
            migrado = check.fetchone()
            
            if migrado:
                status = "✓ MIGRADO"
                migrados_count += 1
            else:
                status = "✗ NÃO MIGRADO"
                nao_migrados_count += 1
            
            print(f"  Vacina {codigo} ({descricao or 'SEM NOME'}): {status}")

print(f"\nResultado: {migrados_count}/10 migrados, {nao_migrados_count}/10 não migrados")

# 12. Calcular estimativa de migração possível
print("\n" + "="*80)
print("ESTIMATIVA DE MIGRAÇÃO")
print("="*80 + "\n")

pct_pets_migrados = (pets_migrados / total_pets_origem * 100) if total_pets_origem > 0 else 0
pct_vacinas_migradas = (vacinas_migradas / total_vacinas_origem * 100) if total_vacinas_origem > 0 else 0

print(f"Pets migrados: {pets_migrados:,} de {total_pets_origem:,} ({pct_pets_migrados:.1f}%)")
print(f"Vacinas migradas: {vacinas_migradas:,} de {total_vacinas_origem:,} ({pct_vacinas_migradas:.1f}%)")

# Estimativa de aplicações que podem ser migradas
estimativa_migravel = int(aplicacoes_valiidas * (pct_pets_migrados/100) * (pct_vacinas_migradas/100))
print(f"\nEstimativa de aplicações que PODEM ser migradas: ~{estimativa_migravel:,}")
print(f"Aplicações atualmente no destino: {total_destino:,}")

if total_destino < estimativa_migravel * 0.9:
    print(f"\n⚠️  ATENÇÃO: Faltam ~{estimativa_migravel - total_destino:,} aplicações para migrar!")

print("\n" + "="*80)
print("RECOMENDAÇÕES")
print("="*80 + "\n")

if pets_migrados == 0:
    print("❌ PROBLEMA CRÍTICO: Nenhum pet foi migrado!")
    print("   → EXECUTE: pipenv run python src/migrations/pets/migrate_pets.py")
elif pct_pets_migrados < 50:
    print(f"⚠️  ATENÇÃO: Apenas {pct_pets_migrados:.1f}% dos pets foram migrados")
    print("   → EXECUTE novamente: pipenv run python src/migrations/pets/migrate_pets.py")
elif pets_migrados < total_pets_origem:
    print(f"✓ Pets OK: {pct_pets_migrados:.1f}% migrados ({total_pets_origem - pets_migrados:,} faltando)")

if vacinas_migradas == 0:
    print("❌ PROBLEMA CRÍTICO: Nenhuma vacina foi migrada!")
    print("   → EXECUTE: pipenv run python src/migrations/vacinas/migrate_vacinas.py")
elif pct_vacinas_migradas < 50:
    print(f"⚠️  ATENÇÃO: Apenas {pct_vacinas_migradas:.1f}% das vacinas foram migradas")
    print("   → EXECUTE novamente: pipenv run python src/migrations/vacinas/migrate_vacinas.py")
elif vacinas_migradas < total_vacinas_origem:
    print(f"✓ Vacinas OK: {pct_vacinas_migradas:.1f}% migradas ({total_vacinas_origem - vacinas_migradas:,} faltando)")

print()

if total_destino == 0:
    print("🎯 PRÓXIMO PASSO:")
    print("   → EXECUTE MIGRAÇÃO FULL:")
    print("   pipenv run python src/migrations/aplicacoes_vacinas/migrate_aplicacoes_vacinas.py --batch-size 1000")
elif total_destino < estimativa_migravel * 0.5:
    print("🎯 PRÓXIMO PASSO:")
    print("   → MIGRAÇÃO INCOMPLETA. Execute novamente:")
    print("   pipenv run python src/migrations/aplicacoes_vacinas/migrate_aplicacoes_vacinas.py --batch-size 1000")
else:
    print("✅ MIGRAÇÃO COMPLETA!")
    print(f"   {total_destino:,} aplicações migradas de ~{estimativa_migravel:,} possíveis")

print()
