"""
Script para analisar estrutura das tabelas de origem (PET_ANIMAL, PET_RACA, etc)
"""
from sqlalchemy import text
from common.db_utils import get_engine_from_env

def analyze_legacy_tables():
    engine = get_engine_from_env("LEGACY_DB_URL")
    
    tables = ["PET_ANIMAL", "PET_RACA", "PET_COR", "PET_SEXO", "PET_ESPECIE"]
    
    print("="*80)
    print("ANÁLISE DAS TABELAS DE ORIGEM (LEGADO)")
    print("="*80 + "\n")
    
    for table in tables:
        print(f"\n{'='*80}")
        print(f"TABELA: {table}")
        print("="*80)
        
        # Obter estrutura da tabela
        schema_sql = text(f"""
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    IS_NULLABLE,
    COLUMN_DEFAULT
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '{table}'
ORDER BY ORDINAL_POSITION
""")
        
        # Contar registros
        count_sql = text(f"SELECT COUNT(*) as total FROM {table}")
        
        # Sample de dados
        sample_sql = text(f"SELECT TOP 5 * FROM {table}")
        
        with engine.connect() as conn:
            # Estrutura
            print("\nESTRUTURA:")
            result = conn.execute(schema_sql)
            columns = result.fetchall()
            for col in columns:
                nullable = "NULL" if col[3] == "YES" else "NOT NULL"
                size = f"({col[2]})" if col[2] else ""
                default = f" DEFAULT {col[4]}" if col[4] else ""
                print(f"  {col[0]:<30} {col[1]}{size:<15} {nullable}{default}")
            
            # Total
            result = conn.execute(count_sql)
            total = result.fetchone()[0]
            print(f"\nTOTAL DE REGISTROS: {total}")
            
            # Sample
            if total > 0:
                print("\nSAMPLE (5 primeiros registros):")
                result = conn.execute(sample_sql)
                rows = result.fetchall()
                for i, row in enumerate(rows, 1):
                    print(f"\n  Registro {i}:")
                    for j, col in enumerate(columns):
                        col_name = col[0]
                        value = row[j]
                        print(f"    {col_name}: {value}")

if __name__ == "__main__":
    analyze_legacy_tables()
