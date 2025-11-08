"""
Script para analisar estrutura da tabela destino PET
"""
from sqlalchemy import text
from common.db_utils import get_engine_from_env

def analyze_dest_pet():
    engine = get_engine_from_env("DEST_DB_URL")
    
    print("="*80)
    print("ANÁLISE DA TABELA DESTINO: PET")
    print("="*80 + "\n")
    
    # Estrutura da tabela PET
    schema_sql = text("""
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    IS_NULLABLE,
    COLUMN_DEFAULT
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'PET'
ORDER BY ORDINAL_POSITION
""")
    
    # Contar registros
    count_sql = text("SELECT COUNT(*) as total FROM PET")
    
    # Sample de dados
    sample_sql = text("SELECT TOP 5 * FROM PET")
    
    with engine.connect() as conn:
        # Estrutura
        print("ESTRUTURA DA TABELA PET:")
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
        
        # Sample se houver dados
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
    
    print("\n" + "="*80)
    print("ANÁLISE DAS TABELAS RELACIONADAS (RAÇA, COR, SEXO, ESPÉCIE)")
    print("="*80 + "\n")
    
    # Verificar tabelas relacionadas
    related_tables = ["RACA", "COR_PELAGEM", "ESPECIE"]
    
    for table in related_tables:
        print(f"\n{'='*80}")
        print(f"TABELA: {table}")
        print("="*80)
        
        # Estrutura
        schema_sql = text(f"""
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '{table}'
ORDER BY ORDINAL_POSITION
""")
        
        # Sample
        sample_sql = text(f"SELECT TOP 10 * FROM {table}")
        
        with engine.connect() as conn:
            try:
                # Estrutura
                print("\nESTRUTURA:")
                result = conn.execute(schema_sql)
                columns = result.fetchall()
                for col in columns:
                    nullable = "NULL" if col[3] == "YES" else "NOT NULL"
                    size = f"({col[2]})" if col[2] else ""
                    print(f"  {col[0]:<30} {col[1]}{size:<15} {nullable}")
                
                # Sample
                print("\nSAMPLE (10 primeiros registros):")
                result = conn.execute(sample_sql)
                rows = result.fetchall()
                for i, row in enumerate(rows, 1):
                    print(f"  {i}. {dict(row._mapping)}")
            except Exception as e:
                print(f"  ⚠ Erro ao consultar {table}: {e}")

if __name__ == "__main__":
    analyze_dest_pet()
