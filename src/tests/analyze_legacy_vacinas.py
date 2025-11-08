"""
Script de teste para análise da tabela PET_VACINA no banco legado
"""
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

def analyze_pet_vacina():
    """Analisa a estrutura e dados da tabela PET_VACINA"""
    
    legacy_engine = create_engine(os.getenv('LEGACY_DB_URL'))
    
    print("="*70)
    print("ANÁLISE DA TABELA PET_VACINA (ORIGEM)")
    print("="*70 + "\n")
    
    with legacy_engine.connect() as conn:
        # Total de registros
        result = conn.execute(text("SELECT COUNT(*) as total FROM PET_VACINA"))
        total = result.fetchone()[0]
        print(f"📊 Total de registros: {total}\n")
        
        # Todos os registros
        print("-"*70)
        print("TODOS OS REGISTROS:")
        print("-"*70)
        result = conn.execute(text("""
            SELECT 
                Codigo,
                Descricao,
                Frequencia,
                Periodo,
                PrecoCompra,
                PrecoVenda
            FROM PET_VACINA
            ORDER BY Codigo
        """))
        
        print(f"{'Cód':>5} {'Descrição':<30} {'Freq':>5} {'Per':>4} {'Compra':>10} {'Venda':>10}")
        print("-"*70)
        
        for row in result:
            print(f"{row[0]:>5} {row[1]:<30} {row[2]:>5} {row[3]:>4} {row[4]:>10.2f} {row[5]:>10.2f}")
        
        # Estatísticas
        print("\n" + "-"*70)
        print("ESTATÍSTICAS:")
        print("-"*70)
        
        # Frequências distintas
        result = conn.execute(text("""
            SELECT DISTINCT Frequencia, COUNT(*) as qtd
            FROM PET_VACINA
            GROUP BY Frequencia
            ORDER BY Frequencia
        """))
        print("\n📈 Frequências distintas:")
        for row in result:
            print(f"   Frequência {row[0]}: {row[1]} vacina(s)")
        
        # Períodos distintos
        result = conn.execute(text("""
            SELECT DISTINCT Periodo, COUNT(*) as qtd
            FROM PET_VACINA
            GROUP BY Periodo
            ORDER BY Periodo
        """))
        print("\n📅 Períodos distintos:")
        for row in result:
            print(f"   Período {row[0]}: {row[1]} vacina(s)")
        
        # Preços
        result = conn.execute(text("""
            SELECT 
                MIN(PrecoCompra) as min_compra,
                MAX(PrecoCompra) as max_compra,
                AVG(PrecoCompra) as avg_compra,
                MIN(PrecoVenda) as min_venda,
                MAX(PrecoVenda) as max_venda,
                AVG(PrecoVenda) as avg_venda
            FROM PET_VACINA
        """))
        row = result.fetchone()
        print("\n💰 Preços:")
        print(f"   Compra: Min={row[0]:.2f}, Max={row[1]:.2f}, Média={row[2]:.2f}")
        print(f"   Venda:  Min={row[3]:.2f}, Max={row[4]:.2f}, Média={row[5]:.2f}")
    
    print("\n" + "="*70 + "\n")


if __name__ == "__main__":
    analyze_pet_vacina()
