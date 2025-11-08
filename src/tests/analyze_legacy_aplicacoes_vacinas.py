"""
Script de análise da tabela PET_ANIMAL_VACINA no banco legado
"""
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

def analyze_pet_animal_vacina():
    """Analisa a estrutura e dados da tabela PET_ANIMAL_VACINA"""
    
    legacy_engine = create_engine(os.getenv('LEGACY_DB_URL'))
    
    print("="*80)
    print("ANÁLISE DA TABELA PET_ANIMAL_VACINA (ORIGEM)")
    print("="*80 + "\n")
    
    with legacy_engine.connect() as conn:
        # Total de registros
        result = conn.execute(text("SELECT COUNT(*) as total FROM PET_ANIMAL_VACINA"))
        total = result.fetchone()[0]
        print(f"📊 Total de registros: {total}\n")
        
        # Distribuição: Aplicadas vs Previstas
        print("-"*80)
        print("DISTRIBUIÇÃO: APLICADAS VS PREVISTAS")
        print("-"*80)
        result = conn.execute(text("""
            SELECT 
                CASE 
                    WHEN DataAplicacao IS NOT NULL THEN 'Aplicada'
                    ELSE 'Prevista'
                END as Status,
                COUNT(*) as Quantidade,
                CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM PET_ANIMAL_VACINA) AS DECIMAL(5,2)) as Percentual
            FROM PET_ANIMAL_VACINA
            GROUP BY CASE WHEN DataAplicacao IS NOT NULL THEN 'Aplicada' ELSE 'Prevista' END
        """))
        
        for row in result:
            print(f"  {row[0]:<15} {row[1]:>8} registros ({row[2]:>6}%)")
        
        # Top 10 vacinas mais aplicadas
        print("\n" + "-"*80)
        print("TOP 10 VACINAS MAIS APLICADAS/PREVISTAS")
        print("-"*80)
        result = conn.execute(text("""
            SELECT TOP 10
                v.Descricao,
                COUNT(*) as Total,
                SUM(CASE WHEN av.DataAplicacao IS NOT NULL THEN 1 ELSE 0 END) as Aplicadas,
                SUM(CASE WHEN av.DataAplicacao IS NULL THEN 1 ELSE 0 END) as Previstas
            FROM PET_ANIMAL_VACINA av
            JOIN PET_VACINA v ON av.Vacina = v.Codigo
            GROUP BY v.Descricao
            ORDER BY COUNT(*) DESC
        """))
        
        print(f"{'Vacina':<30} {'Total':>8} {'Aplicadas':>10} {'Previstas':>10}")
        print("-"*80)
        for row in result:
            print(f"{row[0]:<30} {row[1]:>8} {row[2]:>10} {row[3]:>10}")
        
        # Distribuição por PreAutorizado
        print("\n" + "-"*80)
        print("PRÉ-AUTORIZADO")
        print("-"*80)
        result = conn.execute(text("""
            SELECT 
                CASE 
                    WHEN PreAutorizado = 1 THEN 'Sim'
                    ELSE 'Não'
                END as PreAutorizado,
                COUNT(*) as Quantidade
            FROM PET_ANIMAL_VACINA
            GROUP BY PreAutorizado
        """))
        
        for row in result:
            print(f"  {row[0]:<15} {row[1]:>8} registros")
        
        # Campos nulos
        print("\n" + "-"*80)
        print("ANÁLISE DE CAMPOS NULOS")
        print("-"*80)
        result = conn.execute(text("""
            SELECT 
                SUM(CASE WHEN Partida IS NULL OR Partida = '' THEN 1 ELSE 0 END) as Partida_Nulo,
                SUM(CASE WHEN Laboratorio IS NULL OR Laboratorio = '' THEN 1 ELSE 0 END) as Laboratorio_Nulo,
                SUM(CASE WHEN LocalAplicacao IS NULL OR LocalAplicacao = '' THEN 1 ELSE 0 END) as LocalAplicacao_Nulo,
                SUM(CASE WHEN DataAplicacao IS NULL THEN 1 ELSE 0 END) as DataAplicacao_Nulo
            FROM PET_ANIMAL_VACINA
        """))
        
        row = result.fetchone()
        print(f"  Partida vazio/nulo:        {row[0]:>8} ({row[0]*100.0/total:.1f}%)")
        print(f"  Laboratório vazio/nulo:    {row[1]:>8} ({row[1]*100.0/total:.1f}%)")
        print(f"  Local vazio/nulo:          {row[2]:>8} ({row[2]*100.0/total:.1f}%)")
        print(f"  Data aplicação nulo:       {row[3]:>8} ({row[3]*100.0/total:.1f}%)")
        
        # Amostra de registros
        print("\n" + "-"*80)
        print("AMOSTRA DE REGISTROS (5 aplicados + 5 previstos)")
        print("-"*80)
        
        # Aplicados
        print("\n📌 APLICADOS:")
        result = conn.execute(text("""
            SELECT TOP 5
                av.Codigo,
                av.Animal,
                v.Descricao as Vacina,
                av.DataPrevista,
                av.DataAplicacao,
                av.Laboratorio,
                av.LocalAplicacao
            FROM PET_ANIMAL_VACINA av
            JOIN PET_VACINA v ON av.Vacina = v.Codigo
            WHERE av.DataAplicacao IS NOT NULL
            ORDER BY av.Codigo
        """))
        
        for row in result:
            print(f"  [{row[0]}] Animal={row[1]} | {row[2]} | Prev:{row[3].strftime('%d/%m/%Y')} | Aplic:{row[4].strftime('%d/%m/%Y')} | Lab:{row[5] or 'N/A'}")
        
        # Previstos
        print("\n📅 PREVISTOS:")
        result = conn.execute(text("""
            SELECT TOP 5
                av.Codigo,
                av.Animal,
                v.Descricao as Vacina,
                av.DataPrevista
            FROM PET_ANIMAL_VACINA av
            JOIN PET_VACINA v ON av.Vacina = v.Codigo
            WHERE av.DataAplicacao IS NULL
            ORDER BY av.Codigo
        """))
        
        for row in result:
            print(f"  [{row[0]}] Animal={row[1]} | {row[2]} | Prevista para: {row[3].strftime('%d/%m/%Y')}")
    
    print("\n" + "="*80 + "\n")


if __name__ == "__main__":
    analyze_pet_animal_vacina()
