"""
Verificar vacinas migradas no banco destino
"""
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

dest_engine = create_engine(os.getenv('DEST_DB_URL'))

print('='*100)
print('VACINAS MIGRADAS (DESTINO)')
print('='*100 + '\n')

with dest_engine.connect() as conn:
    # Buscar vacinas do tenant padrão
    tenant = os.getenv('DEFAULT_TENANT')
    result = conn.execute(text('''
        SELECT 
            sNmVacina,
            nCdEspecie,
            nNrFrequencia,
            nCdPeriodicidade,
            nVlPrecoCompra,
            nVlPrecoVenda,
            nPcDescontoMensalista,
            bFlInclusoPlanoMensalista,
            bFlAtivo
        FROM VACINA
        WHERE sCdTenant = :tenant
        ORDER BY sNmVacina
    '''), {'tenant': tenant})
    
    rows = result.fetchall()
    
    print(f"{'Nome':<30} {'Esp':>3} {'Freq':>5} {'Per':>4} {'Compra':>10} {'Venda':>10} {'Desc%':>6} {'Plano':>5} {'Ativo':>5}")
    print('-'*100)
    
    for row in rows:
        plano = 'Sim' if row[7] else 'Não'
        ativo = 'Sim' if row[8] else 'Não'
        print(f"{row[0]:<30} {row[1]:>3} {row[2]:>5} {row[3]:>4} {row[4]:>10.2f} {row[5]:>10.2f} {row[6]:>6.2f} {plano:>5} {ativo:>5}")
    
    print('-'*100)
    print(f'Total de vacinas migradas: {len(rows)}\n')
