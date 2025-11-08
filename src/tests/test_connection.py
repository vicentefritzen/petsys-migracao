"""
Script para testar conexão com Azure SQL Database
"""
import os
from pathlib import Path
from urllib.parse import unquote
from dotenv import load_dotenv
import pymssql

# Carrega variáveis do arquivo .env
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

def parse_connection_string(url):
    """Parse manual da connection string com decode de URL encoding"""
    # Formato: mssql+pymssql://user:pass@host:port/database
    url = url.replace("mssql+pymssql://", "")
    
    # Separar credenciais do resto
    creds, rest = url.split("@", 1)  # Usar maxsplit=1 caso tenha @ na senha
    
    # Separar user:password
    if ":" in creds:
        user, password = creds.split(":", 1)  # maxsplit=1 caso tenha : na senha
    else:
        user = creds
        password = ""
    
    # Decodificar URL encoding
    user = unquote(user)
    password = unquote(password)
    
    # Separar host:porta do database
    host_port, database = rest.split("/")
    host, port = host_port.split(":")
    
    return {
        "user": user,
        "password": password,
        "server": host,
        "port": int(port),
        "database": database
    }

def test_connection(label, connection_url):
    """Testa conexão com o banco"""
    print(f"\n{'='*60}")
    print(f"Testando conexão: {label}")
    print(f"{'='*60}")
    
    try:
        params = parse_connection_string(connection_url)
        print(f"Servidor: {params['server']}")
        print(f"Porta: {params['port']}")
        print(f"Database: {params['database']}")
        print(f"Usuário: {params['user']}")
        print(f"Senha: {'*' * len(params['password'])} ({len(params['password'])} caracteres)")
        print(f"Conectando...")
        
        # Tentar conexão com timeout e TDS version para Azure
        conn = pymssql.connect(
            server=params['server'],
            user=params['user'],
            password=params['password'],
            database=params['database'],
            port=params['port'],
            timeout=30,
            login_timeout=30,
            tds_version='7.4'  # Versão TDS para Azure SQL
        )
        
        print("✓ Conexão estabelecida com sucesso!")
        
        # Testar query simples
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        row = cursor.fetchone()
        print(f"✓ Versão do SQL Server: {row[0][:50]}...")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"✗ Erro na conexão:")
        print(f"  {type(e).__name__}: {e}")
        return False

if __name__ == "__main__":
    print("\n" + "="*60)
    print("TESTE DE CONEXÃO COM AZURE SQL DATABASE")
    print("="*60)
    
    legacy_url = os.getenv("LEGACY_DB_URL")
    dest_url = os.getenv("DEST_DB_URL")
    
    if not legacy_url or not dest_url:
        print("✗ Variáveis LEGACY_DB_URL ou DEST_DB_URL não encontradas no .env")
        exit(1)
    
    # Testar conexão com banco legado
    legacy_ok = test_connection("BANCO LEGADO", legacy_url)
    
    # Testar conexão com banco destino
    dest_ok = test_connection("BANCO DESTINO", dest_url)
    
    print("\n" + "="*60)
    print("RESUMO")
    print("="*60)
    print(f"Banco Legado: {'✓ OK' if legacy_ok else '✗ FALHOU'}")
    print(f"Banco Destino: {'✓ OK' if dest_ok else '✗ FALHOU'}")
    
    if legacy_ok and dest_ok:
        print("\n✓ Ambas as conexões funcionaram! Pode prosseguir com a migração.")
    else:
        print("\n✗ Verifique as credenciais e firewall do Azure SQL Database.")
        print("  Dica: Adicione seu IP público no firewall do Azure SQL.")
    
    print("="*60 + "\n")
