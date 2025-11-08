"""
Script para mostrar as credenciais decodificadas do .env
"""
import os
from pathlib import Path
from urllib.parse import unquote
from dotenv import load_dotenv

# Carrega variáveis do arquivo .env
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

def parse_and_show(label, url):
    print(f"\n{'='*60}")
    print(f"{label}")
    print(f"{'='*60}")
    
    # Parse manual
    url = url.replace("mssql+pymssql://", "")
    creds, rest = url.split("@", 1)
    user, password_encoded = creds.split(":", 1)
    password = unquote(password_encoded)
    
    host_port, database = rest.split("/")
    host, port = host_port.split(":")
    
    print(f"Servidor: {host}")
    print(f"Porta: {port}")
    print(f"Database: {database}")
    print(f"Usuário: {user}")
    print(f"Senha (encoded): {password_encoded}")
    print(f"Senha (decoded): {password}")
    print(f"Tamanho da senha: {len(password)} caracteres")

if __name__ == "__main__":
    print("\n" + "="*60)
    print("CREDENCIAIS DECODIFICADAS DO .ENV")
    print("="*60)
    
    legacy_url = os.getenv("LEGACY_DB_URL")
    dest_url = os.getenv("DEST_DB_URL")
    
    if legacy_url:
        parse_and_show("BANCO LEGADO", legacy_url)
    
    if dest_url:
        parse_and_show("BANCO DESTINO", dest_url)
    
    print("\n" + "="*60)
    print("Verifique se as senhas acima estão EXATAMENTE como esperado")
    print("="*60 + "\n")
