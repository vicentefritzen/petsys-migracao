import os
import time
import argparse
import requests
from pathlib import Path
from sqlalchemy import text
from dotenv import load_dotenv
from db import get_engine_from_env

try:
    from rapidfuzz import fuzz, process
    FUZZY_LIB = "rapidfuzz"
except ImportError:
    try:
        from fuzzywuzzy import fuzz, process
        FUZZY_LIB = "fuzzywuzzy"
    except ImportError:
        print("⚠ AVISO: Instale rapidfuzz ou fuzzywuzzy para matching fuzzy de cidades")
        print("Execute: pipenv install rapidfuzz")
        FUZZY_LIB = None

# Carrega variáveis do arquivo .env
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

DEFAULT_TENANT = os.getenv("DEFAULT_TENANT", "dfedd5f4-f30c-45ea-bc1e-695081d8415c")
VIACEP_DELAY_SECONDS = int(os.getenv("VIACEP_DELAY_SECONDS", "5"))
VIACEP_BATCH_SIZE = int(os.getenv("VIACEP_BATCH_SIZE", "100"))
FUZZY_MIN_SCORE = int(os.getenv("FUZZY_MIN_SCORE", "85"))


def clean_cep(cep: str) -> str:
    """Remove caracteres não numéricos do CEP."""
    if not cep:
        return ""
    return "".join(filter(str.isdigit, str(cep)))


def consulta_viacep(cep: str) -> dict:
    """Consulta a API ViaCEP e retorna os dados do endereço.
    
    Retorna dict com localidade, uf, ibge ou None se houver erro.
    """
    cep_limpo = clean_cep(cep)
    if len(cep_limpo) != 8:
        return None
    
    url = f"http://viacep.com.br/ws/{cep_limpo}/json/"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if "erro" not in data:
                return data
    except Exception as e:
        print(f"Erro ao consultar CEP {cep_limpo}: {e}")
    return None


def buscar_cidade_por_nome_uf(dest_engine, nome_cidade: str, uf: str, tenant_id: str):
    """Busca cidade pelo nome (com fuzzy matching) e UF.
    
    Retorna o sCdCidade que melhor corresponde ao nome + UF.
    Em caso de empate, prefere cidades de SC.
    """
    if not nome_cidade or not uf:
        return None
    
    # Buscar todas as cidades da UF (ou todas se não encontrar na UF)
    select_sql = text("""
SELECT sCdCidade, sNmCidade, sCdUf
FROM CIDADE
WHERE sCdUf = :uf
""")
    
    select_all_sql = text("""
SELECT sCdCidade, sNmCidade, sCdUf
FROM CIDADE
""")
    
    cidades = []
    with dest_engine.connect() as conn:
        try:
            result = conn.execute(select_sql, {"uf": uf.upper()})
            cidades = result.fetchall()
            
            # Se não encontrou nenhuma cidade na UF, buscar em todas
            if not cidades:
                result = conn.execute(select_all_sql)
                cidades = result.fetchall()
        except Exception as e:
            print(f"  Erro ao buscar cidades: {e}")
            return None
    
    if not cidades:
        return None
    
    # Se não temos biblioteca de fuzzy matching, tentar match exato
    if FUZZY_LIB is None:
        for cidade in cidades:
            if cidade[1].upper() == nome_cidade.upper():
                return str(cidade[0])
        return None
    
    # Criar lista de tuplas (sNmCidade, (sCdCidade, sCdUf)) para o fuzzy matching
    choices = {cidade[1]: (str(cidade[0]), cidade[2]) for cidade in cidades}
    
    # Fazer fuzzy matching
    result = process.extractOne(
        nome_cidade, 
        choices.keys(), 
        scorer=fuzz.ratio,
        score_cutoff=FUZZY_MIN_SCORE
    )
    
    if result:
        cidade_match, score, _ = result if len(result) == 3 else (result[0], result[1], None)
        scd_cidade, uf_match = choices[cidade_match]
        
        print(f"  Match fuzzy: '{nome_cidade}' -> '{cidade_match}' ({uf_match}) [score: {score}%]")
        
        # Se o score for < 95 e a UF não bater, tentar encontrar em SC
        if score < 95 and uf_match != uf.upper() and uf.upper() != "SC":
            # Buscar especificamente em SC
            cidades_sc = [c for c in cidades if c[2] == "SC"]
            if cidades_sc:
                choices_sc = {cidade[1]: (str(cidade[0]), cidade[2]) for cidade in cidades_sc}
                result_sc = process.extractOne(
                    nome_cidade,
                    choices_sc.keys(),
                    scorer=fuzz.ratio,
                    score_cutoff=FUZZY_MIN_SCORE - 10  # Aceitar score menor para SC
                )
                if result_sc:
                    cidade_sc, score_sc, _ = result_sc if len(result_sc) == 3 else (result_sc[0], result_sc[1], None)
                    # Se o score de SC for próximo (diferença < 15), preferir SC
                    if score_sc >= score - 15:
                        scd_cidade_sc, _ = choices_sc[cidade_sc]
                        print(f"  ⭐ Preferência SC: '{cidade_sc}' [score: {score_sc}%]")
                        return scd_cidade_sc
        
        return scd_cidade
    
    return None


def buscar_cidade_por_ibge(dest_engine, codigo_ibge: str, tenant_id: str):
    """DEPRECATED: Mantido para compatibilidade, mas não é mais usado.
    
    A tabela CIDADE não possui campo sNrIBGE.
    """
    return None


def atualizar_endereco_pessoa(dest_engine, scd_pessoa: str, dados_endereco: dict, dry_run: bool = False):
    """Atualiza o endereço completo de uma pessoa no banco destino.
    
    dados_endereco deve conter:
    - scd_cidade: sCdCidade (obrigatório)
    - logradouro: nome da rua (opcional)
    - bairro: nome do bairro (opcional)
    - complemento: complemento do endereço (opcional)
    """
    # Montar UPDATE dinamicamente apenas com campos não-vazios
    campos_update = ["sCdCidade = :cidade"]
    params = {"cidade": dados_endereco["scd_cidade"], "pessoa": scd_pessoa}
    
    if dados_endereco.get("logradouro"):
        campos_update.append("sDsEndereco = :logradouro")
        params["logradouro"] = dados_endereco["logradouro"]
    
    if dados_endereco.get("bairro"):
        campos_update.append("sNmBairro = :bairro")
        params["bairro"] = dados_endereco["bairro"]
    
    if dados_endereco.get("complemento"):
        campos_update.append("sDsComplemento = :complemento")
        params["complemento"] = dados_endereco["complemento"]
    
    update_sql = text(f"""
UPDATE PESSOA 
SET {', '.join(campos_update)}
WHERE sCdPessoa = :pessoa
""")
    
    if dry_run:
        campos_str = ", ".join([f"{k}={v}" for k, v in params.items() if k != "pessoa"])
        print(f"[dry-run] Atualizar PESSOA {scd_pessoa} -> {campos_str}")
        return
    
    with dest_engine.begin() as conn:
        conn.execute(update_sql, params)


def update_cities(args):
    """Atualiza as cidades das pessoas consultando a API ViaCEP."""
    dest_engine = get_engine_from_env("DEST_DB_URL")
    tenant_id = args.tenant or DEFAULT_TENANT
    
    print(f"\n{'='*60}")
    print(f"CONFIGURAÇÃO:")
    print(f"  • Batch size: {VIACEP_BATCH_SIZE} requisições")
    print(f"  • Delay entre lotes: {VIACEP_DELAY_SECONDS} segundos")
    print(f"  • Score mínimo fuzzy: {FUZZY_MIN_SCORE}%")
    print(f"  • Dry-run: {'SIM' if args.dry_run else 'NÃO'}")
    print(f"{'='*60}\n")
    
    # Buscar todas as pessoas com CEP válido
    select_sql = text("""
SELECT sCdPessoa, nNrCep, sCdCidade
FROM PESSOA
WHERE sCdTenant = :tenant
  AND nNrCep IS NOT NULL
  AND nNrCep != ''
ORDER BY tDtCadastro
""")
    
    with dest_engine.connect() as conn:
        result = conn.execute(select_sql, {"tenant": tenant_id})
        pessoas = result.fetchall()
    
    total = len(pessoas)
    print(f"Total de pessoas com CEP para processar: {total}")
    
    processados = 0
    atualizados = 0
    erros = 0
    
    for idx, row in enumerate(pessoas, start=1):
        scd_pessoa = str(row[0])
        cep = str(row[1])
        cidade_atual = str(row[2]) if row[2] else None
        
        print(f"[{idx}/{total}] Processando pessoa {scd_pessoa}, CEP: {cep}")
        
        # Consultar ViaCEP
        dados_cep = consulta_viacep(cep)
        
        if dados_cep:
            localidade = dados_cep.get("localidade")
            uf = dados_cep.get("uf")
            logradouro = dados_cep.get("logradouro", "")
            bairro = dados_cep.get("bairro", "")
            complemento = dados_cep.get("complemento", "")
            
            print(f"  ViaCEP: {localidade}/{uf}")
            if logradouro:
                print(f"    Logradouro: {logradouro}")
            if bairro:
                print(f"    Bairro: {bairro}")
            
            # Buscar cidade no banco pelo nome + UF (com fuzzy matching)
            scd_cidade = buscar_cidade_por_nome_uf(dest_engine, localidade, uf, tenant_id)
            
            if scd_cidade:
                # Preparar dados do endereço completo
                dados_endereco = {
                    "scd_cidade": scd_cidade,
                    "logradouro": logradouro,
                    "bairro": bairro,
                    "complemento": complemento
                }
                
                # Atualizar endereço completo (não só cidade)
                atualizar_endereco_pessoa(dest_engine, scd_pessoa, dados_endereco, dry_run=args.dry_run)
                print(f"  ✓ Endereço atualizado completo")
                atualizados += 1
            else:
                print(f"  ⚠ Cidade '{localidade}/{uf}' não encontrada no banco destino (score < {FUZZY_MIN_SCORE}%)")
                erros += 1
        else:
            print(f"  ✗ CEP inválido ou não encontrado no ViaCEP")
            erros += 1
        
        processados += 1
        
        # Delay a cada VIACEP_BATCH_SIZE registros para respeitar rate limit da API
        if idx % VIACEP_BATCH_SIZE == 0 and idx < total:
            print(f"\n💤 Processados {idx}/{total} - Aguardando {VIACEP_DELAY_SECONDS} segundos (rate limit ViaCEP)...\n")
            time.sleep(VIACEP_DELAY_SECONDS)
    
    print("\n" + "="*60)
    print(f"Processamento concluído!")
    print(f"Total processado: {processados}")
    print(f"Atualizados: {atualizados}")
    print(f"Erros/Não encontrados: {erros}")
    print("="*60)


def main():
    parser = argparse.ArgumentParser(
        description="Atualiza cidades das pessoas consultando API ViaCEP"
    )
    parser.add_argument("--tenant", help="sCdTenant a usar (UUID)")
    parser.add_argument(
        "--dry-run", 
        action="store_true", 
        help="Não atualiza o banco, apenas mostra operações"
    )
    args = parser.parse_args()
    update_cities(args)


if __name__ == "__main__":
    main()
