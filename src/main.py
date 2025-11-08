#!/usr/bin/env python3
"""
Sistema de Migração PetSys - Legado para Web

Menu interativo para executar migrações de diferentes entidades.
"""
import sys
from pathlib import Path

# Adicionar src ao path para imports funcionarem
sys.path.insert(0, str(Path(__file__).parent))

from migrations.clientes.migrate_clientes import migrate_clientes
from migrations.pets.migrate_pets import migrate_pets
from migrations.vacinas.migrate_vacinas import migrate_vacinas


def print_header():
    """Imprime o cabeçalho do sistema."""
    print("\n" + "="*60)
    print("  SISTEMA DE MIGRAÇÃO PETSYS")
    print("  Legado -> Web")
    print("="*60 + "\n")


def print_menu():
    """Imprime o menu de opções."""
    print("Escolha a migração que deseja executar:")
    print()
    print("  1. Clientes (PET_CLIENTE -> PESSOA)")
    print("  2. Pets (PET_ANIMAL -> PET)")
    print("  3. Vacinas (PET_VACINA -> VACINA)")
    print("  4. Atualizar Cidades via ViaCEP [EM BREVE]")
    print()
    print("  0. Sair")
    print()


def confirm_action(message="Deseja continuar?"):
    """Solicita confirmação do usuário."""
    while True:
        response = input(f"{message} (s/n): ").strip().lower()
        if response in ['s', 'sim', 'y', 'yes']:
            return True
        elif response in ['n', 'nao', 'não', 'no']:
            return False
        print("Por favor, responda 's' para sim ou 'n' para não.")


def get_batch_size():
    """Solicita tamanho do batch ao usuário."""
    while True:
        try:
            size = input("Tamanho do batch (padrão 500): ").strip()
            if not size:
                return 500
            size = int(size)
            if size > 0:
                return size
            print("Por favor, informe um número maior que zero.")
        except ValueError:
            print("Por favor, informe um número válido.")


def run_migration_clientes():
    """Executa a migração de clientes."""
    print("\n" + "-"*60)
    print("MIGRAÇÃO DE CLIENTES")
    print("-"*60 + "\n")
    
    if not confirm_action("Deseja continuar?"):
        print("\nMigração cancelada.\n")
        return
    
    # Executar migração
    total = migrate_clientes(batch_size=500)
    
    print(f"\n✓ Migração concluída! {total} registros processados.\n")


def run_migration_pets():
    """Executa a migração de pets."""
    print("\n" + "-"*60)
    print("MIGRAÇÃO DE PETS")
    print("-"*60 + "\n")
    
    print("⚠ IMPORTANTE: Execute a migração de CLIENTES antes!")
    print("  Pets sem proprietário migrado serão pulados.\n")
    
    if not confirm_action("Deseja continuar?"):
        print("\nMigração cancelada.\n")
        return
    
    # Executar migração
    total = migrate_pets(batch_size=500)
    
    print(f"\n✓ Migração concluída! {total} registros processados.\n")


def run_migration_vacinas():
    """Executa a migração de vacinas."""
    print("\n" + "-"*60)
    print("MIGRAÇÃO DE VACINAS")
    print("-"*60 + "\n")
    
    print("Esta migração irá:")
    print("  • Ler registros de PET_VACINA (banco legado)")
    print("  • Mapear para tabela VACINA (banco destino)")
    print("  • Definir espécie padrão como CANINA (1)")
    print("  • Configurar valores padrão para desconto e plano\n")
    
    # Perguntar sobre dry-run
    if confirm_action("Executar em modo DRY-RUN primeiro?"):
        print("\n→ Executando DRY-RUN...\n")
        migrate_vacinas(batch_size=500, dry_run=True)
        
        if not confirm_action("\nDeseja executar a migração real agora?"):
            print("\nMigração cancelada.\n")
            return
    
    # Solicitar batch size
    batch_size = get_batch_size()
    
    # Executar migração real
    print("\n→ Executando migração real...\n")
    stats = migrate_vacinas(batch_size=batch_size, dry_run=False)
    
    print(f"\n✓ Migração concluída!")
    print(f"  Total: {stats['total']}")
    print(f"  Inseridos: {stats['inseridos']}")
    print(f"  Atualizados: {stats['atualizados']}\n")


def run_update_cities():
    """Executa atualização de cidades via ViaCEP."""
    print("\n" + "-"*60)
    print("ATUALIZAÇÃO DE CIDADES VIA VIACEP")
    print("-"*60 + "\n")
    
    print("⚠ FUNCIONALIDADE EM BREVE")
    print("Use: python src/update_cities.py\n")


def main():
    """Função principal do menu."""
    print_header()
    
    while True:
        print_menu()
        
        try:
            choice = input("Opção: ").strip()
            
            if choice == "0":
                print("\nEncerrando sistema de migração. Até logo!\n")
                break
            
            elif choice == "1":
                run_migration_clientes()
            
            elif choice == "2":
                run_migration_pets()
            
            elif choice == "3":
                run_migration_vacinas()
            
            elif choice == "4":
                run_update_cities()
            
            else:
                print("\n✗ Opção inválida. Por favor, escolha uma opção válida.\n")
        
        except KeyboardInterrupt:
            print("\n\nOperação cancelada pelo usuário. Até logo!\n")
            break
        except Exception as e:
            print(f"\n✗ Erro: {e}\n")
            if confirm_action("Deseja continuar?"):
                continue
            else:
                break


if __name__ == "__main__":
    main()
