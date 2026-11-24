from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


def get_admin_client():
    try:
        return KafkaAdminClient(
            bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
            client_id='admin-client'
        )
    except Exception as e:
        print(f"ERRO ao conectar: {e}")
        return None


def descrever_cluster(admin):
    try:
        print("\n" + "=" * 70)
        print("CLUSTER KAFKA")
        print("=" * 70)
        
        # describe_cluster() devolve um dicionario com 'brokers' e 'controller_id'
        cluster = admin.describe_cluster()
        print(f"Cluster ID: {cluster['cluster_id']}")
        print(f"Controller: Broker {cluster['controller_id']}")
        print(f"\nBrokers:")

        for broker in sorted(cluster['brokers'], key=lambda b: b['node_id']):
            print(f"  Broker {broker['node_id']}: {broker['host']}:{broker['port']}")
        
        print("=" * 70)
    except Exception as e:
        print(f"ERRO: {e}")


def listar_topicos(admin):
    try:
        print("\n" + "=" * 70)
        print("TOPICOS DISPONIVEIS")
        print("=" * 70)
        
        # describe_topics() devolve uma lista de dicionarios, um por topico.
        # Os topicos internos do Kafka (ex.: __consumer_offsets) ficam de fora.
        metadata = [t for t in admin.describe_topics() if not t['is_internal']]

        if not metadata:
            print("Nenhum topico encontrado")
            return

        for topico in sorted(metadata, key=lambda t: t['topic']):
            print(f"\nTopico: {topico['topic']}")
            print(f"Particoes: {len(topico['partitions'])}")

            for particao in sorted(topico['partitions'], key=lambda p: p['partition']):
                print(f"  Particao {particao['partition']}:")
                print(f"    Leader: {particao['leader']}")
                print(f"    Replicas: {particao['replicas']}")
                print(f"    ISR: {particao['isr']}")
        
        print("\n" + "=" * 70)
    except Exception as e:
        print(f"ERRO: {e}")


def criar_topico(admin):
    try:
        nome = input("\nNome do topico: ").strip()
        particoes = int(input("Numero de particoes (default 3): ").strip() or "3")
        replicacao = int(input("Fator de replicacao (default 3): ").strip() or "3")
        
        topico = NewTopic(
            name=nome,
            num_partitions=particoes,
            replication_factor=replicacao
        )
        
        admin.create_topics([topico])
        print(f"\nOK: Topico '{nome}' criado!")
        print(f"  Particoes: {particoes}")
        print(f"  Replicacao: {replicacao}")
    except TopicAlreadyExistsError:
        print(f"\nAVISO: Topico ja existe")
    except Exception as e:
        print(f"\nERRO: {e}")


def deletar_topico(admin):
    try:
        nome = input("\nNome do topico a deletar: ").strip()
        confirmacao = input(f"Confirmar delecao de '{nome}'? (s/n): ").strip().lower()
        
        if confirmacao == 's':
            admin.delete_topics([nome])
            print(f"\nOK: Topico '{nome}' deletado")
        else:
            print("\nCancelado")
    except Exception as e:
        print(f"\nERRO: {e}")


def criar_topicos_demo(admin):
    try:
        print("\nCriando topicos de demonstracao...\n")
        
        topicos = [
            NewTopic(name='aula-events', num_partitions=3, replication_factor=3),
            NewTopic(name='products-events', num_partitions=3, replication_factor=3),
            NewTopic(name='user-actions', num_partitions=2, replication_factor=3),
            NewTopic(name='logs', num_partitions=4, replication_factor=2),
        ]
        
        admin.create_topics(topicos)
        
        print("OK: Topicos criados!")
        print("\nTopicos:")
        print("  - aula-events (3 particoes, RF=3)")
        print("  - products-events (3 particoes, RF=3)")
        print("  - user-actions (2 particoes, RF=3)")
        print("  - logs (4 particoes, RF=2)")
    except TopicAlreadyExistsError:
        print("AVISO: Um ou mais topicos ja existem")
    except Exception as e:
        print(f"ERRO: {e}")


def main():
    print("\n" + "=" * 70)
    print("GERENCIADOR DE TOPICOS KAFKA")
    print("=" * 70)
    print("1. Descrever cluster")
    print("2. Listar topicos")
    print("3. Criar novo topico")
    print("4. Deletar topico")
    print("5. Criar topicos de demonstracao")
    print("6. Sair")
    print("=" * 70)
    
    admin = get_admin_client()
    if not admin:
        print("ERRO: Nao conseguiu conectar ao Kafka")
        print("Verifique: docker-compose ps")
        return
    
    try:
        while True:
            escolha = input("\nDigite a opcao (1-6): ").strip()
            
            if escolha == '1':
                descrever_cluster(admin)
            elif escolha == '2':
                listar_topicos(admin)
            elif escolha == '3':
                criar_topico(admin)
            elif escolha == '4':
                deletar_topico(admin)
            elif escolha == '5':
                criar_topicos_demo(admin)
            elif escolha == '6':
                print("\nEncerrando...")
                break
            else:
                print("\nOpcao invalida")
    except KeyboardInterrupt:
        print("\n\nEncerrando...")
    except Exception as e:
        print(f"\nERRO: {e}")
    finally:
        admin.close()

if __name__ == '__main__':
    main()