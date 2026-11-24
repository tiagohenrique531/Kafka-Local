import json
from kafka import KafkaConsumer

def consumer_aggregate():
    print("\n" + "=" * 70)
    print("CONSUMER AGREGACAO - Sumario de Vendas")
    print("=" * 70)
    
    consumer = KafkaConsumer(
        'products-events',
        bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
        group_id='agregacao-group',
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        enable_auto_commit=True
    )
    
    print("\nAgregando dados (Ctrl+C para parar)...\n")
    
    vendas = {}
    contador = 0
    
    try:
        for message in consumer:
            data = message.value
            produto = data['produto_nome']
            
            if produto not in vendas:
                vendas[produto] = {'quantidade': 0, 'valor_total': 0, 'eventos': 0}
            
            vendas[produto]['quantidade'] += data['quantidade']
            vendas[produto]['valor_total'] += data['valor_total']
            vendas[produto]['eventos'] += 1
            contador += 1
            
            if contador % 10 == 0:
                print(f"\nSUMÁRIO APÓS {contador} MENSAGENS\n")
                for prod, stats in sorted(vendas.items()):
                    print(f"{prod:12} | Qtd: {stats['quantidade']:>5} | "
                          f"Receita: R$ {stats['valor_total']:>10,.2f} | "
                          f"Eventos: {stats['eventos']}")
                print("-" * 70)
    except KeyboardInterrupt:
        print("\nOK: Consumer interrompido")
    except Exception as e:
        print(f"\nERRO: {e}")
    finally:
        consumer.close()

def main():
    
    try:
        consumer_aggregate()
    except Exception as e:
        print(f"\nERRO: {e}")
        print("Verifique: docker compose ps")


if __name__ == '__main__':
    main()