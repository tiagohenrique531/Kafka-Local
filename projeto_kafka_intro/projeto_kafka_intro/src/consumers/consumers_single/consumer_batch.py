import json
from kafka import KafkaConsumer

def consumer_batch():
    print("\n" + "=" * 70)
    print("KAFKA CONSUMER BATCH - processamento em Lotes")
    print("=" * 70)
    
    consumer = KafkaConsumer(
        'products-events',
        bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
        group_id='batch-group',
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        enable_auto_commit=True
    )
    
    print("\nProcessando em lotes (Ctrl+C para parar)...\n")
    
    batch_id = 0
    batch_size = 5
    batch = []
    
    try:
        for message in consumer:
            batch.append(message.value)
            
            if len(batch) >= batch_size:
                total_value = sum(item['valor_total'] for item in batch)
                item_count = len(batch)
                
                print(f"\nLOTE PROCESSADO {batch_id}: {item_count} itens | Valor total: R$ {total_value:>10,.2f}")
                
                for item in batch:
                    print(f"  -> {item['produto_nome']:<12} - R$ {item['valor_total']:>10,.2f}")
                
                print("-" * 70)
                batch_id+=1
                batch = []
    except KeyboardInterrupt:
        print("\nOK: Consumer interrompido")
    except Exception as e:
        print(f"\nERRO: {e}")
    finally:
        consumer.close()

def main():

    try:
        consumer_batch()
    except Exception as e:
        print(f"\nERRO: {e}")
        print("Verifique: docker compose ps")


if __name__ == '__main__':
    main()