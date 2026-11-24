import json
from kafka import KafkaConsumer


def consumer_basic():
    print("\n" + "=" * 70)
    print("KAFKA CONSUMER BÁSICO - Leitura Simples")
    print("=" * 70)
    
    consumer = KafkaConsumer(
        'products-events',
        bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
        group_id='basic-group',
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        enable_auto_commit=True
    )
    
    print("\nAguardando mensagens (Ctrl+C para parar)...\n")
    
    try:
        for message in consumer:
            data = message.value
            print(f"Topico: {message.topic:20} | Particao: {message.partition} | "
                  f"Offset: {message.offset:<5} | {data['produto_nome']:<12} - "
                  f"R$ {data['valor_total']:>10,.2f}")
    except KeyboardInterrupt:
        print("\nOK: Consumer interrompido")
    except Exception as e:
        print(f"\nERRO: {e}")
    finally:
        consumer.close()

def main():
    
    try:
        consumer_basic()
    except Exception as e:
        print(f"\nERRO: {e}")
        print("Verifique: docker compose ps")

if __name__ == '__main__':
    main()