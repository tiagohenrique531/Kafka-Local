import json
from kafka import KafkaConsumer

LIMITE = 500  # so gera alerta para vendas acima deste valor (R$)

def consumer_filter():
    print("\n" + "=" * 70)
    print(f"CONSUMER FILTRO - Apenas valor > R$ {LIMITE}")
    print("=" * 70)
    
    consumer = KafkaConsumer(
        'products-events',
        bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
        group_id='filtro-group',
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        enable_auto_commit=True
    )
    
    print(f"\nMonitorando produtos > R$ {LIMITE} (Ctrl+C para parar)...\n")
    event_id = 0
    try:
        for message in consumer:
            data = message.value
            total_value = data['valor_total']
            
            if total_value > LIMITE:
                print(f"ALERTA {event_id}: {data['produto_nome']}")
                print(f"  Particao: {message.partition} | Offset: {message.offset}")
                print(f"  Valor: R$ {total_value:>10,.2f} "
                      f"(Qtd: {data['quantidade']} x R$ {data['preco']:.2f})")
                print("-" * 70)
                event_id +=1

    except KeyboardInterrupt:
        print("\nOK: Consumer interrompido")
    except Exception as e:
        print(f"\nERRO: {e}")
    finally:
        consumer.close()

def main():

    try:
        consumer_filter()
    except Exception as e:
        print(f"\nERRO: {e}")
        print("Verifique: docker compose ps")


if __name__ == '__main__':
    main()