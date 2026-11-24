from random import randint, choice
import json
import time
from kafka import KafkaProducer
from datetime import datetime


def main():
    print("\n" + "=" * 70)
    print("KAFKA PRODUCER - Envia eventos de produtos")
    print("=" * 70)
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("\nOK: Conectado aos brokers Kafka")
    except Exception as e:
        print(f"\nERRO: {e}")
        print("Verifique: docker compose ps")
        return
    
    list_product_name = ['Notebook', 'Mouse', 'Teclado', 'Monitor', 'Webcam']
    list_product_brand = ['lenovo', 'dell', 'vaio', 'sansumg', 'acer']
    list_product_amount = [20, 15, 8, 10, 5]
    list_product_price = [50.00, 150.00, 3000.00, 800.00, 250.00]
    
    print("\nEnviando eventos...\n")
    
    try:
        for ciclo in range(1000):
            print(f"\n--- Ciclo {ciclo + 1} ---\n")
            
            for item in range(5):
                amout = choice(list_product_amount)
                price = choice(list_product_price)
                product_name = f'{choice(list_product_name)} - {choice(list_product_brand)}'
                evento = {
                    'timestamp': datetime.now().isoformat(),
                    'produto_id': randint(1, 100000),
                    'produto_nome': product_name,
                    'quantidade': amout,
                    'preco': price,
                    'valor_total': amout * price
                }
                
                try:
                    future = producer.send('products-events', value=evento)
                    record_metadata = future.get(timeout=10)
                    
                    print(f"OK {product_name:<12} - Qtd: {amout:<3} - "
                          f"R$ {evento['valor_total']:>10,.2f} - Offset: {record_metadata.offset}")
                    
                except Exception as e:
                    print(f"ERRO {product_name}: {e}")
                
                time.sleep(0.5)
            
            if ciclo % 10 == 0:
                print(f"\nAguardando 5 segundos...")
                time.sleep(5)
        
        print("\n" + "=" * 70)
        print("OK: Todos os eventos foram enviados!")
        print("=" * 70)
        
    except KeyboardInterrupt:
        print("\n\nInterrompido pelo usuario")
    except Exception as e:
        print(f"\n\nERRO: {e}")
    finally:
        producer.flush()
        producer.close()
        print("OK: Producer fechado\n")


if __name__ == '__main__':
    main()

# Comando de execução
# python consumer_parallel.py --group vendas --instance 1
# python src/consumers/consumers_parallel/consumer_parallel.py --group vendas --instance 1
# python src/consumers/consumers_parallel/consumer_parallel.py --group vendas --instance 2
# python src/consumers/consumers_parallel/consumer_parallel.py --group vendas --instance 3