#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import argparse
import time
from kafka import KafkaConsumer, ConsumerRebalanceListener


class AvisoRebalanceamento(ConsumerRebalanceListener):
    """Mostra na tela quando o Kafka tira ou entrega particoes a esta instancia."""

    def __init__(self, instance_id):
        self.instance_id = instance_id

    def on_partitions_revoked(self, revoked):
        if revoked:
            particoes = sorted(tp.partition for tp in revoked)
            print(f"\n>>> [Inst {self.instance_id}] REBALANCEAMENTO: perdeu as particoes {particoes}\n")

    def on_partitions_assigned(self, assigned):
        particoes = sorted(tp.partition for tp in assigned)
        if particoes:
            print(f"\n>>> [Inst {self.instance_id}] Particoes atribuidas: {particoes}\n")
        else:
            print(f"\n>>> [Inst {self.instance_id}] Nenhuma particao atribuida: instancia OCIOSA\n")


def consumer_paralelo(group_id, instance_id):
    
    print("\n" + "=" * 70)
    print(f"CONSUMER PARALELO - Instancia {instance_id}")
    print("=" * 70)
    print(f"Grupo: {group_id}")
    print(f"Instancia: {instance_id}")
    print("\nAguardando rebalancing (Ctrl+C para parar)...\n")
    
    consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
        group_id=group_id,
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        enable_auto_commit=True,
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000
    )
    # Assina o topico avisando a cada rebalanceamento
    consumer.subscribe(['products-events'], listener=AvisoRebalanceamento(instance_id))
    
    contador = 0
    inicio = time.time()
    
    try:
        for message in consumer:
            contador += 1
            data = message.value
            
            print(f"[Inst {instance_id}] Particao {message.partition:1} | "
                  f"Offset {message.offset:5} | {data['produto_nome']:12} - "
                  f"R$ {data['valor_total']:>10,.2f}")
            
            if contador % 10 == 0:
                tempo = time.time() - inicio
                taxa = contador / tempo
                print(f"\n  -> {contador} mensagens em {tempo:.1f}s ({taxa:.1f} msg/s)\n")
    
    except KeyboardInterrupt:
        tempo = time.time() - inicio
        taxa = contador / tempo if tempo > 0 else 0
        
        print(f"\n\n{'=' * 70}")
        print(f"Instancia {instance_id} finalizada")
        print(f"Mensagens: {contador}")
        print(f"Tempo: {tempo:.1f}s")
        print(f"Taxa: {taxa:.2f} msg/s")
        print(f"{'=' * 70}\n")
    
    except Exception as e:
        print(f"\nERRO: {e}")
    finally:
        consumer.close()


def main():
    parser = argparse.ArgumentParser(
        description='Consumer paralelo para rebalancing'
    )
    parser.add_argument('--group', default='paralelo-group', help='ID do grupo')
    parser.add_argument('--instance', type=int, default=1, help='ID da instancia')
    
    args = parser.parse_args()
    
    print("\n" + "=" * 70)
    print("CONSUMER PARALELO - Demonstracao de Rebalancing")
    print("=" * 70)
    
    print(f"""
        Para demonstrar rebalancing, execute em MULTIPLOS TERMINAIS:

        Terminal 1:
            python consumer_parallel.py --group vendas --instance 1

        Terminal 2:
            python consumer_parallel.py --group vendas --instance 2

        Terminal 3:
            python consumer_parallel.py --group vendas --instance 3

        RESULTADO:
        OK Cada instancia recebe de particoes diferentes
        OK Kafka redistribui automaticamente
        OK Nenhuma mensagem duplicada

        MONITORAR: http://localhost:9000
        """)
    
    try:
        consumer_paralelo(args.group, args.instance)
    except Exception as e:
        print(f"\nERRO: {e}")


if __name__ == '__main__':
    main()