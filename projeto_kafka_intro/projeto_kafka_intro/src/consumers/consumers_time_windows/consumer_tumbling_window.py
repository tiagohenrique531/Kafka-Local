"""
================================================================================
JANELA DE TEMPO ENCERRADA
Período: 21:30:15 - 21:30:25
================================================================================

Produto: Monitor             | Eventos:   2 | Qtd Total:    16 | Total: R$   12800.00 | Preço Médio: R$  6400.00
Produto: Mouse               | Eventos:   3 | Qtd Total:    45 | Total: R$    2250.00 | Preço Médio: R$   750.00
Produto: Notebook            | Eventos:   2 | Qtd Total:    10 | Total: R$   30000.00 | Preço Médio: R$ 15000.00
Produto: Teclado             | Eventos:   1 | Qtd Total:     8 | Total: R$    1200.00 | Preço Médio: R$  1200.00
Produto: Webcam              | Eventos:   2 | Qtd Total:    20 | Total: R$    5000.00 | Preço Médio: R$  2500.00
--------------------------------------------------------------------------------
RESUMO GERAL         | Eventos:  10 | Qtd Total:    99 | Total: R$   51250.00


**Explicação:**
- **Período:** Janela de tempo (21:30:15 até 21:30:25 = 10 segundos)
- **Por Produto:**
  - `Eventos:` Quantas vezes o produto apareceu na janela
  - `Qtd Total:` Soma das quantidades vendidas
  - `Total:` Receita total do produto na janela
  - `Preço Médio:` Valor médio das transações
- **RESUMO GERAL:** Agregação de todos os produtos da janela

#### Nova Janela Inicia Automaticamente
Após processar, uma nova janela de 10 segundos começa imediatamente.

"""

import json
import time
from collections import defaultdict
from datetime import datetime, timedelta
from kafka import KafkaConsumer


class TimeWindowConsumer:
    """
    Consumer que agrupa mensagens em janelas de tempo.
    
    Exemplo: A cada 10 segundos, calcula o total de vendas
    dos últimos 10 segundos.
    """
    
    def __init__(self, topic, window_size_seconds=10, group_id="time-window-group"):
        """
        Inicializa o consumer com janela de tempo.
        
        Args:
            topic (str): Nome do tópico Kafka
            window_size_seconds (int): Tamanho da janela em segundos
            group_id (str): ID do consumer group
        """
        self.topic = topic
        self.window_size = window_size_seconds
        self.group_id = group_id
        
        # Buffer para armazenar eventos da janela atual
        self.window_events = defaultdict(list)
        self.window_start_time = datetime.now()
        
        # Conectar ao Kafka
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
            group_id=group_id,
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            enable_auto_commit=True,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000
        )
        
        print(f"✓ Consumer com Janela de Tempo Iniciado")
        print(f"  Tópico: {topic}")
        print(f"  Janela: {window_size_seconds} segundos")
        print(f"  Grupo: {group_id}")
        print()
    
    def check_window_expiration(self):
        """
        Verifica se a janela de tempo expirou.
        Se sim, processa a janela e inicia uma nova.
        """
        current_time = datetime.now()
        window_end_time = self.window_start_time + timedelta(seconds=self.window_size)
        
        if current_time >= window_end_time:
            # Janela expirou - processar dados
            self.process_window()
            
            # Iniciar nova janela
            self.window_start_time = current_time
            self.window_events.clear()
            
            return True
        
        return False
    
    def add_event_to_window(self, message):
        """Adiciona um evento à janela atual."""
        data = message.value
        
        # Extrair informações principais
        produto = data.get('produto_nome', 'desconhecido')
        valor = data.get('valor_total', 0)
        quantidade = data.get('quantidade', 0)
        timestamp = data.get('timestamp', datetime.now().isoformat())
        
        # Armazenar evento com timestamp
        event = {
            'timestamp': timestamp,
            'produto': produto,
            'valor': valor,
            'quantidade': quantidade,
            'offset': message.offset,
            'particao': message.partition
        }
        
        # Agrupar por produto
        self.window_events[produto].append(event)
    
    def process_window(self):
        """Processa os dados acumulados na janela."""
        if not self.window_events:
            return
        
        # Calcular estatísticas por produto
        print("=" * 80)
        print(f"JANELA DE TEMPO ENCERRADA")
        print(f"Período: {self.window_start_time.strftime('%H:%M:%S')} - {datetime.now().strftime('%H:%M:%S')}")
        print("=" * 80)
        print()
        
        total_geral = 0
        quantidade_total = 0
        eventos_total = 0
        
        for produto, eventos in sorted(self.window_events.items()):
            total_produto = sum(e['valor'] for e in eventos)
            qtd_produto = sum(e['quantidade'] for e in eventos)
            eventos_produto = len(eventos)
            preco_medio = total_produto / eventos_produto if eventos_produto > 0 else 0
            
            print(f"Produto: {produto:20} | Eventos: {eventos_produto:3} | Qtd Total: {qtd_produto:5} | Total: R$ {total_produto:10.2f} | Preço Médio: R$ {preco_medio:8.2f}")
            
            total_geral += total_produto
            quantidade_total += qtd_produto
            eventos_total += eventos_produto
        
        print("-" * 80)
        print(f"{'RESUMO GERAL':20} | Eventos: {eventos_total:3} | Qtd Total: {quantidade_total:5} | Total: R$ {total_geral:10.2f}")
        print()
    
    def run(self):
        """Executa o consumer com janela de tempo."""
        print("Aguardando mensagens (Ctrl+C para parar)...\n")
        
        try:
            for message in self.consumer:
                # Adicionar evento à janela
                self.add_event_to_window(message)
                
                # Verificar se janela expirou
                self.check_window_expiration()
                
                # Mostrar progresso
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Evento recebido: {message.value.get('produto_nome', '?'):15} - Offset: {message.offset:3} - Eventos na janela: {sum(len(v) for v in self.window_events.values())}")
        
        except KeyboardInterrupt:
            print("\n\nConsumer interrompido.")
            # Processar última janela
            if self.window_events:
                print("\nProcessando última janela:")
                self.process_window()
        
        finally:
            self.consumer.close()


def main():
    janela = 10  # tamanho da janela em segundos

    print(f"\n🔹 Consumer com Janela de Tempo ({janela} segundos)")
    print("-" * 80)

    consumer = TimeWindowConsumer(
        topic='products-events',
        window_size_seconds=janela,
        group_id='time-window-group'
    )
    consumer.run()

if __name__ == '__main__':
    main()