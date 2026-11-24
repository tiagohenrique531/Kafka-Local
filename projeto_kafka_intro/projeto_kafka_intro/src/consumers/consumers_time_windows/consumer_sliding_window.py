"""
====================================================================================================
MÉDIA MÓVEL - 21:35:15 (últimos 5s)
====================================================================================================
  Monitor              | Eventos:  2 | Preço Médio: R$   800.00 | Qtd Média:    8.0 | Total: R$   12800.00
  Mouse                | Eventos:  3 | Preço Médio: R$    50.00 | Qtd Média:   15.0 | Total: R$     750.00
  Notebook             | Eventos:  1 | Preço Médio: R$  3000.00 | Qtd Média:    5.0 | Total: R$   15000.00
  Teclado              | Eventos:  2 | Preço Médio: R$   150.00 | Qtd Média:   11.5 | Total: R$    1725.00
  Webcam               | Eventos:  1 | Preço Médio: R$   250.00 | Qtd Média:   10.0 | Total: R$    2500.00


**Explicação:**
- **MÉDIA MÓVEL - 21:35:15 (últimos 5s):** Estatísticas dos eventos recebidos nos últimos 5 segundos
- **Diferença da Janela Fixa:** A janela "desliza" continuamente. Eventos antigos são removidos automaticamente quando passam de 5 segundos
- **Preço Médio:** Média dos preços unitários (não do valor total)
- **Qtd Média:** Média das quantidades vendidas
- **Total:** Soma dos valores totais na janela

#### Comportamento
- A cada 5 segundos, um novo relatório é gerado
- Eventos com mais de 5 segundos são automaticamente descartados
- Sempre mostra a "visão atual" das métricas
"""

import json
from collections import defaultdict
from datetime import datetime, timedelta
from kafka import KafkaConsumer

class RollingAverageConsumer:
    """
    Consumer que calcula média móvel em janela de tempo.
    Mostra a evolução do preço médio ao longo do tempo.
    """
    
    def __init__(self, topic, window_size_seconds=5, group_id="rolling-avg-group"):
        """
        Inicializa o consumer com média móvel.
        
        Args:
            topic (str): Nome do tópico Kafka
            window_size_seconds (int): Tamanho da janela em segundos
            group_id (str): ID do consumer group
        """
        self.topic = topic
        self.window_size = window_size_seconds
        self.group_id = group_id
        
        # Histórico de eventos com timestamp
        self.events_history = []
        self.last_window_report = datetime.now()
        
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
            group_id=group_id,
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            enable_auto_commit=True
        )
        
        print(f"✓ Consumer com Média Móvel Iniciado")
        print(f"  Tópico: {topic}")
        print(f"  Janela: {window_size_seconds} segundos")
        print(f"  Grupo: {group_id}")
        print()
    
    def clean_old_events(self):
        """Remove eventos fora da janela de tempo."""
        current_time = datetime.now()
        window_start = current_time - timedelta(seconds=self.window_size)
        
        # Manter apenas eventos dentro da janela
        self.events_history = [
            e for e in self.events_history 
            if e['timestamp'] >= window_start
        ]
    
    def add_event(self, message):
        """Adiciona evento ao histórico."""
        data = message.value
        
        event = {
            'timestamp': datetime.now(),
            'produto': data.get('produto_nome', 'desconhecido'),
            'preco': data.get('preco', 0),
            'quantidade': data.get('quantidade', 0),
            'valor': data.get('valor_total', 0)
        }
        
        self.events_history.append(event)
    
    def calculate_rolling_average(self):
        """Calcula estatísticas da janela atual."""
        self.clean_old_events()
        
        if not self.events_history:
            return None
        
        # Agrupar por produto
        produtos = defaultdict(list)
        for event in self.events_history:
            produtos[event['produto']].append(event)
        
        resultado = {}
        for produto, eventos in produtos.items():
            resultado[produto] = {
                'eventos': len(eventos),
                'preco_medio': sum(e['preco'] for e in eventos) / len(eventos),
                'quantidade_media': sum(e['quantidade'] for e in eventos) / len(eventos),
                'valor_total': sum(e['valor'] for e in eventos)
            }
        
        return resultado
    
    def report_window(self):
        """Gera relatório da janela atual."""
        stats = self.calculate_rolling_average()
        
        if not stats:
            return
        
        print("=" * 100)
        print(f"MÉDIA MÓVEL - {datetime.now().strftime('%H:%M:%S')} (últimos {self.window_size}s)")
        print("=" * 100)
        
        for produto, dados in sorted(stats.items()):
            print(f"  {produto:20} | Eventos: {dados['eventos']:2} | Preço Médio: R$ {dados['preco_medio']:8.2f} | Qtd Média: {dados['quantidade_media']:6.1f} | Total: R$ {dados['valor_total']:10.2f}")
        
        print()
    
    def run(self):
        """Executa o consumer com média móvel."""
        print("Aguardando mensagens (Ctrl+C para parar)...\n")
        
        try:
            for message in self.consumer:
                self.add_event(message)
                
                # Relatório a cada intervalo
                if (datetime.now() - self.last_window_report).total_seconds() >= self.window_size:
                    self.report_window()
                    self.last_window_report = datetime.now()
                
                print(f"[{datetime.now().strftime('%H:%M:%S')}] {message.value.get('produto_nome', '?'):15} - Preço: R$ {message.value.get('preco', 0):8.2f}")
        
        except KeyboardInterrupt:
            print("\n\nConsumer interrompido.")
            # Relatório final
            print("\nRelatório Final:")
            self.report_window()
        
        finally:
            self.consumer.close()


def main():
    
    print("\n🔹 Consumer com Média Móvel (5 segundos)")
    print("-" * 80)
    consumer = RollingAverageConsumer(
        topic='products-events',
        window_size_seconds=5,
        group_id='rolling-avg-group'
    )
    consumer.run()

if __name__ == '__main__':
    main()