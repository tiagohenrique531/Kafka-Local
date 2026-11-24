import json
from datetime import datetime
from kafka import KafkaConsumer

class SessionWindowConsumer:
    """
    Consumer com sessões de tempo.
    Agrupa eventos em sessões com intervalo de inatividade.
    """
    
    def __init__(self, topic, inactivity_timeout_seconds=5, group_id="session-window-group"):
        """
        Inicializa o consumer com janela de sessão.
        
        Args:
            topic (str): Nome do tópico Kafka
            inactivity_timeout_seconds (int): Tempo máximo sem mensagens
            group_id (str): ID do consumer group
        """
        self.topic = topic
        self.inactivity_timeout = inactivity_timeout_seconds
        self.group_id = group_id
        
        # Sessão atual
        self.session_events = []
        self.last_event_time = None
        self.session_id = 1
        
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
            group_id=group_id,
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            enable_auto_commit=True,
            # consumer_timeout_ms=self.inactivity_timeout * 1000  # REMOVIDO: Fazia o consumer parar após timeout
        )
        
        print(f"✓ Consumer com Janela de Sessão Iniciado")
        print(f"  Tópico: {topic}")
        print(f"  Timeout: {inactivity_timeout_seconds} segundos")
        print(f"  Grupo: {group_id}")
        print()
    
    def check_session_timeout(self):
        """Verifica se a sessão expirou por inatividade."""
        if self.last_event_time is None:
            return False
        
        elapsed = (datetime.now() - self.last_event_time).total_seconds()
        return elapsed > self.inactivity_timeout
    
    def end_session(self):
        """Encerra a sessão atual e processa dados."""
        if not self.session_events:
            return
        
        print("=" * 80)
        print(f"SESSÃO #{self.session_id} ENCERRADA (Timeout de Inatividade)")
        print("=" * 80)
        
        total_eventos = len(self.session_events)
        total_valor = sum(e['valor'] for e in self.session_events)
        duracao = (self.session_events[-1]['timestamp'] - self.session_events[0]['timestamp']).total_seconds()
        
        print(f"Total de eventos: {total_eventos}")
        print(f"Valor total: R$ {total_valor:.2f}")
        print(f"Duração: {duracao:.1f} segundos")
        print()
        
        # Detalhes de cada evento
        for evento in self.session_events:
            print(f"  {evento['timestamp'].strftime('%H:%M:%S')} | {evento['produto']:15} | R$ {evento['valor']:10.2f}")
        
        print()
    
    def add_event_to_session(self, message):
        """Adiciona evento à sessão."""
        data = message.value
        
        evento = {
            'timestamp': datetime.now(),
            'produto': data.get('produto_nome', 'desconhecido'),
            'valor': data.get('valor_total', 0),
            'quantidade': data.get('quantidade', 0)
        }
        
        self.session_events.append(evento)
        self.last_event_time = datetime.now()
    
    def run(self):
        """Executa o consumer com sessões."""
        print("Aguardando mensagens (Ctrl+C para parar)...\n")
        print(f"Sessão será encerrada após {self.inactivity_timeout} segundos sem mensagens\n")
        
        try:
            for message in self.consumer:
                # Verificar se sessão anterior expirou
                if self.check_session_timeout():
                    self.end_session()
                    self.session_id += 1
                    self.session_events = []
                
                # Adicionar evento à sessão atual
                self.add_event_to_session(message)
                
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Evento adicionado à Sessão #{self.session_id}")
        
        except KeyboardInterrupt:
            print("\n\nConsumer interrompido.")
            # Processar última sessão
            if self.session_events:
                print("\nProcessando última sessão:")
                self.end_session()
        
        finally:
            self.consumer.close()

def main():
    inatividade = 5  # segundos sem mensagem para encerrar a sessao

    print(f"\n🔹 Consumer com Sessão ({inatividade} segundos de inatividade)")
    print("-" * 80)
    consumer = SessionWindowConsumer(
        topic='products-events',
        inactivity_timeout_seconds=inatividade,
        group_id='session-window-group'
    )
    consumer.run()

if __name__ == '__main__':
    main()