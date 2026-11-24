# Explicação Detalhada do docker-compose.yaml

Este documento explica cada linha do arquivo `docker-compose.yaml`, que sobe um cluster Apache
Kafka com **3 brokers**, o **Zookeeper** e a interface de monitoramento **Kafdrop**.

> **Versões:** as imagens `confluentinc/cp-*:7.5.0` são da **Confluent Platform 7.5**, que traz o
> **Apache Kafka 3.5**. O Kafdrop está na versão **4.3.1**.

---

## Estrutura Geral

```yaml
services:
```
**Linha 1:** Inicia a seção de definição dos serviços (containers) que serão executados.

> **E o `version:`?** Arquivos antigos começavam com `version: '3.8'`. O Docker Compose atual
> (`docker compose`, v2) **ignora** esse campo e mostra o aviso
> `the attribute 'version' is obsolete`. Por isso ele não aparece mais aqui.

---

## 1. Zookeeper

```yaml
  zookeeper:
```
**Linha 3:** Define o serviço Zookeeper, responsável por:
- Gerenciar metadados do cluster Kafka
- Coordenar a eleição do **controller** (o broker que comanda as eleições de líder das partições)
- Manter a configuração de tópicos e brokers
- Armazenar ACLs e quotas

```yaml
    image: confluentinc/cp-zookeeper:7.5.0
```
**Linha 4:** Imagem Docker do Zookeeper distribuída pela Confluent, versão 7.5.0.

```yaml
    environment:
```
**Linha 5:** Inicia as configurações por variáveis de ambiente.

```yaml
      ZOOKEEPER_CLIENT_PORT: 2181
```
**Linha 6:** Porta onde o Zookeeper escuta conexões de clientes (os brokers). O padrão é 2181.

```yaml
      ZOOKEEPER_TICK_TIME: 2000
```
**Linha 7:** Unidade básica de tempo do Zookeeper, em milissegundos. Os prazos de sessão e de
detecção de falhas são múltiplos dela. 2000 ms = 2 segundos.

```yaml
    ports:
      - "2181:2181"
```
**Linhas 8-9:** Publica a porta 2181 do container na porta 2181 do host (formato `HOST:CONTAINER`).
Os brokers falam com o Zookeeper **pela rede interna**, então esta publicação é **opcional**:
serve só para inspecionar o Zookeeper a partir do seu computador.

```yaml
    networks:
      - kafka-network
```
**Linhas 10-11:** Conecta o Zookeeper à rede Docker `kafka-network`, onde os containers se
encontram pelo **nome do serviço**.

---

## 2. Kafka Broker 1

```yaml
  kafka-broker-1:
```
**Linha 14:** Define o primeiro broker Kafka.

```yaml
    image: confluentinc/cp-kafka:7.5.0
```
**Linha 15:** Imagem Docker do Kafka distribuída pela Confluent (CP 7.5.0 = Kafka 3.5).

```yaml
    depends_on:
      - zookeeper
```
**Linhas 16-17:** Define a **ordem de partida**: o container do Zookeeper é iniciado antes deste
broker. **Atenção:** o `depends_on` não espera o Zookeeper ficar **pronto**, só espera o container
**iniciar**. Por isso o cluster precisa de 20 a 30 segundos depois do `docker compose up -d`.

```yaml
    ports:
      - "9092:9092"
```
**Linhas 18-19:** Publica a porta **9092** no host. É por ela que os programas Python (rodando
**fora** do Docker) se conectam a este broker.

```yaml
    environment:
```
**Linha 20:** Inicia as variáveis de ambiente do Kafka.

```yaml
      KAFKA_BROKER_ID: 1
```
**Linha 21:** Identificador do broker no cluster. Precisa ser único: 1, 2 e 3.

```yaml
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
```
**Linha 22:** Endereço do Zookeeper: o **nome do serviço** (`zookeeper`) e a porta 2181.

```yaml
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-broker-1:29092,PLAINTEXT_HOST://localhost:9092
```
**Linha 23:** **CONFIGURAÇÃO CRÍTICA.** Define os endereços que o broker **anuncia** aos clientes.
O cliente conecta em qualquer broker, recebe esses endereços e passa a usá-los. Por isso há dois:
- `PLAINTEXT://kafka-broker-1:29092`: para quem está **dentro** da rede Docker (os outros
  brokers e o Kafdrop). A porta **29092** é interna e não é publicada no host.
- `PLAINTEXT_HOST://localhost:9092`: para quem está **fora** do Docker (os programas Python).

Se o broker anunciasse só `kafka-broker-1:29092`, o Python no seu computador não conseguiria
resolver esse nome e a conexão falharia.

```yaml
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
```
**Linha 24:** Associa cada nome de listener a um protocolo de segurança:
- `PLAINTEXT` → sem criptografia nem autenticação
- `PLAINTEXT_HOST` → também sem criptografia (o nome é só um rótulo para o listener externo)

```yaml
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
```
**Linha 25:** Qual listener os brokers usam para falar **entre si** (replicação): o interno.

```yaml
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
```
**Linha 26:** Fator de replicação do tópico interno `__consumer_offsets`, que guarda até onde
cada consumer group já leu. Com 3, essa informação existe nos 3 brokers.

```yaml
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
```
**Linha 27:** Desliga a criação automática de tópicos. O tópico precisa ser criado antes (pelo
`manager.py`). Isso evita tópicos criados por engano com um nome digitado errado.

```yaml
      KAFKA_LOG_RETENTION_MS: 3600000
```
**Linha 28:** Tempo de **retenção** das mensagens: 3.600.000 ms = **1 hora**. Depois disso o Kafka
pode apagar as mensagens antigas, **mesmo que ninguém as tenha lido**. (O padrão do Kafka é 7
dias; aqui foi reduzido para economizar disco no laboratório.)

```yaml
    networks:
      - kafka-network
```
**Linhas 29-30:** Conecta o broker à rede `kafka-network`.

---

## 3. Kafka Broker 2

```yaml
  kafka-broker-2:
```
**Linha 33:** Define o segundo broker Kafka.

**Linhas 34-49:** Mesma configuração do Broker 1, com estas diferenças:
- **Porta publicada 9093** (linha 38)
- **KAFKA_BROKER_ID: 2** (linha 40)
- **KAFKA_ADVERTISED_LISTENERS** (linha 42):
  - Interno: `kafka-broker-2:29093`
  - Externo: `localhost:9093`

---

## 4. Kafka Broker 3

```yaml
  kafka-broker-3:
```
**Linha 52:** Define o terceiro broker Kafka.

**Linhas 53-68:** Mesma configuração dos anteriores, com estas diferenças:
- **Porta publicada 9094** (linha 57)
- **KAFKA_BROKER_ID: 3** (linha 59)
- **KAFKA_ADVERTISED_LISTENERS** (linha 61):
  - Interno: `kafka-broker-3:29094`
  - Externo: `localhost:9094`

---

## 5. Kafdrop (Interface Web)

```yaml
  kafdrop:
```
**Linha 71:** Define o serviço Kafdrop, uma interface web para monitorar o Kafka.

```yaml
    image: obsidiandynamics/kafdrop:4.3.1
```
**Linha 72:** Imagem do Kafdrop na versão **4.3.1**. A versão é fixada (em vez de `latest`) para
que o projeto funcione igual hoje e daqui a um ano: com `latest`, uma versão nova poderia mudar
a interface ou quebrar a configuração sem aviso.

```yaml
    depends_on:
      - kafka-broker-1
      - kafka-broker-2
      - kafka-broker-3
```
**Linhas 73-76:** Inicia o Kafdrop **depois** dos 3 brokers (de novo, só a ordem de partida).

```yaml
    ports:
      - "9000:9000"
```
**Linhas 77-78:** Interface web acessível em http://localhost:9000.

```yaml
    environment:
```
**Linha 79:** Variáveis de ambiente do Kafdrop.

```yaml
      KAFKA_BROKERCONNECT: kafka-broker-1:29092,kafka-broker-2:29093,kafka-broker-3:29094
```
**Linha 80:** Brokers aos quais o Kafdrop se conecta. Como ele roda **dentro** da rede Docker, usa
os nomes dos serviços e as portas **internas** (29092 a 29094). Por isso, na tela do Kafdrop, os
brokers aparecem como `kafka-broker-1:29092`, e não `localhost:9092`.

```yaml
      JVM_OPTS: "-Xms32M -Xmx64M"
```
**Linha 81:** Memória da JVM (Java Virtual Machine) do Kafdrop:
- `-Xms32M`: memória inicial de 32 MB
- `-Xmx64M`: memória máxima de 64 MB
(Valores baixos para economizar recursos.)

```yaml
      SERVER_SERVLET_CONTEXTPATH: "/"
```
**Linha 82:** Caminho raiz da aplicação web: ela responde direto em `/`.

```yaml
    networks:
      - kafka-network
```
**Linhas 83-84:** Conecta o Kafdrop à rede `kafka-network`.

---

## 6. Redes (Networks)

```yaml
networks:
  kafka-network:
    driver: bridge
```
**Linhas 86-88:**
- Define uma rede personalizada chamada `kafka-network`
- O driver `bridge` cria uma rede interna isolada no host
- Dentro dela, os containers se encontram pelo **nome do serviço** (`zookeeper`,
  `kafka-broker-1`…), graças ao DNS interno do Docker

---

## Diagrama da Arquitetura

```
┌──────────────────────────────────────────────────────────────┐
│                    kafka-network (bridge)                    │
│                                                              │
│                     ┌──────────────┐                         │
│                     │  Zookeeper   │                         │
│                     │  :2181       │                         │
│                     └──────┬───────┘                         │
│            ┌───────────────┼───────────────┐                 │
│      ┌─────▼─────┐   ┌─────▼─────┐   ┌─────▼─────┐           │
│      │ Broker 1  │◄─►│ Broker 2  │◄─►│ Broker 3  │ replicação│
│      │ int:29092 │   │ int:29093 │   │ int:29094 │ (portas   │
│      │ ext:9092  │   │ ext:9093  │   │ ext:9094  │ internas) │
│      └─────▲─────┘   └─────▲─────┘   └─────▲─────┘           │
│            └───────────────┼───────────────┘                 │
│                      ┌─────┴────┐                            │
│                      │ Kafdrop  │ (usa as portas internas)   │
│                      │ :9000    │                            │
│                      └──────────┘                            │
└──────────────────────────────────────────────────────────────┘
        │ :2181        │ :9092       │ :9093       │ :9094       │ :9000
        ▼              ▼             ▼             ▼             ▼
     (opcional)    Programas Python no host: localhost:9092-9094    Navegador
```

---

## Portas Resumidas

| Serviço | Porta Interna | Porta Externa (Host) | Uso |
|---------|---------------|----------------------|-----|
| Zookeeper | 2181 | 2181 (opcional) | Metadados e coordenação do cluster |
| Kafka Broker 1 | 29092 | 9092 | Interna: brokers e Kafdrop. Externa: programas Python |
| Kafka Broker 2 | 29093 | 9093 | Interna: brokers e Kafdrop. Externa: programas Python |
| Kafka Broker 3 | 29094 | 9094 | Interna: brokers e Kafdrop. Externa: programas Python |
| Kafdrop | 9000 | 9000 | Interface web de monitoramento |

---

## Comandos Úteis

```bash
# Iniciar todos os serviços
docker compose up -d

# Ver status dos containers
docker compose ps

# Ver logs de um serviço específico
docker compose logs -f kafka-broker-1

# Parar e REMOVER os containers (apaga tópicos e mensagens)
docker compose down

# Parar SEM remover (tópicos e mensagens continuam lá)
docker compose stop
docker compose start

# Parar ou iniciar um broker específico (simular falha)
docker compose stop kafka-broker-2
docker compose start kafka-broker-2
```

> **Onde ficam os dados?** Este compose **não define volumes**: os tópicos e as mensagens ficam
> dentro dos containers. Por isso o `docker compose down`, que **remove** os containers, já apaga
> tudo. Para manter os dados entre aulas, use `stop`/`start`.

---

## Pontos Importantes

1. **Replicação:** com 3 brokers, um tópico pode ter fator de replicação de até 3.
2. **Alta disponibilidade dos brokers:** se 1 broker cair, as partições continuam funcionando,
   porque uma réplica em outro broker vira a nova líder.
3. **Ponto único de falha:** há **um só Zookeeper**. Se ele cair, o cluster não consegue eleger
   novos líderes nem criar tópicos. Em produção se usam 3 ou 5 nós de Zookeeper (ou o KRaft).
4. **ADVERTISED_LISTENERS:** é a configuração mais delicada: define como os clientes de dentro e
   de fora do Docker encontram cada broker.
5. **Auto Create Topics:** desligado, para forçar a criação explícita dos tópicos.
6. **Zookeeper x KRaft:** desde o **Kafka 3.3**, o Kafka pode funcionar sem Zookeeper, no modo
   **KRaft** (os próprios brokers guardam os metadados). No **Kafka 4.0** o Zookeeper foi
   removido. Este projeto usa Kafka 3.5 com Zookeeper porque a arquitetura fica mais fácil de
   visualizar em aula.

---

## Troubleshooting

### Erro: "NoBrokersAvailable" ou "NodeNotReadyError"
- **Causa mais comum:** os brokers ainda estão iniciando.
- **Solução:** espere 20 a 30 segundos depois do `docker compose up -d`. Confira com
  `docker compose logs kafka-broker-1 | grep started`.
- **Se persistir:** verifique se o `KAFKA_ADVERTISED_LISTENERS` anuncia `localhost` nas portas
  9092 a 9094.

### Container não inicia: "port is already allocated"
- **Causa:** outro programa (ou outro projeto Docker) já usa a porta (9000, 9092, 9093, 9094 ou
  2181).
- **Solução:** descubra quem usa a porta (`docker ps` para outros containers; `netstat -ano` no
  Windows ou `ss -ltnp` no Linux) e pare esse programa. Outra opção é trocar a porta **do host**
  no mapeamento, por exemplo `"9001:9000"` para o Kafdrop. Não troque as portas 9092 a 9094 sem
  mudar também o `KAFKA_ADVERTISED_LISTENERS` e os programas Python.

### Consumer não recebe mensagens
- **Causa 1:** o tópico não existe. Com a criação automática desligada, crie com o `manager.py`.
- **Causa 2:** o consumer lê um tópico diferente do producer. Confira o nome nos dois.
- **Causa 3:** o grupo já leu tudo. Um consumer group continua de onde parou; para reler desde
  o início, use um `group_id` novo.

---

**Versão:** 2.0
**Última atualização:** Novembro 2026
**Versões:** Confluent Platform 7.5.0 (Apache Kafka 3.5) e Kafdrop 4.3.1
