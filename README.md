# 🌐 Ormuz: Sistema de Vigilância IoT com Blockchain Distribuído

🚀 Este projeto foi desenvolvido como solução para o **Problema 3 da disciplina TEC502**.\
Evolução do sistema anterior, agora com um **ledger blockchain distribuído** para registro imutável de missões, **pagamento de créditos entre empresas**, **assinaturas digitais Ed25519** nos laudos e um **cliente interativo** para auditoria e requisição de escoltas.

---

## 🧠 Conceito da Solução

O sistema monitora quatro setores geográficos (Norte, Sul, Leste, Oeste) com sensores que disparam alertas. Brokers coordenam entre si usando um **Token Ring** para atribuir drones a missões sem duplicação. Ao concluir uma missão, o broker registra um laudo assinado digitalmente na **blockchain distribuída**, debitando créditos da empresa solicitante. Empresas e auditores podem consultar saldos e laudos a qualquer momento pelo **cliente interativo**.

```
Sensores / Cliente  →  Broker (Token Ring)  →  Drones
        ↑                      ↕                   ↓
    alertas            Token Ring + State       missões
    saldo OK?           Update broadcast       concluídas
                               ↓
                      Ledger Blockchain (PoW)
                      laudo + débito de créditos
```

### 📡 Sensores (TCP)
- Geram alertas com valor numérico (70–100)
- Calculam prioridade automaticamente (1=médio, 2=alto, 3=crítico)
- Enviam via TCP com **fallback** para qualquer broker da lista
- Só emitem alertas para valores acima de 70

### 🔁 Broker — Token Ring
- Quatro brokers em anel: Norte, Sul, Leste, Oeste
- Somente o **detentor do token** atribui drones a alertas
- Token carrega o estado global: fila de alertas, status dos drones, log de missões
- **StateUpdate broadcast** replica o estado para todos os peers a cada round
- Token perdido → regeneração automática por eleição topológica (predecessor imediato morto = assumo)
- Ao concluir uma missão, **assina o laudo com Ed25519** e submete ao ledger

### 🤖 Drones (TCP bidirecional)
- Registram-se em **todos** os brokers ao iniciar
- Enviam heartbeat a cada 10s para todos os brokers
- Recebem ordens de missão (`DRONE_DISPATCH`) via TCP reverso (broker → drone)
- Reportam conclusão de missão (`DRONE_DONE`) com fallback de broker
- Resultado sorteado com pesos realistas: 60% rota segura, 20% obstáculo, 15% atividade suspeita, 5% área interditada
- Drone morto: missão vira **órfã** e é reinserida na fila com prioridade 3 (crítico)

### ⛓️ Ledger — Blockchain Distribuída
- Quatro nós independentes com Proof of Work (dificuldade 3 zeros)
- **Mempool** local: transações aguardam confirmação em bloco
- Sincronização automática entre nós via `SYNC_REQ` / `SYNC_RESP`
- Dois tipos de transação: `CREDIT_TRANSFER` (pagamento de escolta) e `MISSION_REPORT` (laudo imutável)
- Bloco gênese inicializa créditos de todas as empresas do consórcio
- Hash SHA-256 encadeado garante imutabilidade da cadeia

### 🖥️ Cliente — Console Interativo
- Menu de texto para empresas do consórcio (NavegacaoNorte/Sul/Leste/Oeste)
- **Consulta de saldo** via nó de ledger (com fallover)
- **Requisição de escolta** com verificação prévia de saldo e custo = prioridade × 10 créditos
- **Histórico de transações** filtrado por empresa ou visão global
- **Leitura de laudos** com hash de integridade verificado

---

## ⚙️ Tecnologias Utilizadas

- 🐹 **Go 1.22** — toda a lógica de brokers, sensores, drones, ledger e cliente
- 🌐 **net** — conexões TCP puras (sem framework)
- 📦 **encoding/json** — serialização de mensagens
- 🔐 **crypto/ed25519** — assinatura digital de laudos de missão
- 🔑 **crypto/sha256** — hashing de blocos (PoW) e geração de chaves de fallback
- 🔄 **sync** (Mutex / RWMutex) — proteção de estado concorrente
- 🐳 **Docker** — build multi-stage e orquestração via Compose

---

## 📁 Estrutura do Projeto

```
ormuz/
├── broker.go              # Broker com Token Ring, fila de alertas, despacho de drones, assinatura Ed25519 e integração com ledger
├── sensor.go              # Sensor virtual com geração de alertas e fallback TCP
├── drone.go               # Drone com servidor TCP, heartbeat, loop de missão e laudo probabilístico
├── ledger.go              # Nó blockchain com PoW, mempool, sincronização e consulta de saldo
├── client.go              # Console interativo: saldo, escolta, transações e laudos
├── go.mod                 # Módulo Go (ormuz, go 1.22)
├── dockerfile             # Build multi-stage: builder Alpine + runtime mínimo (TARGET seleciona o binário)
└── docker-compose.yaml    # 4 ledgers + 4 brokers + 8 sensores + 8 drones + 1 cliente
```

---

## 🔌 Portas e Protocolo

As portas internas utilizadas na comunicação dentro da rede Docker, bem como as portas mapeadas no host (conforme o `docker-compose.yaml`), são estruturadas da seguinte forma:

| Componente | Porta Interna (Docker) | Porta Host Mapeada | Protocolo | Direção |
| :--- | :--- | :--- | :--- | :--- |
| **Ledger (Norte)** | `7000` | `7007` | TCP | brokers / clientes / peers → ledger |
| **Ledger (Sul)** | `7000` | `7008` | TCP | brokers / clientes / peers → ledger |
| **Ledger (Leste)** | `7000` | `7010` | TCP | brokers / clientes / peers → ledger |
| **Ledger (Oeste)** | `7000` | `7016` | TCP | brokers / clientes / peers → ledger |
| **Broker (Norte)** | `5000` | `5007` | TCP | sensores / drones / peers → broker |
| **Broker (Sul)** | `5000` | `5008` | TCP | sensores / drones / peers → broker |
| **Broker (Leste)** | `5000` | `5010` | TCP | sensores / drones / peers → broker |
| **Broker (Oeste)** | `5000` | `5016` | TCP | sensores / drones / peers → broker |
| **Drone** | `6000` | — | TCP | broker → drone (`DRONE_DISPATCH`) |

Todas as mensagens trafegam como JSON delimitado por newline (`json.Encoder`).

---

## 📨 Tipos de Mensagem

### Broker ↔ Sensores / Drones / Peers

| Tipo               | Remetente   | Destinatário | Descrição                               |
|--------------------|-------------|------------- |-----------------------------------------|
| `ALERT`            | Sensor / Cliente | Broker  | Alerta com prioridade e setor           |
| `DRONE_REGISTER`   | Drone       | Broker(s)    | Cadastro inicial com endereço TCP       |
| `DRONE_HEARTBEAT`  | Drone       | Broker(s)    | Sinal de vida com status atual          |
| `DRONE_DONE`       | Drone       | Broker       | Missão concluída com laudo              |
| `DRONE_DISPATCH`   | Broker      | Drone        | Ordem de missão                         |
| `TOKEN`            | Broker      | Broker       | Passagem do token com estado global     |
| `STATE_UPDATE`     | Broker      | Broker(s)    | Réplica de estado após cada round       |
| `PEER_PING`        | Broker      | Broker       | Heartbeat entre brokers                 |
| `Cobertura_SECTOR` | Broker      | Broker(s)    | Aviso de cobertura de setor morto       |

### Broker / Cliente ↔ Ledger

| Tipo             | Remetente        | Destinatário | Descrição                              |
|------------------|------------------|--------------|----------------------------------------|
| `SUBMIT_TX`      | Broker / Cliente | Ledger       | Submete transação à mempool            |
| `NEW_BLOCK`      | Ledger           | Ledger(s)    | Anuncia bloco recém-minerado           |
| `SYNC_REQ`       | Ledger / Cliente | Ledger       | Pedido de sincronização da cadeia      |
| `SYNC_RESP`      | Ledger           | Ledger / Cliente | Cadeia completa em resposta          |
| `BALANCE_QUERY`  | Broker / Cliente | Ledger       | Consulta saldo de uma empresa          |
| `BALANCE_RESP`   | Ledger           | Broker / Cliente | Saldo atual                          |
| `PING`           | Ledger           | Ledger(s)    | Heartbeat entre nós do ledger          |

---

## 🚀 Como Executar

### Pré-requisitos
- Docker ≥ 24 e Docker Compose V2

### 1. Subir a infraestrutura

Execute em um terminal. O flag `-d` roda em segundo plano; omita-o se quiser ver os logs de todos os serviços.

```bash
docker compose up --build -d
```

Isso inicializa:
- **4 nós de ledger** (Norte, Sul, Leste, Oeste) nos IPs internos `10.200.0.7/8/10/16:7000` (mapeados no host em `localhost:7007/7008/7010/7016`)
- **4 brokers** (Norte, Sul, Leste, Oeste) nos IPs internos `10.200.0.17/18/20/26:5000` (mapeados no host em `localhost:5007/5008/5010/5016`)
- **8 sensores** (Radar e Naval por setor)
- **8 drones** (dois por setor)

> [!TIP]
> Aguarde alguns segundos antes de abrir o cliente para que os ledgers concluam a sincronização inicial e os drones se registrem nos brokers.

### 2. Abrir o cliente interativo

O serviço `client` já sobe junto com o `docker compose up` (configurado com `stdin_open: true` e `tty: true`). Para abrir o console interativo, anexe ao container em execução:

```bash
docker attach ormuz_client
```

Isso abre o console da empresa configurada em `COMPANY` (padrão: `NavegacaoNorte`):

```
  🚀 Bem-vindo ao Console Ormuz!
  Empresa : NavegacaoNorte
  Saldo   : 500 créditos

  ╔══════════════════════════════════════════════════════════════════╗
  ║         CONSÓRCIO ORMUZ — NavegacaoNorte                        ║
  ╠══════════════════════════════════════════════════════════════════╣
  ║  [1] Consultar saldo de créditos                                 ║
  ║  [2] Requisitar escolta de drone                                 ║
  ║  [3] Ver histórico de transações                                 ║
  ║  [4] Ler laudos de missão                                        ║
  ║  [0] Sair                                                        ║
  ╚══════════════════════════════════════════════════════════════════╝
```

Para iniciar um cliente de **outra empresa**, sobrescreva a variável `COMPANY` ao subir a stack (antes do `up`), ou suba um container avulso:

```bash
docker compose run --rm -it -e COMPANY=NavegacaoSul client
```

Empresas disponíveis: `NavegacaoNorte` · `NavegacaoSul` · `NavegacaoLeste` · `NavegacaoOeste` · `ConsorcioGeral`

### 3. Acompanhar os logs da infraestrutura

```bash
# Todos os serviços
docker compose logs -f

# Apenas os brokers
docker compose logs -f broker-norte broker-sul broker-leste broker-oeste

# Apenas os ledgers
docker compose logs -f ledger-norte ledger-sul ledger-leste ledger-oeste
```

### 4. Parar e limpar

```bash
docker compose down
```

---

## ⚙️ Variáveis de Ambiente

### Broker

| Variável       | Exemplo                                               | Descrição                          |
|----------------|-------------------------------------------------------|------------------------------------|
| `SECTOR_NAME`  | `Norte`                                               | Nome do setor                      |
| `PORT`         | `5000`                                                | Porta TCP de escuta                |
| `MY_IP`        | `10.200.0.17`                                         | IP próprio (para o ping)           |
| `RING_ADDRS`   | `10.200.0.17:5000,...`                                | Lista completa do anel             |
| `LEDGER_NODES` | `10.200.0.7:7000,...`                                 | Nós do ledger para submissão de tx |

### Sensor

| Variável      | Exemplo                                               | Descrição                        |
|---------------|-------------------------------------------------------|----------------------------------|
| `SECTOR_NAME` | `Norte`                                               | Setor de origem dos alertas      |
| `SENSOR_TYPE` | `Radar` ou `Naval`                                    | Tipo do sensor                   |
| `BROKER_LIST` | `10.200.0.17:5000,...`                                | Brokers (fallback em ordem)      |

### Drone

| Variável      | Exemplo                                               | Descrição                        |
|---------------|-------------------------------------------------------|----------------------------------|
| `SECTOR_NAME` | `Norte`                                               | Setor base do drone              |
| `DRONE_ID`    | `DRONE-NORTE-A`                                       | Prefixo do ID                    |
| `DRONE_PORT`  | `6000`                                                | Porta TCP para receber despachos |
| `MY_IP`       | `10.200.0.31`                                         | IP próprio (anunciado ao broker) |
| `BROKER_LIST` | `10.200.0.17:5000,...`                                | Brokers para registro e heartbeat|

### Ledger

| Variável      | Exemplo                                               | Descrição                        |
|---------------|-------------------------------------------------------|----------------------------------|
| `NODE_ID`     | `ledger-norte`                                        | Identificador único do nó        |
| `PORT`        | `7000`                                                | Porta TCP de escuta              |
| `GENESIS`     | `true` / `false`                                      | Se este nó emite o bloco gênese  |
| `DIFFICULTY`  | `3`                                                   | Zeros exigidos no PoW (SHA-256)  |
| `PEERS`       | `10.200.0.8:7000,...`                                 | Outros nós do ledger             |

### Cliente

| Variável       | Exemplo                                               | Descrição                           |
|----------------|-------------------------------------------------------|-------------------------------------|
| `COMPANY`      | `NavegacaoNorte`                                      | Empresa logada no console           |
| `LEDGER_NODES` | `10.200.0.7:7000,...`                                 | Nós do ledger para consultas        |
| `BROKER_LIST`  | `10.200.0.17:5000,...`                                | Brokers para envio de alertas       |

---

## 🔍 Funcionalidades

### Sensores
- ✅ Geração aleatória de alertas com intervalos de 5–15 s
- ✅ Filtragem: apenas valores > 70 geram alertas
- ✅ Cálculo automático de prioridade (1/2/3)
- ✅ IDs únicos de alerta (pool de 0–100, embaralhado)
- ✅ Fallback para brokers alternativos em caso de falha

### Brokers (Token Ring)
- ✅ Token Ring TCP com bypass automático de nós mortos
- ✅ Fila de alertas global ordenada por prioridade + timestamp
- ✅ Despacho de drone preferencial por setor geográfico
- ✅ StateUpdate broadcast após cada round (replicação de estado)
- ✅ Regeneração de token por eleição topológica (sem coordenador fixo)
- ✅ Missões órfãs: drone morto em missão → reinsere alerta com prioridade 3
- ✅ Log histórico imutável de missões (PENDENTE → DESPACHADA → CONCLUÍDA/FALHA/ÓRFÃ)
- ✅ Heartbeat entre peers a cada 8 s com detecção de reconexão
- ✅ **Assinatura Ed25519** do laudo ao marcar missão como CONCLUÍDA
- ✅ Verificação de assinaturas de todos os laudos no token (integridade distribuída)
- ✅ Chaves criptográficas persistidas em disco (`keys/<setor>.priv` / `.pub`)
- ✅ Integração com ledger: submete `CREDIT_TRANSFER` + `MISSION_REPORT` ao concluir missão
- ✅ Verifica saldo da empresa antes de despachar drone (débito só se saldo suficiente)

### Drones
- ✅ Registro simultâneo em todos os brokers no boot
- ✅ Servidor TCP reverso para receber `DRONE_DISPATCH`
- ✅ Heartbeat periódico (10 s) para todos os brokers
- ✅ Execução de missão com duração aleatória (10–20 s)
- ✅ Laudo sorteado por peso: 60% rota_segura, 20% obstaculo_detectado, 15% atividade_suspeita, 5% area_interditada
- ✅ Reporte de conclusão com fallback de broker
- ✅ Re-registro automático se todos os brokers estiverem inacessíveis após missão

### Ledger (Blockchain)
- ✅ Proof of Work com dificuldade configurável (SHA-256 com prefixo de zeros)
- ✅ Bloco gênese com emissão inicial de créditos para todas as empresas
- ✅ Mempool: transações aguardam agrupamento em bloco
- ✅ Propagação de blocos entre nós (`NEW_BLOCK`)
- ✅ Sincronização de cadeia completa (`SYNC_REQ` / `SYNC_RESP`) ao iniciar
- ✅ Regra da cadeia mais longa para resolução de conflitos
- ✅ Prevenção de duplicatas via `seenTxs` e `seenBlocks`
- ✅ Consulta de saldo em tempo real via `BALANCE_QUERY`
- ✅ Heartbeat entre nós para manter mapa de liveness

### Cliente Interativo
- ✅ Consulta de saldo da própria empresa ou de qualquer empresa do consórcio
- ✅ Visão consolidada de saldos de todas as empresas (calculada da blockchain)
- ✅ Requisição de escolta com verificação prévia de saldo (custo = prioridade × 10)
- ✅ Histórico completo de transações filtrado por empresa ou visão global
- ✅ Leitura de laudos de missão com hash de integridade

---

## 🏗️ Ciclo de Vida de uma Missão

```
Sensor ou Cliente gera alerta
            ↓
Broker recebe → armazena em localAlerts
            ↓
[Cliente] Ledger verifica saldo da empresa
            ↓
Token chega neste broker
            ↓
Alerta entra na fila global (ordenada por prioridade)
            ↓
Broker consulta saldo da empresa no ledger
  → Saldo insuficiente: alerta descartado
  → Saldo OK: continua
            ↓
Broker escolhe drone disponível (preferência: mesmo setor)
            ↓
DRONE_DISPATCH → drone via TCP direto
            ↓
Drone executa missão (10–20 s) → sorteia laudo
            ↓
DRONE_DONE → broker atualiza log: CONCLUÍDA
            ↓
Broker assina laudo com Ed25519
            ↓
Broker submete ao Ledger:
  • CREDIT_TRANSFER (empresa → SYSTEM, custo em créditos)
  • MISSION_REPORT (laudo imutável com hash)
            ↓
Ledger minera bloco com PoW → propaga NEW_BLOCK
```

---

## ⛓️ Modelo Econômico

As empresas do consórcio possuem créditos registrados na blockchain:

| Empresa           | Créditos iniciais |
|-------------------|:-----------------:|
| NavegacaoNorte    | 500               |
| NavegacaoSul      | 500               |
| NavegacaoLeste    | 500               |
| NavegacaoOeste    | 500               |
| ConsorcioGeral    | 1000              |

**Custo por missão** = `Prioridade × 10 créditos`

| Prioridade | Custo |
|-----------|:-----:|
| 1 — Médio  | 10    |
| 2 — Alto   | 20    |
| 3 — Crítico| 30    |

O débito é atômico: a transação só é submetida ao ledger após a conclusão confirmada da missão.

---

## 🛡️ Tolerância a Falhas

| Falha                          | Comportamento                                                    |
|--------------------------------|------------------------------------------------------------------|
| Broker cai                     | Token bypassado; peers detectam via ping (8 s)                   |
| Token perdido                  | Regeneração automática pelo predecessor imediato ativo           |
| Sensor não alcança broker      | Fallback sequencial para próximo broker da lista                 |
| Drone cai em missão            | Missão marcada ÓRFÃ; reinserida na fila com prioridade 3         |
| Drone cai ocioso               | Marcado FAILED após 30 s sem heartbeat; ignorado                 |
| Todos os brokers mortos        | Broker remanescente retém o token e opera sozinho                |
| Nó de ledger cai               | Broker usa fallover para outro nó; cadeia sincroniza ao voltar   |
| Saldo insuficiente da empresa  | Alerta descartado na fila; drone não é despachado                |

---

## 🔐 Segurança e Integridade

- **Ed25519**: cada broker gera um par de chaves ao iniciar (ou carrega do disco). Todo laudo de missão CONCLUÍDA é assinado pela chave privada do broker responsável.
- **Verificação distribuída**: ao receber o token, cada broker verifica as assinaturas de todos os laudos do log. Assinatura inválida gera alerta crítico no log.
- **Hash SHA-256**: cada bloco da blockchain encadeia o hash do anterior. Qualquer adulteração invalida toda a cadeia subsequente.
- **Hash de laudo**: o payload de `MISSION_REPORT` contém um hash do conteúdo do laudo para verificação adicional no cliente.

---

## 💡 Resumo

🔁 **Token Ring** — coordenação distribuída sem líder fixo\
⛓️ **Blockchain com PoW** — registro imutável e auditável de missões\
💰 **Créditos** — modelo econômico integrado, débito pós-confirmação\
🔐 **Ed25519** — autenticidade de laudos garantida por criptografia assimétrica\
⚡ **TCP puro** — sem dependência de middleware externo\
🛡️ **Tolerância a falhas** em todos os níveis (sensor, broker, drone, ledger)\
📊 **Log histórico** com ciclo de vida completo e rastreável\
🖥️ **Cliente interativo** — auditoria e requisição em tempo real\
🐳 **Docker multi-stage** — imagem de runtime mínima (Alpine)
