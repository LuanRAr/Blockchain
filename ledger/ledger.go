// ledger.go — Nó de Ledger Distribuído para o Sistema Ormuz
//
// Implementa uma blockchain simplificada sem frameworks externos:
//   - Blocos encadeados por hash SHA-256 (prova de integridade)
//   - Prova de Trabalho (PoW) com dificuldade configurável
//   - Consenso por maioria simples entre os nós do consórcio
//   - Gestão de créditos (saldo por empresa) com proteção contra duplo gasto
//   - Log imutável de laudos de missão
//   - Comunicação exclusivamente via TCP + JSON (sem frameworks)
//
// Variáveis de ambiente:
//   NODE_ID    — nome/ID deste nó  (ex: "ledger-norte")
//   PORT       — porta TCP de escuta (padrão: 7000)
//   PEERS      — lista de outros nós "IP:porta,IP:porta,..."
//   GENESIS    — se "true", este nó cria o bloco gênese e distribui créditos iniciais
//   DIFFICULTY — número de zeros iniciais para PoW (padrão: 3)

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// Tipos de transação suportados pelo ledger
// ─────────────────────────────────────────────────────────────────────────────

type TxType string

const (
	// TxCredit representa emissão inicial de créditos para uma empresa (só no gênese ou por consenso)
	TxCredit TxType = "CREDIT_ISSUE"

	// TxTransfer representa pagamento de créditos de uma empresa para outra (ou para o "sistema")
	TxTransfer TxType = "CREDIT_TRANSFER"

	// TxMission é o laudo imutável de uma missão de drone concluída
	TxMission TxType = "MISSION_REPORT"
)

// ─────────────────────────────────────────────────────────────────────────────
// Estrutura de uma transação
// ─────────────────────────────────────────────────────────────────────────────

type Transaction struct {
	TxID      string    `json:"tx_id"`      // UUID da transação (gerado pelo solicitante)
	Type      TxType    `json:"type"`       // tipo da transação
	From      string    `json:"from"`       // remetente (empresa ou "SYSTEM")
	To        string    `json:"to"`         // destinatário (empresa ou "SYSTEM")
	Amount    int       `json:"amount"`     // créditos transferidos (0 para TxMission)
	Payload   string    `json:"payload"`    // JSON livre: laudo da missão, motivo, etc.
	Timestamp time.Time `json:"timestamp"`
}

// ─────────────────────────────────────────────────────────────────────────────
// Estrutura de um bloco da blockchain
// ─────────────────────────────────────────────────────────────────────────────

type Block struct {
	Index     uint64        `json:"index"`      // posição na cadeia (0 = gênese)
	PrevHash  string        `json:"prev_hash"`  // hash do bloco anterior
	Timestamp time.Time     `json:"timestamp"`
	Txs       []Transaction `json:"txs"`        // transações incluídas neste bloco
	Nonce     uint64        `json:"nonce"`      // valor ajustado para satisfazer PoW
	Hash      string        `json:"hash"`       // SHA-256 deste bloco (calculado após PoW)
	MinedBy   string        `json:"mined_by"`   // ID do nó que minerou o bloco
}

// ─────────────────────────────────────────────────────────────────────────────
// Mensagens trocadas entre nós via TCP
// ─────────────────────────────────────────────────────────────────────────────

type MsgType string

const (
	// Enviado por brokers ou clientes externos: pede inclusão de tx na mempool
	MsgSubmitTx MsgType = "SUBMIT_TX"

	// Nó anuncia um bloco recém-minerado para todos os peers
	MsgNewBlock MsgType = "NEW_BLOCK"

	// Pedido de sincronização da cadeia completa
	MsgSyncReq MsgType = "SYNC_REQ"

	// Resposta com a cadeia completa
	MsgSyncResp MsgType = "SYNC_RESP"

	// Ping entre nós para manter mapa de liveness
	MsgPing MsgType = "PING"

	// Consulta de saldo de uma empresa
	MsgBalanceQuery MsgType = "BALANCE_QUERY"

	// Resposta de saldo
	MsgBalanceResp MsgType = "BALANCE_RESP"
)

type Envelope struct {
	Type    MsgType         `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// ─────────────────────────────────────────────────────────────────────────────
// Estado do nó
// ─────────────────────────────────────────────────────────────────────────────

type LedgerNode struct {
	ID         string   // identificador único deste nó
	Port       string   // porta TCP de escuta
	Peers      []string // endereços dos outros nós
	Difficulty int      // número de zeros hex exigidos pelo PoW

	mu         sync.RWMutex
	chain      []Block            // cadeia local de blocos
	mempool    []Transaction      // transações ainda não confirmadas
	balances   map[string]int     // saldo atual por empresa
	seenTxs    map[string]bool    // IDs de tx já processados (evita duplicatas)
	seenBlocks map[string]bool    // hashes de blocos já processados
	peerAlive  map[string]bool    // liveness dos peers

	mineSignal chan struct{} // acorda o minerador quando há novas txs
}

// NewLedgerNode cria e inicializa um nó de ledger.
func NewLedgerNode(id, port string, peers []string, difficulty int) *LedgerNode {
	n := &LedgerNode{
		ID:         id,
		Port:       port,
		Peers:      peers,
		Difficulty: difficulty,
		balances:   make(map[string]int),
		seenTxs:    make(map[string]bool),
		seenBlocks: make(map[string]bool),
		peerAlive:  make(map[string]bool),
		mineSignal: make(chan struct{}, 16),
	}
	for _, p := range peers {
		n.peerAlive[p] = false // considera offline até o ping confirmar
	}
	return n
}

// ─────────────────────────────────────────────────────────────────────────────
// Hashing e Prova de Trabalho
// ─────────────────────────────────────────────────────────────────────────────

// blockContent serializa os campos imutáveis do bloco para cálculo de hash.
// O campo Hash em si é excluído para evitar circularidade.
func blockContent(b Block) string {
	txsBytes, _ := json.Marshal(b.Txs)
	return fmt.Sprintf("%d|%s|%s|%s|%d",
		b.Index,
		b.PrevHash,
		b.Timestamp.Format(time.RFC3339Nano),
		string(txsBytes),
		b.Nonce,
	)
}

// calcHash retorna o SHA-256 hexadecimal do conteúdo de um bloco.
func calcHash(b Block) string {
	sum := sha256.Sum256([]byte(blockContent(b)))
	return hex.EncodeToString(sum[:])
}

// difficultyPrefix retorna o prefixo de zeros exigido pelo PoW.
func difficultyPrefix(d int) string {
	return strings.Repeat("0", d)
}

// mineBlock executa a prova de trabalho: incrementa Nonce até o hash satisfazer
// a dificuldade configurada. Retorna o bloco com Nonce e Hash definidos.
func mineBlock(b Block, difficulty int) Block {
	prefix := difficultyPrefix(difficulty)
	for {
		b.Hash = calcHash(b)
		if strings.HasPrefix(b.Hash, prefix) {
			return b
		}
		b.Nonce++
	}
}

// validateBlock verifica integridade e PoW de um bloco contra seu predecessor.
func validateBlock(b, prev Block, difficulty int) bool {
	// (a) encadeamento correto
	if b.PrevHash != prev.Hash {
		return false
	}
	// (b) índice sequencial
	if b.Index != prev.Index+1 {
		return false
	}
	// (c) hash coerente com o conteúdo
	if b.Hash != calcHash(b) {
		return false
	}
	// (d) satisfaz prova de trabalho
	if !strings.HasPrefix(b.Hash, difficultyPrefix(difficulty)) {
		return false
	}
	return true
}

// ─────────────────────────────────────────────────────────────────────────────
// Gênese
// ─────────────────────────────────────────────────────────────────────────────

// createGenesisBlock cria o bloco 0 da cadeia, emitindo créditos iniciais para
// as empresas de navegação do consórcio. O bloco gênese tem PrevHash = "0"*64.
func createGenesisBlock(difficulty int) Block {
	// Empresas iniciais do consórcio com saldo de arranque
	initialBalances := map[string]int{
		"NavegacaoNorte":  1000,
		"NavegacaoSul":    1000,
		"NavegacaoLeste":  1000,
		"NavegacaoOeste":  1000,
		"ConsorcioGeral":  5000,
	}

	// Data de criação determinística do bloco gênese
	genesisTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Ordenação determinística das transações gênese para manter consistência de hash
	companies := []string{
		"ConsorcioGeral",
		"NavegacaoLeste",
		"NavegacaoNorte",
		"NavegacaoOeste",
		"NavegacaoSul",
	}

	var txs []Transaction
	for _, company := range companies {
		amount := initialBalances[company]
		txs = append(txs, Transaction{
			TxID:      fmt.Sprintf("genesis-%s", company),
			Type:      TxCredit,
			From:      "SYSTEM",
			To:        company,
			Amount:    amount,
			Payload:   `{"reason":"distribuição_inicial"}`,
			Timestamp: genesisTime,
		})
	}

	genesis := Block{
		Index:     0,
		PrevHash:  strings.Repeat("0", 64), // convencional para o bloco gênese
		Timestamp: genesisTime,
		Txs:       txs,
		MinedBy:   "SYSTEM",
	}
	// O bloco gênese também passa por PoW para manter consistência de validação
	return mineBlock(genesis, difficulty)
}

// ─────────────────────────────────────────────────────────────────────────────
// Gestão de balances (aplicada ao receber um novo bloco confirmado)
// ─────────────────────────────────────────────────────────────────────────────

// applyBlock atualiza o mapa de saldos com as transações do bloco.
// Chamada APENAS após validação bem-sucedida para manter consistência.
func (n *LedgerNode) applyBlock(b Block) {
	for _, tx := range b.Txs {
		switch tx.Type {
		case TxCredit:
			// Emissão: credita destinatário
			n.balances[tx.To] += tx.Amount

		case TxTransfer:
			// Transferência: debita remetente, credita destinatário
			n.balances[tx.From] -= tx.Amount
			n.balances[tx.To] += tx.Amount

		case TxMission:
			// Laudo: não altera saldo, apenas registra o fato
		}
		// Marca tx como vista para nunca reprocessar
		n.seenTxs[tx.TxID] = true
	}
	// Marca o bloco como visto
	n.seenBlocks[b.Hash] = true
}

// ─────────────────────────────────────────────────────────────────────────────
// Validação de transação antes de entrar na mempool
// ─────────────────────────────────────────────────────────────────────────────

// validateTx verifica se a transação pode ser aceita no estado atual:
//   - Não duplicada
//   - Saldo suficiente para transferências (proteção contra duplo gasto)
func (n *LedgerNode) validateTx(tx Transaction) error {
	// (1) Unicidade: se já processamos este txID, rejeitamos
	if n.seenTxs[tx.TxID] {
		return fmt.Errorf("transação duplicada: %s", tx.TxID)
	}
	// (2) Também verifica a mempool atual para evitar duplo gasto na mesma rodada
	pendingSpend := 0
	for _, pending := range n.mempool {
		if pending.From == tx.From && pending.Type == TxTransfer {
			pendingSpend += pending.Amount
		}
	}

	// (3) Saldo suficiente para transferências
	if tx.Type == TxTransfer {
		available := n.balances[tx.From] - pendingSpend
		if available < tx.Amount {
			return fmt.Errorf("saldo insuficiente: %s tem %d disponível, solicitou %d",
				tx.From, available, tx.Amount)
		}
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Mineração contínua: empacota mempool em blocos e propaga
// ─────────────────────────────────────────────────────────────────────────────

// minerLoop fica em espera por sinal de novas txs e, quando há trabalho,
// monta e minera um bloco novo, depois o propaga para os peers.
func (n *LedgerNode) minerLoop() {
	for {
		// Aguarda sinal de nova transação ou timer de 10s (para não travar indefinidamente)
		select {
		case <-n.mineSignal:
			// Pequeno delay de coalescência para acumular transações que chegam em rajada
			time.Sleep(500 * time.Millisecond)
		case <-time.After(10 * time.Second):
		}

		n.mu.Lock()
		if len(n.mempool) == 0 {
			n.mu.Unlock()
			continue
		}

		// Drena o canal de sinais acumulados
		for {
			select {
			case <-n.mineSignal:
			default:
				goto drained
			}
		}
	drained:

		// Captura snapshot da mempool atual
		txsToMine := make([]Transaction, len(n.mempool))
		copy(txsToMine, n.mempool)
		n.mempool = nil

		// Cabeça da cadeia atual
		prev := n.chain[len(n.chain)-1]
		n.mu.Unlock()

		// Monta o bloco candidato (sem Nonce/Hash ainda)
		candidate := Block{
			Index:     prev.Index + 1,
			PrevHash:  prev.Hash,
			Timestamp: time.Now(),
			Txs:       txsToMine,
			MinedBy:   n.ID,
		}

		fmt.Printf("[%s] ⛏  minerando bloco %d (%d txs, dificuldade=%d)…\n",
			n.ID, candidate.Index, len(txsToMine), n.Difficulty)
		start := time.Now()
		mined := mineBlock(candidate, n.Difficulty)
		fmt.Printf("[%s] ✅ bloco %d minerado | hash=%s… | nonce=%d | tempo=%v\n",
			n.ID, mined.Index, mined.Hash[:12], mined.Nonce, time.Since(start).Round(time.Millisecond))

		// Adiciona à cadeia local e aplica saldos
		n.mu.Lock()
		// Verifica novamente se ninguém inseriu um bloco conflitante enquanto minerávamos
		currentHead := n.chain[len(n.chain)-1]
		if currentHead.Hash != prev.Hash {
			// Cadeia avançou enquanto minerávamos: descarta e reinsere txs na mempool
			fmt.Printf("[%s] cadeia avançou durante mineração — descartando bloco e recolocando %d txs\n",
				n.ID, len(txsToMine))
			n.mempool = append(txsToMine, n.mempool...)
			n.mu.Unlock()
			continue
		}
		n.chain = append(n.chain, mined)
		n.applyBlock(mined)
		n.printChainState()
		n.mu.Unlock()

		// Propaga o bloco para todos os peers vivos
		go n.broadcastBlock(mined)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Recepção de bloco vindo de peer
// ─────────────────────────────────────────────────────────────────────────────

// receiveBlock processa um bloco anunciado por um peer:
//   - Se já conhecido: ignora
//   - Se válido e encadeado: adiciona
//   - Se há gap (peer está adiantado): solicita sincronização completa
func (n *LedgerNode) receiveBlock(b Block, senderAddr string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Já vimos este bloco?
	if n.seenBlocks[b.Hash] {
		return
	}

	head := n.chain[len(n.chain)-1]

	// Caso 1: bloco consecutivo válido
	if b.Index == head.Index+1 {
		if !validateBlock(b, head, n.Difficulty) {
			fmt.Printf("[%s] bloco %d INVÁLIDO recebido de %s — descartado\n",
				n.ID, b.Index, senderAddr)
			return
		}
		// Remove da mempool txs que já estão neste bloco
		n.removeMempoolTxs(b.Txs)
		n.chain = append(n.chain, b)
		n.applyBlock(b)
		fmt.Printf("[%s] 📥 bloco %d aceito de %s | hash=%s…\n",
			n.ID, b.Index, senderAddr, b.Hash[:12])
		n.printChainState()
		return
	}

	// Caso 2: bloco atrasado (já temos esse índice ou mais) — ignora
	if b.Index <= head.Index {
		return
	}

	// Caso 3: existe gap — nossa cadeia está atrás; solicitar sincronização
	fmt.Printf("[%s] gap detectado (local=%d, peer=%d) — solicitando sincronização de %s\n",
		n.ID, head.Index, b.Index, senderAddr)
	go n.requestSync(senderAddr)
}

// removeMempoolTxs elimina da mempool transações já confirmadas num bloco.
func (n *LedgerNode) removeMempoolTxs(confirmed []Transaction) {
	seen := make(map[string]bool, len(confirmed))
	for _, tx := range confirmed {
		seen[tx.TxID] = true
	}
	filtered := n.mempool[:0]
	for _, tx := range n.mempool {
		if !seen[tx.TxID] {
			filtered = append(filtered, tx)
		}
	}
	n.mempool = filtered
}

// ─────────────────────────────────────────────────────────────────────────────
// Sincronização de cadeia (pull da cadeia completa de um peer)
// ─────────────────────────────────────────────────────────────────────────────

func (n *LedgerNode) requestSync(peerAddr string) {
	conn, err := net.DialTimeout("tcp", peerAddr, 5*time.Second)
	if err != nil {
		fmt.Printf("[%s] sync: falha ao conectar em %s: %v\n", n.ID, peerAddr, err)
		return
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(15 * time.Second))

	req := Envelope{Type: MsgSyncReq}
	reqBytes, _ := json.Marshal(map[string]string{"from": n.ID})
	req.Payload = reqBytes

	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return
	}

	var resp Envelope
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return
	}
	if resp.Type != MsgSyncResp {
		return
	}

	var chain []Block
	if err := json.Unmarshal(resp.Payload, &chain); err != nil {
		return
	}

	n.replaceChainIfLonger(chain, peerAddr)
}

// replaceChainIfLonger substitui a cadeia local se a recebida for mais longa
// e completamente válida — regra "longest valid chain wins".
func (n *LedgerNode) replaceChainIfLonger(incoming []Block, from string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Considera a cadeia local um placeholder quando ela tem apenas 1 bloco
	// e o hash desse bloco é todo zeros (situação inicial dos nós secundários).
	localIsPlaceholder := len(n.chain) == 1 &&
		n.chain[0].Hash == strings.Repeat("0", 64)

	if !localIsPlaceholder && len(incoming) <= len(n.chain) {
		return // não é mais longa e não é placeholder: ignora
	}

	if len(incoming) == 0 {
		return
	}

	// Valida se a raiz da cadeia recebida é idêntica à raiz local (evita bifurcações/Split-Brain de origens diferentes)
	if !localIsPlaceholder && incoming[0].Hash != n.chain[0].Hash {
		fmt.Printf("[%s] sync: cadeia recebida de %s REJEITADA por divergência no bloco gênese (local=%s, recebida=%s)\n",
			n.ID, from, n.chain[0].Hash[:16], incoming[0].Hash[:16])
		return
	}

	// Valida a cadeia recebida completa do bloco 1 em diante
	for i := 1; i < len(incoming); i++ {
		if !validateBlock(incoming[i], incoming[i-1], n.Difficulty) {
			fmt.Printf("[%s] sync: cadeia inválida recebida de %s no bloco %d — rejeitada\n",
				n.ID, from, i)
			return
		}
	}

	fmt.Printf("[%s] sync: substituindo cadeia local (%d blocos) pela de %s (%d blocos)\n",
		n.ID, len(n.chain), from, len(incoming))

	// Reconstrói saldos do zero a partir da nova cadeia
	n.chain = incoming
	n.balances = make(map[string]int)
	n.seenTxs = make(map[string]bool)
	n.seenBlocks = make(map[string]bool)
	for _, b := range incoming {
		n.applyBlock(b)
	}
	n.printChainState()
}

// ─────────────────────────────────────────────────────────────────────────────
// Broadcast de bloco para todos os peers
// ─────────────────────────────────────────────────────────────────────────────

func (n *LedgerNode) broadcastBlock(b Block) {
	payload, _ := json.Marshal(b)
	env := Envelope{Type: MsgNewBlock, Payload: payload}

	n.mu.RLock()
	peers := make([]string, len(n.Peers))
	copy(peers, n.Peers)
	n.mu.RUnlock()

	for _, addr := range peers {
		n.mu.RLock()
		alive := n.peerAlive[addr]
		n.mu.RUnlock()
		if !alive {
			continue
		}
		go func(a string) {
			conn, err := net.DialTimeout("tcp", a, 3*time.Second)
			if err != nil {
				return
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(5 * time.Second))
			json.NewEncoder(conn).Encode(env)
		}(addr)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Ping periódico para manter mapa de liveness
// ─────────────────────────────────────────────────────────────────────────────

func (n *LedgerNode) peerHeartbeats() {
	ticker := time.NewTicker(8 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		for _, addr := range n.Peers {
			conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
			n.mu.Lock()
			wasAlive := n.peerAlive[addr]
			if err != nil {
				if wasAlive {
					fmt.Printf("[%s] peer %s OFFLINE\n", n.ID, addr)
				}
				n.peerAlive[addr] = false
				n.mu.Unlock()
				continue
			}
			// Envia ping
			pingPayload, _ := json.Marshal(map[string]string{"from": n.ID})
			env := Envelope{Type: MsgPing, Payload: pingPayload}
			conn.SetDeadline(time.Now().Add(4 * time.Second))
			json.NewEncoder(conn).Encode(env)
			conn.Close()
			if !wasAlive {
				fmt.Printf("[%s] peer %s voltou ao ar — solicitando sync\n", n.ID, addr)
				go n.requestSync(addr)
			}
			n.peerAlive[addr] = true
			n.mu.Unlock()
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Servidor TCP: recebe conexões de brokers e de outros nós
// ─────────────────────────────────────────────────────────────────────────────

func (n *LedgerNode) serve() {
	ln, err := net.Listen("tcp", ":"+n.Port)
	if err != nil {
		fmt.Printf("[%s] erro ao abrir porta %s: %v\n", n.ID, n.Port, err)
		os.Exit(1)
	}
	fmt.Printf("[%s] 🔗 escutando em :%s\n", n.ID, n.Port)
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go n.handleConn(conn)
	}
}

func (n *LedgerNode) handleConn(conn net.Conn) {
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(15 * time.Second))

	var env Envelope
	if err := json.NewDecoder(conn).Decode(&env); err != nil {
		return
	}

	switch env.Type {

	// ── Broker submete transação ──────────────────────────────────────────────
	case MsgSubmitTx:
		var tx Transaction
		if err := json.Unmarshal(env.Payload, &tx); err != nil {
			return
		}
		n.mu.Lock()
		err := n.validateTx(tx)
		if err != nil {
			n.mu.Unlock()
			fmt.Printf("[%s] ❌ tx rejeitada [%s]: %v\n", n.ID, tx.TxID, err)
			// Responde com erro para o broker saber
			resp := map[string]string{"status": "REJECTED", "reason": err.Error()}
			respBytes, _ := json.Marshal(resp)
			conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			json.NewEncoder(conn).Encode(Envelope{Type: "TX_RESULT", Payload: respBytes})
			return
		}
		// Marca como visto imediatamente para evitar race de duplo gasto
		n.seenTxs[tx.TxID] = true
		n.mempool = append(n.mempool, tx)
		fmt.Printf("[%s] 📨 tx aceita na mempool | id=%s tipo=%s de=%s para=%s valor=%d\n",
			n.ID, tx.TxID, tx.Type, tx.From, tx.To, tx.Amount)
		n.mu.Unlock()

		// Responde confirmação de recebimento
		resp := map[string]string{"status": "ACCEPTED"}
		respBytes, _ := json.Marshal(resp)
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		json.NewEncoder(conn).Encode(Envelope{Type: "TX_RESULT", Payload: respBytes})

		// Acorda o minerador
		select {
		case n.mineSignal <- struct{}{}:
		default:
		}

	// ── Peer anuncia novo bloco ───────────────────────────────────────────────
	case MsgNewBlock:
		var b Block
		if err := json.Unmarshal(env.Payload, &b); err != nil {
			return
		}
		senderAddr := conn.RemoteAddr().String()
		// Remove a porta efêmera para reconstruir o endereço do peer como configurado
		peerAddr := resolveListenAddr(senderAddr, n.Peers)
		n.receiveBlock(b, peerAddr)

	// ── Pedido de sincronização ───────────────────────────────────────────────
	case MsgSyncReq:
		n.mu.RLock()
		chainCopy := make([]Block, len(n.chain))
		copy(chainCopy, n.chain)
		n.mu.RUnlock()

		payload, _ := json.Marshal(chainCopy)
		resp := Envelope{Type: MsgSyncResp, Payload: payload}
		conn.SetWriteDeadline(time.Now().Add(15 * time.Second))
		json.NewEncoder(conn).Encode(resp)

	// ── Consulta de saldo ─────────────────────────────────────────────────────
	case MsgBalanceQuery:
		var req map[string]string
		if err := json.Unmarshal(env.Payload, &req); err != nil {
			return
		}
		company := req["company"]
		n.mu.RLock()
		balance := n.balances[company]
		n.mu.RUnlock()

		respData := map[string]interface{}{
			"company": company,
			"balance": balance,
		}
		payload, _ := json.Marshal(respData)
		resp := Envelope{Type: MsgBalanceResp, Payload: payload}
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		json.NewEncoder(conn).Encode(resp)

	// ── Ping de peer ──────────────────────────────────────────────────────────
	case MsgPing:
		var p map[string]string
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return
		}
		// Atualiza liveness baseado no IP de origem
		senderAddr := conn.RemoteAddr().String()
		peerAddr := resolveListenAddr(senderAddr, n.Peers)
		if peerAddr != "" {
			n.mu.Lock()
			n.peerAlive[peerAddr] = true
			n.mu.Unlock()
		}
	}
}

// resolveListenAddr tenta mapear o endereço de conexão de origem (IP:portaEfêmera)
// para o endereço de escuta configurado (IP:portaFixa) do peer.
func resolveListenAddr(remoteAddr string, peers []string) string {
	// Extrai só o IP da conexão de origem
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return ""
	}
	for _, peer := range peers {
		peerHost, _, err := net.SplitHostPort(peer)
		if err != nil {
			continue
		}
		if peerHost == host {
			return peer
		}
	}
	return ""
}

// ─────────────────────────────────────────────────────────────────────────────
// Exibição de estado
// ─────────────────────────────────────────────────────────────────────────────

func (n *LedgerNode) printChainState() {
	// Chamado com n.mu já mantido pelo chamador
	head := n.chain[len(n.chain)-1]

	// Verifica se alguma transação no bloco altera saldos (Amount > 0)
	hasBalanceChange := false
	for _, tx := range head.Txs {
		if tx.Amount > 0 {
			hasBalanceChange = true
			break
		}
	}

	if !hasBalanceChange && head.Index > 0 {
		// Bloco sem alteração de saldo: imprime apenas um resumo em uma linha
		fmt.Printf("[%s] 📦 Bloco %d confirmado | hash=%s… | %d txs (sem alteração de saldo)\n",
			n.ID, head.Index, head.Hash[:16], len(head.Txs))
		return
	}

	fmt.Printf("\n[%s] ╔══════════════ LEDGER STATE (bloco=%d) ══════════════╗\n",
		n.ID, head.Index)
	fmt.Printf("[%s] ║  HEAD  hash=%s…  txs=%d\n",
		n.ID, head.Hash[:16], len(head.Txs))
	fmt.Printf("[%s] ║  SALDOS:\n", n.ID)
	for company, bal := range n.balances {
		fmt.Printf("[%s] ║    %-25s  %d créditos\n", n.ID, company, bal)
	}
	fmt.Printf("[%s] ╚══════════════════════════════════════════════════════╝\n\n", n.ID)
}

// ─────────────────────────────────────────────────────────────────────────────
// Geração de ID de transação único
// ─────────────────────────────────────────────────────────────────────────────

var rng = rand.New(rand.NewSource(time.Now().UnixNano()))

func newTxID() string {
	b := make([]byte, 8)
	rng.Read(b)
	return hex.EncodeToString(b)
}

// ─────────────────────────────────────────────────────────────────────────────
// main
// ─────────────────────────────────────────────────────────────────────────────

func main() {
	nodeID     := os.Getenv("NODE_ID")
	port       := os.Getenv("PORT")
	peersRaw   := os.Getenv("PEERS")
	isGenesis  := os.Getenv("GENESIS") == "true"
	difficulty := 3 // padrão: 3 zeros hexadecimais (ajustável por env)

	if nodeID == "" { nodeID = fmt.Sprintf("ledger-%04d", rng.Intn(10000)) }
	if port == ""   { port = "7000" }

	var peers []string
	for _, p := range strings.Split(peersRaw, ",") {
		if p = strings.TrimSpace(p); p != "" {
			peers = append(peers, p)
		}
	}

	node := NewLedgerNode(nodeID, port, peers, difficulty)

	if isGenesis {
		// Este nó cria a cadeia do zero com o bloco gênese
		fmt.Printf("[%s] 🌱 gerando bloco gênese (dificuldade=%d)…\n", nodeID, difficulty)
		genesis := createGenesisBlock(difficulty)
		node.mu.Lock()
		node.chain = []Block{genesis}
		node.applyBlock(genesis)
		node.mu.Unlock()
		fmt.Printf("[%s] ✅ gênese criado | hash=%s… | nonce=%d\n",
			nodeID, genesis.Hash[:16], genesis.Nonce)
		node.printChainState()
	} else {
		// Nó secundário: inicializa com cadeia vazia e vai sincronizar
		// Bloco placeholder para que o índice 0 exista antes da sincronização
		placeholder := Block{
			Index:    0,
			PrevHash: strings.Repeat("0", 64),
			Hash:     strings.Repeat("0", 64),
		}
		node.mu.Lock()
		node.chain = []Block{placeholder}
		node.mu.Unlock()
	}

	// Inicia goroutines de suporte
	go node.peerHeartbeats()
	go node.minerLoop()

	// Tenta sincronizar com peers após startup para herdar uma cadeia existente mais longa (independente de ser gênese ou não)
	if len(peers) > 0 {
		go func() {
			time.Sleep(3 * time.Second) // aguarda peers subirem
			for _, peer := range peers {
				fmt.Printf("[%s] sincronizando cadeia de %s…\n", nodeID, peer)
				node.requestSync(peer)
				node.mu.RLock()
				chainLen := len(node.chain)
				node.mu.RUnlock()
				if chainLen > 1 {
					break // cadeia obtida com sucesso
				}
			}
		}()
	}

	// Loop TCP principal
	node.serve()
}