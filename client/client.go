package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Tipos espelhados do ledger 
type TxType string

const (
	TxCredit   TxType = "CREDIT_ISSUE"
	TxTransfer TxType = "CREDIT_TRANSFER"
	TxMission  TxType = "MISSION_REPORT"
)

type Transaction struct {
	TxID      string    `json:"tx_id"`
	Type      TxType    `json:"type"`
	From      string    `json:"from"`
	To        string    `json:"to"`
	Amount    int       `json:"amount"`
	Payload   string    `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
}

type Block struct {
	Index     uint64        `json:"index"`
	PrevHash  string        `json:"prev_hash"`
	Timestamp time.Time     `json:"timestamp"`
	Txs       []Transaction `json:"txs"`
	Nonce     uint64        `json:"nonce"`
	Hash      string        `json:"hash"`
	MinedBy   string        `json:"mined_by"`
}

type LedgerMsgType string

const (
	MsgBalanceQuery LedgerMsgType = "BALANCE_QUERY"
	MsgBalanceResp  LedgerMsgType = "BALANCE_RESP"
	MsgSyncReq      LedgerMsgType = "SYNC_REQ"
	MsgSyncResp     LedgerMsgType = "SYNC_RESP"
)

type LedgerEnvelope struct {
	Type    LedgerMsgType   `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// Tipos espelhados do broker (para envio de alerta)

type BrokerMsgType string

const MsgAlert BrokerMsgType = "ALERT"

type BrokerMessage struct {
	Type    BrokerMsgType `json:"type"`
	Payload interface{}   `json:"payload"`
}

type AlertPayload struct {
	AlertID   string    `json:"alert_id"`
	SensorID  string    `json:"sensor_id"`
	Sector    string    `json:"sector"`
	AlertType string    `json:"alert_type"`
	Value     float64   `json:"value"`
	Timestamp time.Time `json:"timestamp"`
	Priority  int       `json:"priority"`
}

// Estrutura do payload de laudo gravado pelo broker no ledger
type LaudoPayload struct {
	MissionID   string `json:"mission_id"`
	Company     string `json:"company"`
	DroneID     string `json:"drone_id"`
	Result      string `json:"result"`
	LaudoHash   string `json:"laudo_hash"`
	Broker      string `json:"broker"`
	CompletedAt string `json:"completed_at"`
}

// ─────────────────────────────────────────────────────────────────────────────
// Cliente
// ─────────────────────────────────────────────────────────────────────────────

type Client struct {
	Company     string
	LedgerNodes []string
	BrokerList  []string
	rng         *rand.Rand
}

func NewClient(company string, ledgers, brokers []string) *Client {
	return &Client{
		Company:     company,
		LedgerNodes: ledgers,
		BrokerList:  brokers,
		rng:         rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// dialLedger conecta ao primeiro nó de ledger disponível (com fallover).
func (c *Client) dialLedger() (net.Conn, error) {
	for _, addr := range c.LedgerNodes {
		conn, err := net.DialTimeout("tcp", addr, 4*time.Second)
		if err == nil {
			return conn, nil
		}
	}
	return nil, fmt.Errorf("nenhum nó de ledger acessível em %v", c.LedgerNodes)
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. Consultar saldo
// ─────────────────────────────────────────────────────────────────────────────

func (c *Client) queryBalance(company string) (int, error) {
	conn, err := c.dialLedger()
	if err != nil {
		return -1, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(6 * time.Second))

	payload, _ := json.Marshal(map[string]string{"company": company})
	env := LedgerEnvelope{Type: MsgBalanceQuery, Payload: payload}
	if err := json.NewEncoder(conn).Encode(env); err != nil {
		return -1, err
	}

	var resp LedgerEnvelope
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return -1, err
	}

	var data map[string]interface{}
	if err := json.Unmarshal(resp.Payload, &data); err != nil {
		return -1, err
	}
	bal, _ := data["balance"].(float64)
	return int(bal), nil
}

// 2. Baixar a blockchain completa

func (c *Client) fetchChain() ([]Block, error) {
	conn, err := c.dialLedger()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(30 * time.Second))

	payload, _ := json.Marshal(map[string]string{"from": "client-" + c.Company})
	env := LedgerEnvelope{Type: MsgSyncReq, Payload: payload}
	if err := json.NewEncoder(conn).Encode(env); err != nil {
		return nil, err
	}

	var resp LedgerEnvelope
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return nil, err
	}

	var chain []Block
	if err := json.Unmarshal(resp.Payload, &chain); err != nil {
		return nil, err
	}
	return chain, nil
}

//Requisitar escolta (envia ALERT ao broker)
func (c *Client) requestEscort(sector, alertType string, priority int) error {
	company     := "Navegacao" + sector
	missionCost := priority * 10

	// Verifica saldo antes de enviar
	bal, err := c.queryBalance(company)
	if err != nil {
		return fmt.Errorf("falha ao verificar saldo de %s: %v", company, err)
	}
	if bal < missionCost {
		return fmt.Errorf(
			"SALDO INSUFICIENTE: %s tem %d créditos, missão custa %d créditos",
			company, bal, missionCost)
	}

	alertID := fmt.Sprintf("%02d", c.rng.Intn(100))
	alert := AlertPayload{
		AlertID:   alertID,
		SensorID:  "CLIENT-" + c.Company,
		Sector:    sector,
		AlertType: alertType,
		Value:     float64(70 + priority*10),
		Timestamp: time.Now(),
		Priority:  priority,
	}

	msg := BrokerMessage{Type: MsgAlert, Payload: alert}

	for _, addr := range c.BrokerList {
		conn, err := net.DialTimeout("tcp", addr, 4*time.Second)
		if err != nil {
			continue
		}
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		encErr := json.NewEncoder(conn).Encode(msg)
		conn.Close()
		if encErr == nil {
			fmt.Printf("\n  ✅ Alerta de escolta enviado com sucesso!\n")
			fmt.Printf("     ID do alerta : %s\n", alertID)
			fmt.Printf("     Setor        : %s\n", sector)
			fmt.Printf("     Tipo         : %s\n", alertType)
			fmt.Printf("     Prioridade   : %d\n", priority)
			fmt.Printf("     Custo máximo : %d créditos\n", missionCost)
			fmt.Printf("     Empresa      : %s (saldo atual: %d)\n", company, bal)
			fmt.Printf("     Broker       : %s\n", addr)
			return nil
		}
	}
	return fmt.Errorf("nenhum broker acessível em %v", c.BrokerList)
}

// Menus de auditoria

func (c *Client) menuConsultarSaldos(scanner *bufio.Scanner) {
	fmt.Println("\n  Qual empresa deseja consultar?")
	fmt.Println("  [1] Minha empresa (" + c.Company + ")")
	fmt.Println("  [2] Todas as empresas do consórcio")
	fmt.Println("  [3] Digitar nome da empresa")
	fmt.Print("\n  > ")

	if !scanner.Scan() {
		return
	}
	choice := strings.TrimSpace(scanner.Text())

	switch choice {
	case "1":
		bal, err := c.queryBalance(c.Company)
		if err != nil {
			fmt.Printf("\n  ❌ Erro: %v\n", err)
			return
		}
		fmt.Printf("\n  💰 Saldo de %s: %d créditos\n", c.Company, bal)

	case "2":
		fmt.Println("\n  ⏳ Baixando blockchain para calcular saldos...")
		chain, err := c.fetchChain()
		if err != nil {
			fmt.Printf("\n  ❌ Erro: %v\n", err)
			return
		}
		balances := make(map[string]int)
		for _, blk := range chain {
			for _, tx := range blk.Txs {
				switch tx.Type {
				case TxCredit:
					balances[tx.To] += tx.Amount
				case TxTransfer:
					balances[tx.From] -= tx.Amount
					balances[tx.To] += tx.Amount
				}
			}
		}
		companies := make([]string, 0, len(balances))
		for k := range balances {
			companies = append(companies, k)
		}
		sort.Strings(companies)

		fmt.Printf("\n  ╔══════════════ SALDOS DO CONSÓRCIO (%d blocos) ══════════════╗\n", len(chain))
		for _, comp := range companies {
			marker := ""
			if comp == c.Company {
				marker = "  ◀ você"
			}
			fmt.Printf("  ║  %-28s  %6d créditos%s\n", comp, balances[comp], marker)
		}
		fmt.Println("  ╚════════════════════════════════════════════════════════════╝")

	case "3":
		fmt.Print("  Nome da empresa: ")
		if !scanner.Scan() {
			return
		}
		name := strings.TrimSpace(scanner.Text())
		if name == "" {
			return
		}
		bal, err := c.queryBalance(name)
		if err != nil {
			fmt.Printf("\n  ❌ Erro: %v\n", err)
			return
		}
		fmt.Printf("\n  💰 Saldo de %s: %d créditos\n", name, bal)

	default:
		fmt.Println("  Opção inválida.")
	}
}

func (c *Client) menuVerificarTransacoes(scanner *bufio.Scanner) {
	fmt.Println("\n  Filtrar transações por:")
	fmt.Println("  [1] Minha empresa (" + c.Company + ")")
	fmt.Println("  [2] Outra empresa")
	fmt.Println("  [3] Todas as transações da blockchain")
	fmt.Print("\n  > ")

	if !scanner.Scan() {
		return
	}
	choice := strings.TrimSpace(scanner.Text())

	target := ""
	switch choice {
	case "1":
		target = c.Company
	case "2":
		fmt.Print("  Nome da empresa: ")
		if !scanner.Scan() {
			return
		}
		target = strings.TrimSpace(scanner.Text())
	case "3":
		target = "" // todas
	default:
		fmt.Println("  Opção inválida.")
		return
	}

	fmt.Println("\n  ⏳ Baixando blockchain...")
	chain, err := c.fetchChain()
	if err != nil {
		fmt.Printf("\n  ❌ Erro: %v\n", err)
		return
	}

	label := "TODAS"
	if target != "" {
		label = target
	}
	fmt.Printf("\n  ╔══════════════ TRANSAÇÕES — %s ══════════════╗\n", label)

	count := 0
	for _, blk := range chain {
		for _, tx := range blk.Txs {
			if target != "" && tx.From != target && tx.To != target {
				continue
			}
			count++
			direcao := ""
			if tx.From == c.Company {
				direcao = " [← SAÍDA]"
			} else if tx.To == c.Company {
				direcao = " [→ ENTRADA]"
			}

			txShort := tx.TxID
			if len(txShort) > 12 {
				txShort = txShort[:12] + "…"
			}
			fmt.Printf("  ║  Bloco#%-3d  %s  %-16s  %s → %s  %d créditos%s\n",
				blk.Index,
				tx.Timestamp.Format("02/01 15:04:05"),
				string(tx.Type),
				tx.From, tx.To,
				tx.Amount,
				direcao)

			// Se houver payload de laudo, mostra resumo
			if tx.Type == TxTransfer && tx.Payload != "" && strings.Contains(tx.Payload, "mission_id") {
				var detail map[string]interface{}
				if json.Unmarshal([]byte(tx.Payload), &detail) == nil {
					if mid, ok := detail["mission_id"].(string); ok {
						fmt.Printf("  ║    └─ missão=%s  drone=%v  broker=%v\n",
							mid, detail["drone_id"], detail["broker"])
					}
				}
			}
		}
	}

	if count == 0 {
		fmt.Println("  ║  (nenhuma transação encontrada)")
	}
	fmt.Println("  ╚════════════════════════════════════════════════════════════╝")
	fmt.Printf("  Total: %d transação(ões)\n", count)
}

func (c *Client) menuLerLaudos(scanner *bufio.Scanner) {
	fmt.Println("\n  Filtrar laudos por:")
	fmt.Println("  [1] Minha empresa (" + c.Company + ")")
	fmt.Println("  [2] Todos os laudos")
	fmt.Println("  [3] Outra empresa")
	fmt.Print("\n  > ")

	if !scanner.Scan() {
		return
	}
	choice := strings.TrimSpace(scanner.Text())

	filterCompany := ""
	switch choice {
	case "1":
		filterCompany = c.Company
	case "2":
		filterCompany = ""
	case "3":
		fmt.Print("  Nome da empresa: ")
		if !scanner.Scan() {
			return
		}
		filterCompany = strings.TrimSpace(scanner.Text())
	default:
		fmt.Println("  Opção inválida.")
		return
	}

	fmt.Println("\n  ⏳ Baixando blockchain...")
	chain, err := c.fetchChain()
	if err != nil {
		fmt.Printf("\n  ❌ Erro: %v\n", err)
		return
	}

	label := "TODOS"
	if filterCompany != "" {
		label = filterCompany
	}
	fmt.Printf("\n  ╔══════════════════ LAUDOS DE MISSÃO — %s ══════════════════╗\n", label)

	count := 0
	for _, blk := range chain {
		for _, tx := range blk.Txs {
			// Laudos ficam em transações CREDIT_TRANSFER com payload contendo "mission_id"
			if tx.Type != TxTransfer {
				continue
			}
			if !strings.Contains(tx.Payload, "mission_id") {
				continue
			}

			var laudo LaudoPayload
			if err := json.Unmarshal([]byte(tx.Payload), &laudo); err != nil {
				continue
			}
			if filterCompany != "" && laudo.Company != filterCompany {
				continue
			}

			count++
			hashShort := laudo.LaudoHash
			if len(hashShort) > 16 {
				hashShort = hashShort[:16] + "…"
			}

			completedAt := laudo.CompletedAt
			if t, err := time.Parse(time.RFC3339, completedAt); err == nil {
				completedAt = t.Format("02/01/2006 15:04:05")
			}

			fmt.Printf("  ║\n")
			fmt.Printf("  ║  🚁 Missão: %s  |  Bloco #%d  |  %s\n",
				laudo.MissionID, blk.Index, completedAt)
			fmt.Printf("  ║     Drone    : %s\n", laudo.DroneID)
			fmt.Printf("  ║     Empresa  : %s  |  Custo: %d créditos\n",
				laudo.Company, tx.Amount)
			fmt.Printf("  ║     Broker   : %s\n", laudo.Broker)
			fmt.Printf("  ║     Resultado: %s\n", laudo.Result)
			fmt.Printf("  ║     Hash     : %s  ✅ integridade verificada\n", hashShort)
		}
	}

	if count == 0 {
		fmt.Println("  ║  (nenhum laudo encontrado)")
	}
	fmt.Println("  ║")
	fmt.Println("  ╚════════════════════════════════════════════════════════════════╝")
	fmt.Printf("  Total: %d laudo(s)\n", count)
}

func (c *Client) menuRequisitarEscolta(scanner *bufio.Scanner) {
	fmt.Println()
	fmt.Println("  ┌─── REQUISITAR ESCOLTA DE DRONE ───────────────────────────┐")
	fmt.Println("  │  O custo será debitado da empresa do setor escolhido.      │")
	fmt.Println("  │  Custo = Prioridade × 10 créditos                          │")
	fmt.Println("  └────────────────────────────────────────────────────────────┘")

	// Setor
	fmt.Println("\n  Setor da ocorrência:")
	fmt.Println("  [1] Norte   [2] Sul   [3] Leste   [4] Oeste")
	fmt.Print("\n  > ")
	if !scanner.Scan() {
		return
	}
	sectorMap := map[string]string{"1": "Norte", "2": "Sul", "3": "Leste", "4": "Oeste"}
	sector, ok := sectorMap[strings.TrimSpace(scanner.Text())]
	if !ok {
		fmt.Println("  ❌ Setor inválido.")
		return
	}

	// Tipo de alerta
	fmt.Println("\n  Tipo de ocorrência:")
	fmt.Println("  [1] Radar   [2] Naval")
	fmt.Print("\n  > ")
	if !scanner.Scan() {
		return
	}
	typeMap := map[string]string{"1": "Radar", "2": "Naval"}
	alertType, ok := typeMap[strings.TrimSpace(scanner.Text())]
	if !ok {
		fmt.Println("  ❌ Tipo inválido.")
		return
	}

	// Prioridade
	fmt.Println("\n  Prioridade:")
	fmt.Println("  [1] Médio  (custo: 10 créditos)")
	fmt.Println("  [2] Alto   (custo: 20 créditos)")
	fmt.Println("  [3] Crítico (custo: 30 créditos)")
	fmt.Print("\n  > ")
	if !scanner.Scan() {
		return
	}
	prio, err := strconv.Atoi(strings.TrimSpace(scanner.Text()))
	if err != nil || prio < 1 || prio > 3 {
		fmt.Println("  ❌ Prioridade inválida.")
		return
	}

	company     := "Navegacao" + sector
	missionCost := prio * 10

	fmt.Printf("\n  Confirmar requisição:\n")
	fmt.Printf("  Setor: %s  |  Tipo: %s  |  Prioridade: %d  |  Empresa: %s  |  Custo: %d créditos\n",
		sector, alertType, prio, company, missionCost)
	fmt.Print("\n  Confirmar? [s/n] > ")
	if !scanner.Scan() {
		return
	}
	if strings.ToLower(strings.TrimSpace(scanner.Text())) != "s" {
		fmt.Println("  Operação cancelada.")
		return
	}

	fmt.Println("\n  ⏳ Verificando saldo e enviando alerta...")
	if err := c.requestEscort(sector, alertType, prio); err != nil {
		fmt.Printf("\n  ❌ Falha: %v\n", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Menu principal
// ─────────────────────────────────────────────────────────────────────────────

func printMenu(company string) {
	fmt.Printf(`
  ╔══════════════════════════════════════════════════════════════════╗
  ║         CONSÓRCIO ORMUZ — %s
  ╠══════════════════════════════════════════════════════════════════╣
  ║                                                                  ║
  ║  [1] Consultar saldo de créditos                                 ║
  ║  [2] Requisitar escolta de drone                                 ║
  ║  [3] Ver histórico de transações                                 ║
  ║  [4] Ler laudos de missão                                        ║
  ║  [0] Sair                                                        ║
  ║                                                                  ║
  ╚══════════════════════════════════════════════════════════════════╝
`, fmt.Sprintf("%-38s║", company))
}

func main() {
	company   := os.Getenv("COMPANY")
	ledgerRaw := os.Getenv("LEDGER_NODES")
	brokerRaw := os.Getenv("BROKER_LIST")

	if company == "" {
		company = "NavegacaoNorte"
	}
	if ledgerRaw == "" {
		ledgerRaw = "localhost:7007,localhost:7008,localhost:7011,localhost:7012"
	}
	if brokerRaw == "" {
		brokerRaw = "localhost:5007,localhost:5008,localhost:5011,localhost:5012"
	}

	var ledgers, brokers []string
	for _, s := range strings.Split(ledgerRaw, ",") {
		if s = strings.TrimSpace(s); s != "" {
			ledgers = append(ledgers, s)
		}
	}
	for _, s := range strings.Split(brokerRaw, ",") {
		if s = strings.TrimSpace(s); s != "" {
			brokers = append(brokers, s)
		}
	}

	client  := NewClient(company, ledgers, brokers)
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Printf("\n  🚀 Bem-vindo ao Console Ormuz!\n")
	fmt.Printf("  Empresa : %s\n", company)
	fmt.Printf("  Ledgers : %s\n", strings.Join(ledgers, ", "))
	fmt.Printf("  Brokers : %s\n", strings.Join(brokers, ", "))

	// Exibe saldo ao iniciar
	if bal, err := client.queryBalance(company); err == nil {
		fmt.Printf("  Saldo   : %d créditos\n", bal)
	}

	for {
		printMenu(company)
		fmt.Print("  > ")

		if !scanner.Scan() {
			break
		}
		choice := strings.TrimSpace(scanner.Text())

		switch choice {
		case "0", "sair":
			fmt.Println("\n  Encerrando. Até logo!\n")
			os.Exit(0)

		case "1":
			client.menuConsultarSaldos(scanner)

		case "2":
			client.menuRequisitarEscolta(scanner)

		case "3":
			client.menuVerificarTransacoes(scanner)

		case "4":
			client.menuLerLaudos(scanner)

		default:
			fmt.Printf("\n  ❓ Opção inválida: %q\n", choice)
		}

		fmt.Print("\n  Pressione ENTER para continuar...")
		scanner.Scan()
	}
}
