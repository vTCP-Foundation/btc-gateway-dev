// Package api provides a REST API for interacting with the KV store.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"btc-gateway/internal/service"
	"btc-gateway/internal/statemachine"
	"btc-gateway/pkg/consensus/integration"
	"btc-gateway/pkg/consensus/types"
)

// Handler provides HTTP handlers for the KV store API.
type Handler struct {
	stateMachine        *statemachine.StateMachine
	node                *integration.Node
	config              *types.ConsensusConfig
	nodeID              types.NodeID
	nonceCounter        uint64
	logger              zerolog.Logger
	accountQueryService *service.AccountQueryService
	txSubmitService     *service.TxSubmitService
}

// NewHandler creates a new API handler.
func NewHandler(sm *statemachine.StateMachine, node *integration.Node, config *types.ConsensusConfig, nodeID types.NodeID) *Handler {
	return &Handler{
		stateMachine: sm,
		node:         node,
		config:       config,
		nodeID:       nodeID,
		nonceCounter: uint64(time.Now().UnixNano()),
		logger:       log.With().Str("component", "api").Logger(),
	}
}

// SetAccountQueryService sets the account query service for the handler.
func (h *Handler) SetAccountQueryService(svc *service.AccountQueryService) {
	h.accountQueryService = svc
}

// SetTxSubmitService sets the transaction submission service for the handler.
func (h *Handler) SetTxSubmitService(svc *service.TxSubmitService) {
	h.txSubmitService = svc
}

// SetupRoutes registers all API routes on the given mux.
func (h *Handler) SetupRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /health", h.handleHealth)
	mux.HandleFunc("GET /status", h.handleStatus)
	mux.HandleFunc("GET /kv", h.handleGetAll)
	mux.HandleFunc("GET /kv/{key}", h.handleGet)
	mux.HandleFunc("POST /kv/{key}", h.handleSet)
	mux.HandleFunc("PUT /kv/{key}", h.handleSet)
	mux.HandleFunc("DELETE /kv/{key}", h.handleDelete)
	mux.HandleFunc("POST /kv/{key}/add", h.handleAdd)
	mux.HandleFunc("POST /kv/{key}/sub", h.handleSub)
	mux.HandleFunc("POST /tx", h.handleTransaction)
	mux.HandleFunc("GET /account/{address}", h.handleGetAccount)
}

// Response types
type ErrorResponse struct {
	Error string `json:"error"`
}

type ValueResponse struct {
	Key   string `json:"key"`
	Value int    `json:"value"`
	Found bool   `json:"found"`
}

type SnapshotResponse struct {
	Data map[string]int `json:"data"`
}

type StatusResponse struct {
	NodeID      int    `json:"node_id"`
	CurrentView uint64 `json:"current_view"`
	IsLeader    bool   `json:"is_leader"`
	Connected   bool   `json:"connected"`
}

type TransactionRequest struct {
	Type         string `json:"type"` // set, add, sub, del, set_balance
	Key          string `json:"key"`
	Value        int    `json:"value,omitempty"`
	Sender       string `json:"sender,omitempty"`
	SenderPubKey []byte `json:"sender_pubkey,omitempty"`
	Signature    []byte `json:"signature,omitempty"`
	Nonce        uint64 `json:"nonce,omitempty"`
}

// AccountResponse represents an account in the REST API.
type AccountResponse struct {
	Address string `json:"address"`
	Nonce   uint64 `json:"nonce"`
	Balance uint64 `json:"balance"`
}

type TransactionResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
	TxHash  string `json:"tx_hash,omitempty"`
}

// handleHealth returns a simple health check.
func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleStatus returns node status information.
func (h *Handler) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	coord := h.node.GetCoordinator()
	currentView := coord.GetCurrentView()

	// Check if this node is the leader for current view
	leader, _ := h.config.GetLeaderForView(currentView)
	isLeader := leader == h.nodeID

	resp := StatusResponse{
		NodeID:      int(h.nodeID),
		CurrentView: uint64(currentView),
		IsLeader:    isLeader,
		Connected:   true,
	}
	json.NewEncoder(w).Encode(resp)
}

// handleGetAll returns all key-value pairs.
func (h *Handler) handleGetAll(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	snapshot, err := h.stateMachine.GetSnapshot(ctx)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to get snapshot: "+err.Error())
		return
	}

	json.NewEncoder(w).Encode(SnapshotResponse{Data: snapshot})
}

// handleGet returns a single value.
func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	key := r.PathValue("key")
	if key == "" {
		h.writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	value, found, err := h.stateMachine.Get(ctx, key)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to get value: "+err.Error())
		return
	}

	json.NewEncoder(w).Encode(ValueResponse{
		Key:   key,
		Value: value,
		Found: found,
	})
}

// handleSet sets a key to a specific value.
func (h *Handler) handleSet(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	key := r.PathValue("key")
	if key == "" {
		h.writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	// Parse value from body or query
	var value int
	var err error

	if r.Body != nil && r.ContentLength > 0 {
		var req struct {
			Value int `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			h.writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
			return
		}
		value = req.Value
	} else if v := r.URL.Query().Get("value"); v != "" {
		value, err = strconv.Atoi(v)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, "invalid value: "+err.Error())
			return
		}
	} else {
		h.writeError(w, http.StatusBadRequest, "value is required")
		return
	}

	// Create and submit transaction
	nonce := atomic.AddUint64(&h.nonceCounter, 1)
	tx := statemachine.NewSetTransaction(key, value, nonce)

	if err := h.submitTransaction(r.Context(), tx); err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to submit transaction: "+err.Error())
		return
	}

	json.NewEncoder(w).Encode(TransactionResponse{
		Success: true,
		Message: fmt.Sprintf("SET %s = %d submitted", key, value),
	})
}

// handleAdd adds a value to an existing key.
func (h *Handler) handleAdd(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	key := r.PathValue("key")
	if key == "" {
		h.writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	var req struct {
		Value int `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	nonce := atomic.AddUint64(&h.nonceCounter, 1)
	tx := statemachine.NewAddTransaction(key, req.Value, nonce)

	if err := h.submitTransaction(r.Context(), tx); err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to submit transaction: "+err.Error())
		return
	}

	json.NewEncoder(w).Encode(TransactionResponse{
		Success: true,
		Message: fmt.Sprintf("ADD %s += %d submitted", key, req.Value),
	})
}

// handleSub subtracts a value from an existing key.
func (h *Handler) handleSub(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	key := r.PathValue("key")
	if key == "" {
		h.writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	var req struct {
		Value int `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	nonce := atomic.AddUint64(&h.nonceCounter, 1)
	tx := statemachine.NewSubTransaction(key, req.Value, nonce)

	if err := h.submitTransaction(r.Context(), tx); err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to submit transaction: "+err.Error())
		return
	}

	json.NewEncoder(w).Encode(TransactionResponse{
		Success: true,
		Message: fmt.Sprintf("SUB %s -= %d submitted", key, req.Value),
	})
}

// handleDelete deletes a key.
func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	key := r.PathValue("key")
	if key == "" {
		h.writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	nonce := atomic.AddUint64(&h.nonceCounter, 1)
	tx := statemachine.NewDelTransaction(key, nonce)

	if err := h.submitTransaction(r.Context(), tx); err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to submit transaction: "+err.Error())
		return
	}

	json.NewEncoder(w).Encode(TransactionResponse{
		Success: true,
		Message: fmt.Sprintf("DEL %s submitted", key),
	})
}

// handleTransaction handles a generic transaction request.
func (h *Handler) handleTransaction(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var req TransactionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	if req.Key == "" {
		h.writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	// If sender fields are provided and TxSubmitService is available, use mempool flow
	if req.Sender != "" && len(req.SenderPubKey) > 0 && len(req.Signature) > 0 && h.txSubmitService != nil {
		var tx *statemachine.Transaction

		switch req.Type {
		case "set":
			tx = statemachine.NewSetTransactionWithSender(req.Key, req.Value, req.Nonce, req.Sender, req.SenderPubKey)
		case "add":
			tx = statemachine.NewAddTransactionWithSender(req.Key, req.Value, req.Nonce, req.Sender, req.SenderPubKey)
		case "sub":
			tx = statemachine.NewSubTransactionWithSender(req.Key, req.Value, req.Nonce, req.Sender, req.SenderPubKey)
		case "del":
			tx = statemachine.NewDelTransactionWithSender(req.Key, req.Nonce, req.Sender, req.SenderPubKey)
		case "set_balance":
			tx = statemachine.NewSetBalanceTransactionWithSender(req.Key, req.Value, req.Nonce, req.Sender, req.SenderPubKey)
		default:
			h.writeError(w, http.StatusBadRequest, "invalid transaction type: "+req.Type)
			return
		}

		tx.Signature = req.Signature

		// Submit via TxSubmitService (validates and adds to mempool)
		if err := h.txSubmitService.Submit(r.Context(), tx, service.TxSourceREST); err != nil {
			h.writeError(w, http.StatusBadRequest, "transaction validation failed: "+err.Error())
			return
		}

		txHash := tx.Hash()
		json.NewEncoder(w).Encode(TransactionResponse{
			Success: true,
			Message: fmt.Sprintf("%s transaction submitted to mempool", req.Type),
			TxHash:  fmt.Sprintf("%x", txHash[:]),
		})
		return
	}

	// Legacy flow: generate nonce server-side and submit directly to consensus
	nonce := atomic.AddUint64(&h.nonceCounter, 1)
	var tx *statemachine.Transaction

	switch req.Type {
	case "set":
		tx = statemachine.NewSetTransaction(req.Key, req.Value, nonce)
	case "add":
		tx = statemachine.NewAddTransaction(req.Key, req.Value, nonce)
	case "sub":
		tx = statemachine.NewSubTransaction(req.Key, req.Value, nonce)
	case "del":
		tx = statemachine.NewDelTransaction(req.Key, nonce)
	case "set_balance":
		tx = statemachine.NewSetBalanceTransaction(req.Key, req.Value, nonce)
	default:
		h.writeError(w, http.StatusBadRequest, "invalid transaction type: "+req.Type)
		return
	}

	if err := h.submitTransaction(r.Context(), tx); err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to submit transaction: "+err.Error())
		return
	}

	json.NewEncoder(w).Encode(TransactionResponse{
		Success: true,
		Message: fmt.Sprintf("%s transaction submitted", req.Type),
	})
}

// submitTransaction adds transaction to mempool for eventual inclusion in a block.
// The BlockBuildService will pick it up when this node becomes leader.
func (h *Handler) submitTransaction(ctx context.Context, tx *statemachine.Transaction) error {
	if h.txSubmitService == nil {
		return fmt.Errorf("transaction submit service not configured")
	}

	// Submit without full validation (no signature for legacy endpoints)
	if err := h.txSubmitService.SubmitWithoutValidation(tx); err != nil {
		return fmt.Errorf("failed to add to mempool: %w", err)
	}

	h.logger.Info().
		Str("key", tx.Key).
		Str("type", tx.Type.String()).
		Msg("Transaction added to mempool")

	return nil
}

// handleGetAccount returns account information for a given address.
func (h *Handler) handleGetAccount(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	address := r.PathValue("address")
	if address == "" {
		h.writeError(w, http.StatusBadRequest, "address is required")
		return
	}

	if h.accountQueryService == nil {
		h.writeError(w, http.StatusServiceUnavailable, "account service not available")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	acc, err := h.accountQueryService.GetAccount(ctx, address)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to get account: "+err.Error())
		return
	}

	json.NewEncoder(w).Encode(AccountResponse{
		Address: acc.Address,
		Nonce:   acc.Nonce,
		Balance: acc.Balance,
	})
}

func (h *Handler) writeError(w http.ResponseWriter, status int, message string) {
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(ErrorResponse{Error: message})
}
