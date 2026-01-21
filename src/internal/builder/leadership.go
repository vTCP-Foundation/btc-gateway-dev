package builder

import (
	"sync"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"btc-gateway/pkg/consensus/types"
)

// LeadershipHandler listens for leadership events and triggers block building.
type LeadershipHandler struct {
	buildService *BlockBuildService
	proposer     ConsensusProposer
	nodeID       types.NodeID
	logger       zerolog.Logger

	mu           sync.Mutex
	lastViewBuilt types.ViewNumber
}

// NewLeadershipHandler creates a new leadership handler.
func NewLeadershipHandler(buildService *BlockBuildService, proposer ConsensusProposer, nodeID types.NodeID) *LeadershipHandler {
	return &LeadershipHandler{
		buildService:  buildService,
		proposer:      proposer,
		nodeID:        nodeID,
		logger:        log.With().Str("component", "leadership_handler").Logger(),
		lastViewBuilt: 0,
	}
}

// OnLeadershipGained is called when this node gains leadership.
// It triggers block building if we haven't already built for this view.
func (h *LeadershipHandler) OnLeadershipGained(view types.ViewNumber) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Prevent duplicate builds for the same view
	if view <= h.lastViewBuilt {
		h.logger.Debug().
			Uint64("view", uint64(view)).
			Uint64("last_built", uint64(h.lastViewBuilt)).
			Msg("Skipping block build - already built for this view")
		return
	}

	// Verify we are still the leader
	if !h.proposer.IsLeader() {
		h.logger.Debug().
			Uint64("view", uint64(view)).
			Msg("Not leader, skipping block build")
		return
	}

	h.logger.Info().
		Uint64("view", uint64(view)).
		Uint16("node_id", uint16(h.nodeID)).
		Msg("Leadership gained, building block")

	// Build and propose block
	txCount, err := h.buildService.BuildAndPropose()
	if err != nil {
		h.logger.Error().
			Err(err).
			Uint64("view", uint64(view)).
			Msg("Failed to build and propose block")
		return
	}

	// Mark view as built
	h.lastViewBuilt = view

	h.logger.Info().
		Uint64("view", uint64(view)).
		Int("tx_count", txCount).
		Msg("Block built and proposed")
}
