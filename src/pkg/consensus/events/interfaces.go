// Package events defines the event tracing abstraction interfaces for consensus testing and monitoring.
// These interfaces allow the consensus engine to operate independently of specific event collection implementations.
package events

import (
	"time"
)

// EventTracer provides production-safe event tracing for consensus protocols.
// Production implementations should use NoOpEventTracer to avoid any overhead.
// Testing implementations can collect events for comprehensive validation.
type EventTracer interface {
	// RecordEvent records a consensus event with associated payload
	RecordEvent(nodeID uint16, eventType EventType, payload EventPayload)
	
	// RecordTransition records a state transition between consensus phases
	RecordTransition(nodeID uint16, from, to State, trigger string)
	
	// RecordMessage records message sending/receiving for network analysis
	RecordMessage(nodeID uint16, direction MessageDirection, msgType string, payload EventPayload)
}

// EventType represents the type of consensus event that occurred
type EventType string

const (
	// Proposal Events - Block proposal lifecycle
	EventProposalCreated     EventType = "proposal_created"
	EventProposalBroadcasted EventType = "proposal_broadcasted"
	EventProposalReceived    EventType = "proposal_received" 
	EventProposalValidated   EventType = "proposal_validated"
	EventProposalRejected    EventType = "proposal_rejected"
	
	// Generic Vote Events (legacy - use phase-specific events below)
	EventVoteCreated   EventType = "vote_created"
	EventVoteSent      EventType = "vote_sent"
	EventVoteReceived  EventType = "vote_received"
	EventVoteValidated EventType = "vote_validated"
	
	// Prepare Phase Events - Phase-specific vote and QC events
	EventPrepareVoteCreated     EventType = "prepare_vote_created"
	EventPrepareVoteSent        EventType = "prepare_vote_sent"
	EventPrepareVoteReceived    EventType = "prepare_vote_received"
	EventPrepareVoteValidated   EventType = "prepare_vote_validated"
	EventPrepareQCFormed        EventType = "prepare_qc_formed"
	EventPrepareQCBroadcasted   EventType = "prepare_qc_broadcasted"
	EventPrepareQCReceived      EventType = "prepare_qc_received"
	EventPrepareQCValidated     EventType = "prepare_qc_validated"
	
	// PreCommit Phase Events - Phase-specific vote and QC events
	EventPreCommitVoteCreated   EventType = "precommit_vote_created"
	EventPreCommitVoteSent      EventType = "precommit_vote_sent"
	EventPreCommitVoteReceived  EventType = "precommit_vote_received"
	EventPreCommitVoteValidated EventType = "precommit_vote_validated"
	EventPreCommitQCFormed      EventType = "precommit_qc_formed"
	EventPreCommitQCBroadcasted EventType = "precommit_qc_broadcasted"
	EventPreCommitQCReceived    EventType = "precommit_qc_received"
	EventPreCommitQCValidated   EventType = "precommit_qc_validated"
	
	// Commit Phase Events - Phase-specific vote and QC events
	EventCommitVoteCreated     EventType = "commit_vote_created"
	EventCommitVoteSent        EventType = "commit_vote_sent"
	EventCommitVoteReceived    EventType = "commit_vote_received"
	EventCommitVoteValidated   EventType = "commit_vote_validated"
	EventCommitQCFormed        EventType = "commit_qc_formed"
	EventCommitQCBroadcasted   EventType = "commit_qc_broadcasted"
	EventCommitQCReceived      EventType = "commit_qc_received"
	EventCommitQCValidated     EventType = "commit_qc_validated"
	
	// Generic QC Events (legacy - use phase-specific events above)
	EventQCFormed     EventType = "qc_formed"
	EventQCReceived   EventType = "qc_received"
	EventQCValidated  EventType = "qc_validated"
	
	// View Management Events - Timeouts, view changes, and leader election
	EventViewTimerStarted         EventType = "view_timer_started"
	EventViewTimeout              EventType = "view_timeout"
	EventViewTimeoutDetected      EventType = "view_timeout_detected"
	EventTimeoutMessageSent       EventType = "timeout_message_sent"
	EventTimeoutMessageReceived   EventType = "timeout_message_received"
	EventTimeoutMessageValidated  EventType = "timeout_message_validated"
	EventViewChange               EventType = "view_change"
	EventViewChangeStarted        EventType = "view_change_started"
	EventNewViewStarted           EventType = "new_view_started"
	EventNewViewMessageSent       EventType = "new_view_message_sent"
	EventNewViewMessageReceived   EventType = "new_view_message_received"
	EventNewViewMessageValidated  EventType = "new_view_message_validated"
	EventLeaderElected            EventType = "leader_elected"
	
	// Block and Safety Events - Block processing and safety predicates
	EventBlockAdded           EventType = "block_added"
	EventBlockValidated       EventType = "block_validated"
	EventBlockCommitted       EventType = "block_committed"
	EventHighestQCUpdated     EventType = "highest_qc_updated"
	EventLockedQCUpdated      EventType = "locked_qc_updated"
	EventSafeNodePassed       EventType = "safenode_check_passed"
	EventSafeNodeFailed       EventType = "safenode_check_failed"
	
	// Storage Events - Persistent state management (phase-specific)
	EventStorageTxBegin        EventType = "storage_tx_begin"        // Generic (for backward compatibility)
	EventStorageTxCommit       EventType = "storage_tx_commit"       // Generic (for backward compatibility)
	EventStorageTxRollback     EventType = "storage_tx_rollback"     // Generic (for backward compatibility)
	
	// Phase-specific storage events (following existing phase-specific pattern)
	EventPrepareStorageTxBegin    EventType = "prepare_storage_tx_begin"
	EventPrepareStorageTxCommit   EventType = "prepare_storage_tx_commit"
	EventPreCommitStorageTxBegin  EventType = "precommit_storage_tx_begin" 
	EventPreCommitStorageTxCommit EventType = "precommit_storage_tx_commit"
	EventCommitStorageTxBegin     EventType = "commit_storage_tx_begin"
	EventCommitStorageTxCommit    EventType = "commit_storage_tx_commit"
	
	// State Synchronization Events - Network partition recovery
	EventStateSyncRequested  EventType = "state_sync_requested"
	EventStateSyncCompleted  EventType = "state_sync_completed"
	
	// Network Partition Events - Partition detection and recovery
	EventPartitionModeEntered EventType = "partition_mode_entered"
	EventPartitionModeExited  EventType = "partition_mode_exited"
	
	// Byzantine Events - Malicious behavior detection and handling
	EventByzantineDetected   EventType = "byzantine_detected"
	EventEquivocationFound   EventType = "equivocation_found"
	EventStaleVoteRejected   EventType = "stale_vote_rejected" // Replay attack detection
	EventEvidenceStored      EventType = "evidence_stored"
	EventEvidenceBroadcasted EventType = "evidence_broadcasted"
	EventValidatorExcluded   EventType = "validator_excluded"
	
	// State Events - Internal state transitions
	EventStateTransition EventType = "state_transition"
	EventPhaseTransition EventType = "phase_transition"
)

// EventPayload contains event-specific data as key-value pairs
type EventPayload map[string]interface{}

// State represents consensus node internal states
type State string

const (
	StateIdle        State = "idle"
	StateProposing   State = "proposing"
	StateVoting      State = "voting"
	StateWaitingQC   State = "waiting_qc"
	StateCommitting  State = "committing"
	StateRecovering  State = "recovering"
	StatePartitioned State = "partitioned"
)

// MessageDirection indicates whether a message is being sent or received
type MessageDirection string

const (
	MessageInbound  MessageDirection = "inbound"
	MessageOutbound MessageDirection = "outbound"
)

// ConsensusEvent represents a single recorded event in the consensus protocol
type ConsensusEvent struct {
	NodeID     uint16           `json:"node_id"`
	EventType  EventType        `json:"event_type"`
	Payload    EventPayload     `json:"payload"`
	Timestamp  time.Time        `json:"timestamp"`
	
	// State transition specific fields
	FromState  State            `json:"from_state,omitempty"`
	ToState    State            `json:"to_state,omitempty"`
	Trigger    string           `json:"trigger,omitempty"`
	
	// Message specific fields
	Direction  MessageDirection `json:"direction,omitempty"`
	MessageType string          `json:"message_type,omitempty"`
}

// NoOpEventTracer provides a zero-overhead implementation for production use.
// All methods are no-ops and should be optimized away by the compiler.
type NoOpEventTracer struct{}

// RecordEvent does nothing in production builds
func (t *NoOpEventTracer) RecordEvent(nodeID uint16, eventType EventType, payload EventPayload) {}

// RecordTransition does nothing in production builds  
func (t *NoOpEventTracer) RecordTransition(nodeID uint16, from, to State, trigger string) {}

// RecordMessage does nothing in production builds
func (t *NoOpEventTracer) RecordMessage(nodeID uint16, direction MessageDirection, msgType string, payload EventPayload) {}