// Package testing provides sequence builders for comprehensive consensus protocol validation.
// This package builds complete expected event sequences based on the HotStuff data flow diagram.
package testing

import (
	"time"
	
	"btc-gateway/pkg/consensus/events"
)

// calculateLeader determines which node is the leader for a given view
func calculateLeader(view uint64, nodeCount uint16) uint16 {
	return uint16(view % uint64(nodeCount))
}

// CalculateQuorum returns the minimum number of votes required for a quorum.
// This MUST match types.ConsensusConfig.QuorumThreshold() exactly.
// HotStuff BFT requires ⌊2n/3⌋+1 votes to ensure >2/3 honest majority.
// For n=5: ⌊10/3⌋+1 = 4 votes required.
func CalculateQuorum(nodeCount int) int {
	return (2*nodeCount)/3 + 1
}

// BuildCompleteHappyPathSequence builds the complete expected event sequence for a successful consensus round
func BuildCompleteHappyPathSequence(nodeCount int, view int) []ExpectedEventSequence {
	leader := calculateLeader(uint64(view), uint16(nodeCount))
	validators := nodeCount - 1 // All nodes except leader
	quorum := CalculateQuorum(nodeCount)
	
	sequence := []ExpectedEventSequence{}
	seqNum := 1
	
	// Build each phase of the consensus protocol
	sequence = append(sequence, buildNewViewPhase(&seqNum, nodeCount, int(leader))...)
	sequence = append(sequence, buildProposalPhase(&seqNum, nodeCount, int(leader))...)
	sequence = append(sequence, buildPreparePhase(&seqNum, nodeCount, int(leader), validators, quorum)...)
	sequence = append(sequence, buildPreCommitPhase(&seqNum, nodeCount, int(leader), validators, quorum)...)
	sequence = append(sequence, buildCommitPhase(&seqNum, nodeCount, int(leader), validators, quorum)...)
	sequence = append(sequence, buildDecidePhase(&seqNum, nodeCount, int(leader))...)
	
	return sequence
}

// buildNewViewPhase builds the expected events for the NewView phase
func buildNewViewPhase(seqNum *int, nodeCount int, leader int) []ExpectedEventSequence {
	sequence := []ExpectedEventSequence{
		{
			SequenceNumber: *seqNum,
			Phase:          "NewView",
			SubPhase:       "Timer",
			NodeRole:       "Leader",
			NodeID:         &leader,
			EventType:      events.EventViewTimerStarted,
			Required:       true,
			MinOccurrences: 1,
			MaxOccurrences: 3, // Allow multiple timer restarts during normal operation
			MaxDelay:       50 * time.Millisecond,
			Description:    "Leader starts NewView collection timer",
			FailureHint:    "Leader failed to start timer - check pacemaker initialization",
		},
		{
			SequenceNumber: *seqNum + 1,
			Phase:          "NewView",
			SubPhase:       "Collection", 
			NodeRole:       "Validator",
			EventType:      events.EventNewViewMessageSent,
			Required:       true,
			MinOccurrences: nodeCount - 1, // All validators send NewView
			MaxOccurrences: nodeCount - 1,
			WindowStart:    events.EventViewTimerStarted,
			WindowDuration: 2 * time.Second,
			DependsOn:      []int{*seqNum},
			Description:    "Validators send NewView messages to leader",
			FailureHint:    "Not all validators sent NewView messages - check network connectivity",
		},
		{
			SequenceNumber: *seqNum + 2,
			Phase:          "NewView",
			SubPhase:       "Reception",
			NodeRole:       "Leader",
			NodeID:         &leader,
			EventType:      events.EventNewViewMessageReceived,
			Required:       true,
			MinOccurrences: CalculateQuorum(nodeCount), // Quorum of NewView messages
			MaxOccurrences: nodeCount - 1,
			WindowStart:    events.EventNewViewMessageSent,
			WindowDuration: 1 * time.Second,
			DependsOn:      []int{*seqNum + 1},
			Description:    "Leader receives quorum of NewView messages",
			FailureHint:    "Leader didn't receive enough NewView messages - check network or validator liveness",
		},
		{
			SequenceNumber: *seqNum + 3,
			Phase:          "NewView",
			SubPhase:       "Validation",
			NodeRole:       "Leader",
			NodeID:         &leader,
			EventType:      events.EventNewViewMessageValidated,
			Required:       true,
			MinOccurrences: CalculateQuorum(nodeCount), // Quorum validation
			MaxOccurrences: nodeCount - 1,
			MaxDelay:       100 * time.Millisecond,
			DependsOn:      []int{*seqNum + 2},
			Description:    "Leader validates NewView message signatures",
			FailureHint:    "NewView message validation failed - check cryptographic signatures",
		},
		{
			SequenceNumber: *seqNum + 4,
			Phase:          "NewView",
			SubPhase:       "QC Update",
			NodeRole:       "Leader", 
			NodeID:         &leader,
			EventType:      events.EventHighestQCUpdated,
			Required:       true,
			MinOccurrences: 1,
			MaxOccurrences: 1,
			MaxDelay:       20 * time.Millisecond,
			DependsOn:      []int{*seqNum + 3},
			EnablesEvents:  []int{*seqNum + 5},
			Description:    "Leader updates highest QC from NewView messages",
			FailureHint:    "Failed to update highest QC - check QC selection logic",
		},
	}
	
	*seqNum += len(sequence)
	return sequence
}

// buildProposalPhase builds the expected events for the block proposal phase
func buildProposalPhase(seqNum *int, nodeCount int, leader int) []ExpectedEventSequence {
	sequence := []ExpectedEventSequence{
		{
			SequenceNumber: *seqNum,
			Phase:          "Proposal",
			SubPhase:       "Creation",
			NodeRole:       "Leader",
			NodeID:         &leader,
			EventType:      events.EventProposalCreated,
			Required:       true,
			MinOccurrences: 1,
			MaxOccurrences: 1,
			MaxDelay:       100 * time.Millisecond,
			Description:    "Leader creates new block proposal",
			FailureHint:    "Leader failed to create proposal - check block construction logic",
		},
		{
			SequenceNumber: *seqNum + 1,
			Phase:          "Proposal",
			SubPhase:       "Broadcast",
			NodeRole:       "Leader",
			NodeID:         &leader,
			EventType:      events.EventProposalBroadcasted,
			Required:       true,
			MinOccurrences: 1,
			MaxOccurrences: 1,
			MaxDelay:       50 * time.Millisecond,
			DependsOn:      []int{*seqNum},
			EnablesEvents:  []int{*seqNum + 2},
			Description:    "Leader broadcasts proposal to all validators",
			FailureHint:    "Leader failed to broadcast proposal - check network layer",
		},
	}
	
	*seqNum += len(sequence)
	return sequence
}

// buildPreparePhase builds the expected events for the Prepare phase
func buildPreparePhase(seqNum *int, nodeCount int, leader int, validators int, quorum int) []ExpectedEventSequence {
	sequence := []ExpectedEventSequence{
		// Validators receive proposal
		{
			SequenceNumber: *seqNum,
			Phase:          "Prepare",
			SubPhase:       "Reception",
			NodeRole:       "Validator",
			EventType:      events.EventProposalReceived,
			Required:       true,
			MinOccurrences: validators,
			MaxOccurrences: validators,
			WindowStart:    events.EventProposalBroadcasted,
			WindowDuration: 500 * time.Millisecond,
			Description:    "All validators receive the proposal",
			FailureHint:    "Not all validators received proposal - check network connectivity",
		},
		
		// Validators validate proposal
		{
			SequenceNumber: *seqNum + 1,
			Phase:          "Prepare", 
			SubPhase:       "Validation",
			NodeRole:       "Validator",
			EventType:      events.EventProposalValidated,
			Required:       true,
			MinOccurrences: validators,
			MaxOccurrences: validators,
			MaxDelay:       50 * time.Millisecond,
			DependsOn:      []int{*seqNum},
			Description:    "All validators validate the proposal structure and signatures",
			FailureHint:    "Proposal validation failed - check proposal validity and signature verification",
		},
		
		// SafeNode predicate check
		{
			SequenceNumber: *seqNum + 2,
			Phase:          "Prepare",
			SubPhase:       "Safety",
			NodeRole:       "Validator",
			EventType:      events.EventSafeNodePassed,
			Required:       true,
			MinOccurrences: validators,
			MaxOccurrences: validators,
			MaxDelay:       10 * time.Millisecond,
			DependsOn:      []int{*seqNum + 1},
			Description:    "SafeNode predicate passes for all validators",
			FailureHint:    "SafeNode check failed - verify block extends from lockedQC or justify.view > lockedQC.view",
		},
		
		// Block added to tree
		{
			SequenceNumber: *seqNum + 3,
			Phase:          "Prepare",
			SubPhase:       "Storage",
			NodeRole:       "Validator",
			EventType:      events.EventBlockAdded,
			Required:       true,
			MinOccurrences: validators,
			MaxOccurrences: validators,
			MaxDelay:       20 * time.Millisecond,
			DependsOn:      []int{*seqNum + 2},
			Description:    "Block added to block tree by all validators",
			FailureHint:    "Failed to add block to tree - check storage layer and block tree logic",
		},
		
		// Validators create prepare votes
		{
			SequenceNumber: *seqNum + 4,
			Phase:          "Prepare",
			SubPhase:       "Voting",
			NodeRole:       "Validator",
			EventType:      events.EventPrepareVoteCreated,
			Required:       true,
			MinOccurrences: validators,
			MaxOccurrences: validators,
			MaxDelay:       30 * time.Millisecond,
			DependsOn:      []int{*seqNum + 3},
			EnablesEvents:  []int{*seqNum + 5},
			Description:    "Validators create prepare votes after validation",
			FailureHint:    "Vote creation failed - check cryptographic signing and vote construction",
		},
		
		// Validators send votes to leader
		{
			SequenceNumber: *seqNum + 5,
			Phase:          "Prepare",
			SubPhase:       "Voting",
			NodeRole:       "Validator",
			EventType:      events.EventPrepareVoteSent,
			Required:       true,
			MinOccurrences: validators,
			MaxOccurrences: validators,
			MaxDelay:       10 * time.Millisecond,
			DependsOn:      []int{*seqNum + 4},
			Description:    "Validators send prepare votes to leader",
			FailureHint:    "Failed to send votes - check network layer and message routing",
		},
		
		// Leader receives votes
		{
			SequenceNumber: *seqNum + 6,
			Phase:          "Prepare",
			SubPhase:       "Vote Collection",
			NodeRole:       "Leader",
			NodeID:         &leader,
			EventType:      events.EventPrepareVoteReceived,
			Required:       true,
			MinOccurrences: quorum,
			MaxOccurrences: validators,
			WindowStart:    events.EventPrepareVoteSent,
			WindowDuration: 1 * time.Second,
			DependsOn:      []int{*seqNum + 5},
			Description:    "Leader receives prepare votes from validators (quorum required)",
			FailureHint:    "Leader didn't receive enough votes - check network connectivity and validator participation",
		},
		
		// Leader validates votes
		{
			SequenceNumber: *seqNum + 7,
			Phase:          "Prepare",
			SubPhase:       "Vote Validation",
			NodeRole:       "Leader",
			NodeID:         &leader,
			EventType:      events.EventPrepareVoteValidated,
			Required:       true,
			MinOccurrences: quorum,
			MaxOccurrences: validators,
			MaxDelay:       50 * time.Millisecond,
			DependsOn:      []int{*seqNum + 6},
			Description:    "Leader validates received prepare votes",
			FailureHint:    "Vote validation failed - check signature verification and vote authenticity",
		},
		
		// Prepare QC Formation
		{
			SequenceNumber: *seqNum + 8,
			Phase:          "Prepare",
			SubPhase:       "QC Formation",
			NodeRole:       "Leader",
			NodeID:         &leader,
			EventType:      events.EventPrepareQCFormed,
			Required:       true,
			MinOccurrences: 1,
			MaxOccurrences: 1,
			MaxDelay:       100 * time.Millisecond,
			DependsOn:      []int{*seqNum + 7},
			EnablesEvents:  []int{*seqNum + 11}, // Enables QC broadcast
			Description:    "Leader forms Prepare QC from collected votes",
			FailureHint:    "QC formation failed - check vote aggregation and threshold validation",
		},
		
		// Storage transaction for QC
		{
			SequenceNumber: *seqNum + 9,
			Phase:          "Prepare",
			SubPhase:       "QC Storage",
			NodeRole:       "Leader",
			NodeID:         &leader,
			EventType:      events.EventPrepareStorageTxBegin,
			Required:       true,
			MinOccurrences: 1,
			MaxOccurrences: 1,
			MaxDelay:       10 * time.Millisecond,
			DependsOn:      []int{*seqNum + 8},
			Description:    "Begin atomic storage transaction for Prepare QC",
			FailureHint:    "Storage transaction failed to start - check storage layer availability",
		},
		
		{
			SequenceNumber: *seqNum + 10,
			Phase:          "Prepare",
			SubPhase:       "QC Storage",
			NodeRole:       "Leader",
			NodeID:         &leader,
			EventType:      events.EventPrepareStorageTxCommit,
			Required:       true,
			MinOccurrences: 1,
			MaxOccurrences: 1,
			MaxDelay:       50 * time.Millisecond,
			DependsOn:      []int{*seqNum + 9},
			Description:    "Commit Prepare QC to persistent storage",
			FailureHint:    "Storage commit failed - check disk space and storage reliability",
		},
		
		// Broadcast Prepare QC
		{
			SequenceNumber: *seqNum + 11,
			Phase:          "Prepare",
			SubPhase:       "QC Broadcast",
			NodeRole:       "Leader",
			NodeID:         &leader,
			EventType:      events.EventPrepareQCBroadcasted,
			Required:       true,
			MinOccurrences: 1,
			MaxOccurrences: 1,
			MaxDelay:       20 * time.Millisecond,
			DependsOn:      []int{*seqNum + 10},
			Description:    "Leader broadcasts Prepare QC to all validators",
			FailureHint:    "QC broadcast failed - check network layer and message routing",
		},
	}
	
	*seqNum += len(sequence)
	return sequence
}

// buildPreCommitPhase builds the expected events for the PreCommit phase
func buildPreCommitPhase(seqNum *int, nodeCount int, leader int, validators int, quorum int) []ExpectedEventSequence {
	sequence := []ExpectedEventSequence{
		// Validators receive Prepare QC
		{
			SequenceNumber: *seqNum,
			Phase:          "PreCommit",
			SubPhase:       "QC Reception",
			NodeRole:       "Validator",
			EventType:      events.EventPrepareQCReceived,
			Required:       true,
			MinOccurrences: validators,
			MaxOccurrences: validators,
			WindowStart:    events.EventPrepareQCBroadcasted,
			WindowDuration: 500 * time.Millisecond,
			Description:    "All validators receive Prepare QC",
			FailureHint:    "Not all validators received Prepare QC - check network connectivity",
		},
		
		// Validators validate Prepare QC
		{
			SequenceNumber: *seqNum + 1,
			Phase:          "PreCommit",
			SubPhase:       "QC Validation",
			NodeRole:       "Validator",
			EventType:      events.EventPrepareQCValidated,
			Required:       true,
			MinOccurrences: validators,
			MaxOccurrences: validators,
			MaxDelay:       50 * time.Millisecond,
			DependsOn:      []int{*seqNum},
			Description:    "All validators validate Prepare QC signatures and threshold",
			FailureHint:    "Prepare QC validation failed - check QC signature verification",
		},
		
		// Locked QC update
		{
			SequenceNumber: *seqNum + 2,
			Phase:          "PreCommit",
			SubPhase:       "Safety Update",
			NodeRole:       "Validator",
			EventType:      events.EventLockedQCUpdated,
			Required:       true,
			MinOccurrences: validators,
			MaxOccurrences: validators,
			MaxDelay:       10 * time.Millisecond,
			DependsOn:      []int{*seqNum + 1},
			Description:    "Validators update locked QC if Prepare QC view ≥ current locked view",
			FailureHint:    "Failed to update locked QC - check view comparison and safety predicate logic",
		},
		
		// Validators create precommit votes
		{
			SequenceNumber: *seqNum + 3,
			Phase:          "PreCommit",
			SubPhase:       "Voting",
			NodeRole:       "Validator",
			EventType:      events.EventPreCommitVoteCreated,
			Required:       true,
			MinOccurrences: validators,
			MaxOccurrences: validators,
			MaxDelay:       30 * time.Millisecond,
			DependsOn:      []int{*seqNum + 2},
			EnablesEvents:  []int{*seqNum + 4},
			Description:    "Validators create precommit votes after QC validation",
			FailureHint:    "Precommit vote creation failed - check cryptographic signing",
		},
		
		// Validators send precommit votes
		{
			SequenceNumber: *seqNum + 4,
			Phase:          "PreCommit",
			SubPhase:       "Voting",
			NodeRole:       "Validator",
			EventType:      events.EventPreCommitVoteSent,
			Required:       true,
			MinOccurrences: validators,
			MaxOccurrences: validators,
			MaxDelay:       10 * time.Millisecond,
			DependsOn:      []int{*seqNum + 3},
			Description:    "Validators send precommit votes to leader",
			FailureHint:    "Failed to send precommit votes - check network layer",
		},
		
		// Leader receives precommit votes
		{
			SequenceNumber: *seqNum + 5,
			Phase:          "PreCommit",
			SubPhase:       "Vote Collection",
			NodeRole:       "Leader",
			NodeID:         &leader,
			EventType:      events.EventPreCommitVoteReceived,
			Required:       true,
			MinOccurrences: quorum,
			MaxOccurrences: validators,
			WindowStart:    events.EventPreCommitVoteSent,
			WindowDuration: 1 * time.Second,
			DependsOn:      []int{*seqNum + 4},
			Description:    "Leader receives precommit votes (quorum required)",
			FailureHint:    "Insufficient precommit votes received - check validator participation",
		},
		
		// Leader validates precommit votes
		{
			SequenceNumber: *seqNum + 6,
			Phase:          "PreCommit",
			SubPhase:       "Vote Validation",
			NodeRole:       "Leader",
			NodeID:         &leader,
			EventType:      events.EventPreCommitVoteValidated,
			Required:       true,
			MinOccurrences: quorum,
			MaxOccurrences: validators,
			MaxDelay:       50 * time.Millisecond,
			DependsOn:      []int{*seqNum + 5},
			Description:    "Leader validates precommit vote signatures",
			FailureHint:    "Precommit vote validation failed - check signature verification",
		},
		
		// PreCommit QC formation
		{
			SequenceNumber: *seqNum + 7,
			Phase:          "PreCommit",
			SubPhase:       "QC Formation",
			NodeRole:       "Leader",
			NodeID:         &leader,
			EventType:      events.EventPreCommitQCFormed,
			Required:       true,
			MinOccurrences: 1,
			MaxOccurrences: 1,
			MaxDelay:       100 * time.Millisecond,
			DependsOn:      []int{*seqNum + 6},
			Description:    "Leader forms PreCommit QC from votes",
			FailureHint:    "PreCommit QC formation failed - check vote aggregation",
		},
		
		// Storage transaction for PreCommit QC
		{
			SequenceNumber: *seqNum + 8,
			Phase:          "PreCommit",
			SubPhase:       "QC Storage",
			NodeRole:       "Leader",
			NodeID:         &leader,
			EventType:      events.EventPreCommitStorageTxBegin,
			Required:       true,
			MinOccurrences: 1,
			MaxOccurrences: 1,
			MaxDelay:       10 * time.Millisecond,
			DependsOn:      []int{*seqNum + 7},
			Description:    "Begin storage transaction for PreCommit QC",
			FailureHint:    "Storage transaction failed to start",
		},
		
		{
			SequenceNumber: *seqNum + 9,
			Phase:          "PreCommit",
			SubPhase:       "QC Storage",
			NodeRole:       "Leader",
			NodeID:         &leader,
			EventType:      events.EventPreCommitStorageTxCommit,
			Required:       true,
			MinOccurrences: 1,
			MaxOccurrences: 1,
			MaxDelay:       50 * time.Millisecond,
			DependsOn:      []int{*seqNum + 8},
			Description:    "Commit PreCommit QC to storage",
			FailureHint:    "Storage commit failed for PreCommit QC",
		},
		
		// Broadcast PreCommit QC
		{
			SequenceNumber: *seqNum + 10,
			Phase:          "PreCommit",
			SubPhase:       "QC Broadcast",
			NodeRole:       "Leader",
			NodeID:         &leader,
			EventType:      events.EventPreCommitQCBroadcasted,
			Required:       true,
			MinOccurrences: 1,
			MaxOccurrences: 1,
			MaxDelay:       20 * time.Millisecond,
			DependsOn:      []int{*seqNum + 9},
			Description:    "Leader broadcasts PreCommit QC",
			FailureHint:    "PreCommit QC broadcast failed",
		},
	}
	
	*seqNum += len(sequence)
	return sequence
}

// buildCommitPhase builds the expected events for the Commit phase
func buildCommitPhase(seqNum *int, nodeCount int, leader int, validators int, quorum int) []ExpectedEventSequence {
	sequence := []ExpectedEventSequence{
		// Validators receive PreCommit QC
		{
			SequenceNumber: *seqNum,
			Phase:          "Commit",
			SubPhase:       "QC Reception",
			NodeRole:       "Validator",
			EventType:      events.EventPreCommitQCReceived,
			Required:       true,
			MinOccurrences: validators,
			MaxOccurrences: validators,
			WindowStart:    events.EventPreCommitQCBroadcasted,
			WindowDuration: 500 * time.Millisecond,
			Description:    "All validators receive PreCommit QC",
			FailureHint:    "Not all validators received PreCommit QC",
		},
		
		// Validators validate PreCommit QC
		{
			SequenceNumber: *seqNum + 1,
			Phase:          "Commit",
			SubPhase:       "QC Validation",
			NodeRole:       "Validator",
			EventType:      events.EventPreCommitQCValidated,
			Required:       true,
			MinOccurrences: validators,
			MaxOccurrences: validators,
			MaxDelay:       50 * time.Millisecond,
			DependsOn:      []int{*seqNum},
			Description:    "All validators validate PreCommit QC",
			FailureHint:    "PreCommit QC validation failed",
		},
		
		// Validators create commit votes
		{
			SequenceNumber: *seqNum + 2,
			Phase:          "Commit",
			SubPhase:       "Voting",
			NodeRole:       "Validator",
			EventType:      events.EventCommitVoteCreated,
			Required:       true,
			MinOccurrences: validators,
			MaxOccurrences: validators,
			MaxDelay:       30 * time.Millisecond,
			DependsOn:      []int{*seqNum + 1},
			Description:    "Validators create commit votes",
			FailureHint:    "Commit vote creation failed",
		},
		
		// Validators send commit votes
		{
			SequenceNumber: *seqNum + 3,
			Phase:          "Commit",
			SubPhase:       "Voting",
			NodeRole:       "Validator",
			EventType:      events.EventCommitVoteSent,
			Required:       true,
			MinOccurrences: validators,
			MaxOccurrences: validators,
			MaxDelay:       10 * time.Millisecond,
			DependsOn:      []int{*seqNum + 2},
			Description:    "Validators send commit votes to leader",
			FailureHint:    "Failed to send commit votes",
		},
		
		// Leader receives commit votes
		{
			SequenceNumber: *seqNum + 4,
			Phase:          "Commit",
			SubPhase:       "Vote Collection",
			NodeRole:       "Leader",
			NodeID:         &leader,
			EventType:      events.EventCommitVoteReceived,
			Required:       true,
			MinOccurrences: quorum,
			MaxOccurrences: validators,
			WindowStart:    events.EventCommitVoteSent,
			WindowDuration: 1 * time.Second,
			DependsOn:      []int{*seqNum + 3},
			Description:    "Leader receives commit votes (quorum required)",
			FailureHint:    "Insufficient commit votes received",
		},
		
		// Leader validates commit votes
		{
			SequenceNumber: *seqNum + 5,
			Phase:          "Commit",
			SubPhase:       "Vote Validation",
			NodeRole:       "Leader",
			NodeID:         &leader,
			EventType:      events.EventCommitVoteValidated,
			Required:       true,
			MinOccurrences: quorum,
			MaxOccurrences: validators,
			MaxDelay:       50 * time.Millisecond,
			DependsOn:      []int{*seqNum + 4},
			Description:    "Leader validates commit votes",
			FailureHint:    "Commit vote validation failed",
		},
		
		// Commit QC formation
		{
			SequenceNumber: *seqNum + 6,
			Phase:          "Commit",
			SubPhase:       "QC Formation",
			NodeRole:       "Leader",
			NodeID:         &leader,
			EventType:      events.EventCommitQCFormed,
			Required:       true,
			MinOccurrences: 1,
			MaxOccurrences: 1,
			MaxDelay:       100 * time.Millisecond,
			DependsOn:      []int{*seqNum + 5},
			Description:    "Leader forms Commit QC",
			FailureHint:    "Commit QC formation failed",
		},
		
		// Storage for Commit QC
		{
			SequenceNumber: *seqNum + 7,
			Phase:          "Commit",
			SubPhase:       "QC Storage",
			NodeRole:       "Leader",
			NodeID:         &leader,
			EventType:      events.EventCommitStorageTxBegin,
			Required:       true,
			MinOccurrences: 1,
			MaxOccurrences: 1,
			MaxDelay:       10 * time.Millisecond,
			DependsOn:      []int{*seqNum + 6},
			Description:    "Begin storage transaction for Commit QC",
			FailureHint:    "Storage transaction failed to start",
		},
		
		{
			SequenceNumber: *seqNum + 8,
			Phase:          "Commit",
			SubPhase:       "QC Storage",
			NodeRole:       "Leader",
			NodeID:         &leader,
			EventType:      events.EventCommitStorageTxCommit,
			Required:       true,
			MinOccurrences: 1,
			MaxOccurrences: 1,
			MaxDelay:       50 * time.Millisecond,
			DependsOn:      []int{*seqNum + 7},
			Description:    "Commit Commit QC to storage",
			FailureHint:    "Storage commit failed for Commit QC",
		},
		
		// Broadcast Commit QC
		{
			SequenceNumber: *seqNum + 9,
			Phase:          "Commit",
			SubPhase:       "QC Broadcast",
			NodeRole:       "Leader",
			NodeID:         &leader,
			EventType:      events.EventCommitQCBroadcasted,
			Required:       true,
			MinOccurrences: 1,
			MaxOccurrences: 1,
			MaxDelay:       20 * time.Millisecond,
			DependsOn:      []int{*seqNum + 8},
			Description:    "Leader broadcasts Commit QC",
			FailureHint:    "Commit QC broadcast failed",
		},
	}
	
	*seqNum += len(sequence)
	return sequence
}

// buildDecidePhase builds the expected events for the final Decide phase
func buildDecidePhase(seqNum *int, nodeCount int, leader int) []ExpectedEventSequence {
	sequence := []ExpectedEventSequence{
		// All nodes receive Commit QC
		{
			SequenceNumber: *seqNum,
			Phase:          "Decide",
			SubPhase:       "QC Reception",
			NodeRole:       "All",
			EventType:      events.EventCommitQCReceived,
			Required:       true,
			MinOccurrences: nodeCount,
			MaxOccurrences: nodeCount,
			WindowStart:    events.EventCommitQCBroadcasted,
			WindowDuration: 500 * time.Millisecond,
			Description:    "All nodes receive Commit QC",
			FailureHint:    "Not all nodes received Commit QC",
		},
		
		// All nodes validate Commit QC
		{
			SequenceNumber: *seqNum + 1,
			Phase:          "Decide",
			SubPhase:       "QC Validation",
			NodeRole:       "All",
			EventType:      events.EventCommitQCValidated,
			Required:       true,
			MinOccurrences: nodeCount,
			MaxOccurrences: nodeCount,
			MaxDelay:       50 * time.Millisecond,
			DependsOn:      []int{*seqNum},
			Description:    "All nodes validate Commit QC",
			FailureHint:    "Commit QC validation failed",
		},
		
		// All nodes commit the block (Simple HotStuff - direct commit)
		{
			SequenceNumber: *seqNum + 2,
			Phase:          "Decide",
			SubPhase:       "Block Commitment",
			NodeRole:       "All",
			EventType:      events.EventBlockCommitted,
			Required:       true,
			MinOccurrences: nodeCount,
			MaxOccurrences: nodeCount,
			MaxDelay:       100 * time.Millisecond,
			DependsOn:      []int{*seqNum + 1},
			Description:    "All nodes commit the block (Simple HotStuff direct commit)",
			FailureHint:    "Block commitment failed - check block finalization logic",
		},
	}
	
	*seqNum += len(sequence)
	return sequence
}