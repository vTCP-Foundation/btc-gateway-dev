//go:build consensus_testing

package engine

import "btc-gateway/pkg/consensus/types"

// ForceSetLockedQC directly sets the lockedQC on SafetyRules without phase validation.
// UNSAFE: This bypasses HotStuff protocol safety rules.
// Only use for testing state synchronization scenarios.
func (sr *SafetyRules) ForceSetLockedQC(qc *types.QuorumCertificate) {
	sr.lockedQC = qc
}
