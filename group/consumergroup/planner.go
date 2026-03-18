package consumergroup

import (
	"sort"
	"time"
)

type leaseState struct {
	ShardID          string
	Owner            string
	ExpiresAt        time.Time
	ParentShardID    string
	AdjacentParentID string
	Completed        bool
}

type assignmentPlan struct {
	ClaimShardIDs   []string
	ReleaseShardIDs []string
	RenewShardIDs   []string
}

type assignmentPlanner struct {
	WorkerID           string
	Now                time.Time
	MaxLeasesForWorker int
}

func (p assignmentPlanner) Plan(leases []leaseState, activeWorkers []string) assignmentPlan {
	workers := normalizeActiveWorkers(activeWorkers, p.WorkerID)
	incompleteLeases := 0
	for _, lease := range leases {
		if !lease.Completed {
			incompleteLeases++
		}
	}
	target := targetLeaseCount(incompleteLeases, len(workers), p.MaxLeasesForWorker)

	var plan assignmentPlan
	var ownedLeases []string
	leaseByShard := make(map[string]leaseState, len(leases))

	for _, lease := range leases {
		leaseByShard[lease.ShardID] = lease
	}

	for _, lease := range leases {
		if lease.Completed {
			continue
		}
		if lease.Owner == p.WorkerID && !isExpired(lease, p.Now) {
			ownedLeases = append(ownedLeases, lease.ShardID)
		}
	}

	sort.Strings(ownedLeases)
	if len(ownedLeases) > target {
		plan.RenewShardIDs = append(plan.RenewShardIDs, ownedLeases[:target]...)
		plan.ReleaseShardIDs = append(plan.ReleaseShardIDs, ownedLeases[target:]...)
		return plan
	}
	plan.RenewShardIDs = append(plan.RenewShardIDs, ownedLeases...)

	need := target - len(ownedLeases)
	if need <= 0 {
		return plan
	}

	// Prefer unowned leases first.
	for _, lease := range leases {
		if need <= 0 {
			break
		}
		if !isClaimable(lease, leaseByShard) {
			continue
		}
		if lease.Owner != "" {
			continue
		}
		plan.ClaimShardIDs = append(plan.ClaimShardIDs, lease.ShardID)
		need--
	}

	// Then expired leases.
	for _, lease := range leases {
		if need <= 0 {
			break
		}
		if !isClaimable(lease, leaseByShard) {
			continue
		}
		if lease.Owner == "" || lease.Owner == p.WorkerID {
			continue
		}
		if !isExpired(lease, p.Now) {
			continue
		}
		plan.ClaimShardIDs = append(plan.ClaimShardIDs, lease.ShardID)
		need--
	}

	return plan
}

func targetLeaseCount(totalLeases, activeWorkers, maxLeasesForWorker int) int {
	if totalLeases <= 0 {
		return 0
	}
	if activeWorkers <= 0 {
		activeWorkers = 1
	}
	target := (totalLeases + activeWorkers - 1) / activeWorkers
	if maxLeasesForWorker > 0 && target > maxLeasesForWorker {
		target = maxLeasesForWorker
	}
	return target
}

func normalizeActiveWorkers(workers []string, self string) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, worker := range workers {
		if worker == "" {
			continue
		}
		if _, ok := seen[worker]; ok {
			continue
		}
		seen[worker] = struct{}{}
		out = append(out, worker)
	}
	if self != "" {
		if _, ok := seen[self]; !ok {
			out = append(out, self)
		}
	}
	return out
}

func isExpired(lease leaseState, now time.Time) bool {
	if lease.ExpiresAt.IsZero() {
		return false
	}
	return !lease.ExpiresAt.After(now)
}

func isClaimable(lease leaseState, byShard map[string]leaseState) bool {
	if lease.Completed {
		return false
	}
	if !parentCompleted(lease.ParentShardID, byShard) {
		return false
	}
	if !parentCompleted(lease.AdjacentParentID, byShard) {
		return false
	}
	return true
}

func parentCompleted(parentShardID string, byShard map[string]leaseState) bool {
	if parentShardID == "" {
		return true
	}
	parent, ok := byShard[parentShardID]
	if !ok {
		return true
	}
	return parent.Completed
}
