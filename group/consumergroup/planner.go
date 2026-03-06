package consumergroup

import "time"

type leaseState struct {
	ShardID   string
	Owner     string
	ExpiresAt time.Time
}

type stealRequest struct {
	ShardID    string
	FromWorker string
}

type assignmentPlan struct {
	ClaimShardIDs []string
	Steals        []stealRequest
	RenewShardIDs []string
}

type assignmentPlanner struct {
	WorkerID           string
	Now                time.Time
	MaxLeasesForWorker int
	MaxLeasesToSteal   int
	EnableStealing     bool
}

func (p assignmentPlanner) Plan(leases []leaseState, activeWorkers []string) assignmentPlan {
	workers := normalizeActiveWorkers(activeWorkers, p.WorkerID)
	target := targetLeaseCount(len(leases), len(workers), p.MaxLeasesForWorker)

	var plan assignmentPlan
	var owned int
	ownerCounts := map[string]int{}

	for _, lease := range leases {
		if lease.Owner != "" && !isExpired(lease, p.Now) {
			ownerCounts[lease.Owner]++
		}
		if lease.Owner == p.WorkerID && !isExpired(lease, p.Now) {
			owned++
			plan.RenewShardIDs = append(plan.RenewShardIDs, lease.ShardID)
		}
	}

	need := target - owned
	if need <= 0 {
		return plan
	}

	// Prefer unowned leases first.
	for _, lease := range leases {
		if need <= 0 {
			break
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
		if lease.Owner == "" || lease.Owner == p.WorkerID {
			continue
		}
		if !isExpired(lease, p.Now) {
			continue
		}
		plan.ClaimShardIDs = append(plan.ClaimShardIDs, lease.ShardID)
		need--
	}

	if need <= 0 || !p.EnableStealing || p.MaxLeasesToSteal <= 0 {
		return plan
	}

	stealsLeft := p.MaxLeasesToSteal
	for _, lease := range leases {
		if need <= 0 || stealsLeft <= 0 {
			break
		}
		if lease.Owner == "" || lease.Owner == p.WorkerID || isExpired(lease, p.Now) {
			continue
		}
		if ownerCounts[lease.Owner] <= target {
			continue
		}
		plan.Steals = append(plan.Steals, stealRequest{
			ShardID:    lease.ShardID,
			FromWorker: lease.Owner,
		})
		ownerCounts[lease.Owner]--
		stealsLeft--
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
