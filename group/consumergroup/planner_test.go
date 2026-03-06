package consumergroup

import (
	"reflect"
	"testing"
	"time"
)

func TestTargetLeaseCount(t *testing.T) {
	cases := []struct {
		name               string
		totalLeases        int
		activeWorkers      int
		maxLeasesForWorker int
		want               int
	}{
		{
			name:               "single worker gets all leases",
			totalLeases:        10,
			activeWorkers:      1,
			maxLeasesForWorker: 0,
			want:               10,
		},
		{
			name:               "two workers split evenly",
			totalLeases:        10,
			activeWorkers:      2,
			maxLeasesForWorker: 0,
			want:               5,
		},
		{
			name:               "odd shard count rounds up",
			totalLeases:        10,
			activeWorkers:      3,
			maxLeasesForWorker: 0,
			want:               4,
		},
		{
			name:               "max leases caps target",
			totalLeases:        10,
			activeWorkers:      2,
			maxLeasesForWorker: 3,
			want:               3,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := targetLeaseCount(tc.totalLeases, tc.activeWorkers, tc.maxLeasesForWorker)
			if got != tc.want {
				t.Fatalf("targetLeaseCount() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestAssignmentPlanner_UnownedThenExpiredThenSteal(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()

	leases := []leaseState{
		{ShardID: "s0"},
		{ShardID: "s1", Owner: "worker-a", ExpiresAt: now.Add(-time.Second)},
		{ShardID: "s2", Owner: "worker-a", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s3", Owner: "worker-a", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s4", Owner: "worker-a", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s5", Owner: "worker-a", ExpiresAt: now.Add(time.Minute)},
	}

	p := assignmentPlanner{
		WorkerID:           "worker-b",
		Now:                now,
		MaxLeasesForWorker: 0,
		MaxLeasesToSteal:   1,
		EnableStealing:     true,
	}

	plan := p.Plan(leases, []string{"worker-a", "worker-b"})

	wantClaims := []string{"s0", "s1"}
	if !reflect.DeepEqual(plan.ClaimShardIDs, wantClaims) {
		t.Fatalf("ClaimShardIDs = %v, want %v", plan.ClaimShardIDs, wantClaims)
	}

	if len(plan.Steals) != 1 {
		t.Fatalf("len(Steals) = %d, want %d", len(plan.Steals), 1)
	}
	if plan.Steals[0].ShardID != "s2" {
		t.Fatalf("steal shard = %q, want %q", plan.Steals[0].ShardID, "s2")
	}
	if plan.Steals[0].FromWorker != "worker-a" {
		t.Fatalf("steal from = %q, want %q", plan.Steals[0].FromWorker, "worker-a")
	}
}

func TestAssignmentPlanner_DisabledStealing(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()

	leases := []leaseState{
		{ShardID: "s0", Owner: "worker-a", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s1", Owner: "worker-a", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s2", Owner: "worker-a", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s3", Owner: "worker-b", ExpiresAt: now.Add(time.Minute)},
	}

	p := assignmentPlanner{
		WorkerID:           "worker-b",
		Now:                now,
		MaxLeasesForWorker: 0,
		MaxLeasesToSteal:   2,
		EnableStealing:     false,
	}

	plan := p.Plan(leases, []string{"worker-a", "worker-b"})
	if len(plan.Steals) != 0 {
		t.Fatalf("len(Steals) = %d, want 0", len(plan.Steals))
	}
}

func TestAssignmentPlanner_RespectsMaxSteals(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()

	leases := []leaseState{
		{ShardID: "s0", Owner: "worker-a", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s1", Owner: "worker-a", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s2", Owner: "worker-a", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s3", Owner: "worker-a", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s4", Owner: "worker-b", ExpiresAt: now.Add(time.Minute)},
	}

	p := assignmentPlanner{
		WorkerID:           "worker-b",
		Now:                now,
		MaxLeasesForWorker: 0,
		MaxLeasesToSteal:   1,
		EnableStealing:     true,
	}

	plan := p.Plan(leases, []string{"worker-a", "worker-b"})
	if len(plan.Steals) != 1 {
		t.Fatalf("len(Steals) = %d, want 1", len(plan.Steals))
	}
}
