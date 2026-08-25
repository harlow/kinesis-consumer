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

func TestAssignmentPlanner_UnownedThenExpired(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()

	leases := []Lease{
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
	}

	plan := p.Plan(leases, []string{"worker-a", "worker-b"})

	wantClaims := []string{"s0", "s1"}
	if !reflect.DeepEqual(plan.ClaimShardIDs, wantClaims) {
		t.Fatalf("ClaimShardIDs = %v, want %v", plan.ClaimShardIDs, wantClaims)
	}
}

func TestAssignmentPlanner_RequestsHandoffFromOverloadedWorker(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()

	leases := []Lease{
		{ShardID: "s0", Owner: "worker-a", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s1", Owner: "worker-a", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s2", Owner: "worker-a", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s3", Owner: "worker-a", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s4", Owner: "worker-a", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s5", Owner: "worker-a", ExpiresAt: now.Add(time.Minute)},
	}

	p := assignmentPlanner{
		WorkerID:           "worker-b",
		Now:                now,
		MaxLeasesForWorker: 0,
	}

	plan := p.Plan(leases, []string{"worker-a", "worker-b"})

	if len(plan.RenewShardIDs) != 0 {
		t.Fatalf("RenewShardIDs = %v, want none", plan.RenewShardIDs)
	}

	wantHandoffs := []handoffRequest{
		{ShardID: "s0", FromWorkerID: "worker-a"},
		{ShardID: "s1", FromWorkerID: "worker-a"},
		{ShardID: "s2", FromWorkerID: "worker-a"},
	}
	if !reflect.DeepEqual(plan.HandoffRequests, wantHandoffs) {
		t.Fatalf("HandoffRequests = %v, want %v", plan.HandoffRequests, wantHandoffs)
	}
	if len(plan.ClaimShardIDs) != 0 {
		t.Fatalf("ClaimShardIDs = %v, want none", plan.ClaimShardIDs)
	}
}

func TestAssignmentPlanner_WaitsForCompletedParents(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()

	leases := []Lease{
		{ShardID: "parent"},
		{ShardID: "child", ParentShardID: "parent"},
	}

	p := assignmentPlanner{
		WorkerID:           "worker-a",
		Now:                now,
		MaxLeasesForWorker: 0,
	}

	plan := p.Plan(leases, []string{"worker-a"})
	if !reflect.DeepEqual(plan.ClaimShardIDs, []string{"parent"}) {
		t.Fatalf("ClaimShardIDs = %v, want [parent]", plan.ClaimShardIDs)
	}

	leases[0].Completed = true
	plan = p.Plan(leases, []string{"worker-a"})
	if !reflect.DeepEqual(plan.ClaimShardIDs, []string{"child"}) {
		t.Fatalf("ClaimShardIDs = %v, want [child]", plan.ClaimShardIDs)
	}
}

func TestAssignmentPlanner_WaitsForBothMergeParents(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()

	leases := []Lease{
		{ShardID: "left", Completed: true},
		{ShardID: "right"},
		{ShardID: "merged", ParentShardID: "left", AdjacentParentID: "right"},
	}

	p := assignmentPlanner{
		WorkerID:           "worker-a",
		Now:                now,
		MaxLeasesForWorker: 0,
	}

	plan := p.Plan(leases, []string{"worker-a"})
	if !reflect.DeepEqual(plan.ClaimShardIDs, []string{"right"}) {
		t.Fatalf("ClaimShardIDs = %v, want [right]", plan.ClaimShardIDs)
	}

	leases[1].Completed = true
	plan = p.Plan(leases, []string{"worker-a"})
	if !reflect.DeepEqual(plan.ClaimShardIDs, []string{"merged"}) {
		t.Fatalf("ClaimShardIDs = %v, want [merged]", plan.ClaimShardIDs)
	}
}
