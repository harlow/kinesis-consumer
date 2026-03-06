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
	}

	plan := p.Plan(leases, []string{"worker-a", "worker-b"})

	wantClaims := []string{"s0", "s1"}
	if !reflect.DeepEqual(plan.ClaimShardIDs, wantClaims) {
		t.Fatalf("ClaimShardIDs = %v, want %v", plan.ClaimShardIDs, wantClaims)
	}
}

func TestAssignmentPlanner_ReleasesExcessOwnedLeases(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()

	leases := []leaseState{
		{ShardID: "s0", Owner: "worker-b", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s1", Owner: "worker-b", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s2", Owner: "worker-b", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s3", Owner: "worker-b", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s4", Owner: "worker-a", ExpiresAt: now.Add(time.Minute)},
		{ShardID: "s5", Owner: "worker-a", ExpiresAt: now.Add(time.Minute)},
	}

	p := assignmentPlanner{
		WorkerID:           "worker-b",
		Now:                now,
		MaxLeasesForWorker: 0,
	}

	plan := p.Plan(leases, []string{"worker-a", "worker-b"})

	wantRenew := []string{"s0", "s1", "s2"}
	if !reflect.DeepEqual(plan.RenewShardIDs, wantRenew) {
		t.Fatalf("RenewShardIDs = %v, want %v", plan.RenewShardIDs, wantRenew)
	}

	wantRelease := []string{"s3"}
	if !reflect.DeepEqual(plan.ReleaseShardIDs, wantRelease) {
		t.Fatalf("ReleaseShardIDs = %v, want %v", plan.ReleaseShardIDs, wantRelease)
	}
	if len(plan.ClaimShardIDs) != 0 {
		t.Fatalf("ClaimShardIDs = %v, want none", plan.ClaimShardIDs)
	}
}
