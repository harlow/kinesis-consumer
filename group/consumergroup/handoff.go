package consumergroup

import "time"

func handoffActive(pendingOwner string, deadline, now time.Time) bool {
	if pendingOwner == "" {
		return false
	}
	if deadline.IsZero() {
		return true
	}
	return deadline.After(now)
}

func handoffPendingForOtherWorker(pendingOwner string, deadline, now time.Time, workerID string) bool {
	if pendingOwner == "" || pendingOwner == workerID {
		return false
	}
	return handoffActive(pendingOwner, deadline, now)
}
