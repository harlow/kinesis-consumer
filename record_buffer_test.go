package connector

import "testing"

type TestModel struct{}

func (u TestModel) ToString() string {
	return "ok"
}

func TestAdd(t *testing.T) {
	var r1, s1 = TestModel{}, "Seq1"
	var r2, s2 = TestModel{}, "Seq2"

	b := RecordBuffer{}
	b.Add(r1, s1)

	if b.NumRecordsInBuffer() != 1 {
		t.Errorf("NumRecordsInBuffer() want %v", 1)
	}

	b.Add(r2, s2)

	if b.NumRecordsInBuffer() != 2 {
		t.Errorf("NumRecordsInBuffer() want %v", 2)
	}

	b.Add(r2, s2)

	if b.NumRecordsInBuffer() != 2 {
		t.Errorf("NumRecordsInBuffer() want %v", 2)
	}
}

func TestSequenceExists(t *testing.T) {
	var r1, s1 = TestModel{}, "Seq1"
	var r2, s2 = TestModel{}, "Seq2"

	b := RecordBuffer{}
	b.Add(r1, s1)

	if b.sequenceExists(s1) != true {
		t.Errorf("sequenceExists() want %v", true)
	}

	b.Add(r2, s2)

	if b.sequenceExists(s2) != true {
		t.Errorf("sequenceExists() want %v", true)
	}
}

func TestFlush(t *testing.T) {
	var r1, s1 = TestModel{}, "SeqNum"
	b := RecordBuffer{}
	b.Add(r1, s1)

	b.Flush()

	if b.NumRecordsInBuffer() != 0 {
		t.Errorf("Count() want %v", 0)
	}
}

func TestLastSequenceNumber(t *testing.T) {
	var r1, s1 = TestModel{}, "Seq1"
	var r2, s2 = TestModel{}, "Seq2"

	b := RecordBuffer{}
	b.Add(r1, s1)

	if b.LastSequenceNumber() != s1 {
		t.Errorf("LastSequenceNumber() want %v", s1)
	}

	b.Add(r2, s2)

	if b.LastSequenceNumber() != s2 {
		t.Errorf("LastSequenceNumber() want %v", s2)
	}
}

func TestFirstSequenceNumber(t *testing.T) {
	var r1, s1 = TestModel{}, "Seq1"
	var r2, s2 = TestModel{}, "Seq2"

	b := RecordBuffer{}
	b.Add(r1, s1)

	if b.FirstSequenceNumber() != s1 {
		t.Errorf("FirstSequenceNumber() want %v", s1)
	}

	b.Add(r2, s2)

	if b.FirstSequenceNumber() != s1 {
		t.Errorf("FirstSequenceNumber() want %v", s1)
	}
}

func TestShouldFlush(t *testing.T) {
	const n = 2
	var r1, s1 = TestModel{}, "Seq1"
	var r2, s2 = TestModel{}, "Seq2"

	b := RecordBuffer{NumRecordsToBuffer: n}
	b.Add(r1, s1)

	if b.ShouldFlush() != false {
		t.Errorf("ShouldFlush() want %v", false)
	}

	b.Add(r2, s2)

	if b.ShouldFlush() != true {
		t.Errorf("ShouldFlush() want %v", true)
	}
}
