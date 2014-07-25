package etl

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

func TestNumMessagesToBuffer(t *testing.T) {
	const n = 25
	b := MsgBuffer{numMessagesToBuffer: n}
	r := b.NumMessagesToBuffer()

	if r != n {
		t.Errorf("NumMessagesToBuffer() = %v, want %v", r, n)
	}
}

func TestConsumeRecord(t *testing.T) {
	var r1, s1 = []byte("Record1"), "Seq1"
	var r2, s2 = []byte("Recrod2"), "Seq2"

	b := MsgBuffer{}
	b.ConsumeRecord(r1, s1)

	if b.NumMessagesInBuffer() != 1 {
		t.Errorf("NumRecordsInBuffer() want %v", 1)
	}

	b.ConsumeRecord(r2, s2)

	if b.NumMessagesInBuffer() != 2 {
		t.Errorf("NumMessagesInBuffer() want %v", 2)
	}

	b.ConsumeRecord(r2, s2)

	if b.NumMessagesInBuffer() != 2 {
		t.Errorf("NumMessagesInBuffer() want %v", 2)
	}
}

func TestSequenceExists(t *testing.T) {
	var r1, s1 = []byte("Record1"), "Seq1"
	var r2, s2 = []byte("Recrod2"), "Seq2"

	b := MsgBuffer{}
	b.ConsumeRecord(r1, s1)

	if b.SequenceExists(s1) != true {
		t.Errorf("SequenceExists() want %v", true)
	}

	b.ConsumeRecord(r2, s2)

	if b.SequenceExists(s2) != true {
		t.Errorf("SequenceExists() want %v", true)
	}
}

func TestFlushBuffer(t *testing.T) {
	var r1, s1 = []byte("Record"), "SeqNum"
	b := MsgBuffer{}
	b.ConsumeRecord(r1, s1)

	b.FlushBuffer()

	if b.NumMessagesInBuffer() != 0 {
		t.Errorf("NumMessagesInBuffer() want %v", 0)
	}
}

func TestLastSequenceNumber(t *testing.T) {
	var r1, s1 = []byte("Record1"), "Seq1"
	var r2, s2 = []byte("Recrod2"), "Seq2"

	b := MsgBuffer{}
	b.ConsumeRecord(r1, s1)

	if b.LastSequenceNumber() != s1 {
		t.Errorf("LastSequenceNumber() want %v", s1)
	}

	b.ConsumeRecord(r2, s2)

	if b.LastSequenceNumber() != s2 {
		t.Errorf("LastSequenceNumber() want %v", s2)
	}
}

func TestFirstSequenceNumber(t *testing.T) {
	var r1, s1 = []byte("Record1"), "Seq1"
	var r2, s2 = []byte("Recrod2"), "Seq2"

	b := MsgBuffer{}
	b.ConsumeRecord(r1, s1)

	if b.FirstSequenceNumber() != s1 {
		t.Errorf("FirstSequenceNumber() want %v", s1)
	}

	b.ConsumeRecord(r2, s2)

	if b.FirstSequenceNumber() != s1 {
		t.Errorf("FirstSequenceNumber() want %v", s1)
	}
}

func TestShouldFlush(t *testing.T) {
	const n = 2
	var r1, s1 = []byte("Record1"), "Seq1"
	var r2, s2 = []byte("Recrod2"), "Seq2"

	b := MsgBuffer{numMessagesToBuffer: n}
	b.ConsumeRecord(r1, s1)

	if b.ShouldFlush() != false {
		t.Errorf("ShouldFlush() want %v", false)
	}

	b.ConsumeRecord(r2, s2)

	if b.ShouldFlush() != true {
		t.Errorf("ShouldFlush() want %v", true)
	}
}

func TestData(t *testing.T) {
	var r1, s1 = []byte("Record1\n"), "Seq1"
	var r2, s2 = []byte("Record2\n"), "Seq2"
	var body = []byte("Record1\nRecord2\n")

	b := MsgBuffer{}
	b.ConsumeRecord(r1, s1)
	b.ConsumeRecord(r2, s2)

	if !bytes.Equal(b.Data(), body) {
		t.Errorf("Data() want %v", body)
	}
}

func TestFileName(t *testing.T) {
	var r1, s1 = []byte("Record1"), "Seq1"
	var r2, s2 = []byte("Record2"), "Seq2"
	date := time.Now().UTC().Format("2006-01-02")
	name := fmt.Sprintf("/%v/Seq1-Seq2.txt", date)

	b := MsgBuffer{}
	b.ConsumeRecord(r1, s1)
	b.ConsumeRecord(r2, s2)

	if b.FileName() != name {
		t.Errorf("FileName() = want %v", b.FileName(), name)
	}
}
