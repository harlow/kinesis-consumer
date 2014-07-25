package connector

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/sendgridlabs/go-kinesis"
)

func readLines(path string) ([]string, error) {
	file, err := os.Open(path)

	if err != nil {
		return nil, err
	}

	defer file.Close()
	var lines []string

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	return lines, scanner.Err()
}

var (
	assignRegex = regexp.MustCompile(`^([^=]+)=(.*)$`)
)

func upcaseInitial(str string) string {
	for i, v := range str {
		return string(unicode.ToUpper(v)) + str[i+1:]
	}

	return ""
}

// Opens the file path and loads config values into the sturct.
func LoadConfig(config interface{}, filename string) error {
	lines, err := readLines(filename)

	if err != nil {
		log.Fatalf("Config Load ERROR: %s\n", err)
	}

	mutable := reflect.ValueOf(config).Elem()

	for _, line := range lines {
		line = strings.TrimSpace(line)

		if len(line) == 0 || line[0] == ';' || line[0] == '#' {
			continue
		}

		if groups := assignRegex.FindStringSubmatch(line); groups != nil {
			key, val := groups[1], groups[2]
			key, val = strings.TrimSpace(key), strings.TrimSpace(val)
			key = upcaseInitial(key)
			field := mutable.FieldByName(key)

			if !field.IsValid() {
				log.Fatalf("Config ERROR: Field %s not found\n", key)
			}

			switch field.Type().Name() {
			case "int":
				val, _ := strconv.ParseInt(val, 0, 64)
				mutable.FieldByName(key).SetInt(val)
			case "bool":
				val, _ := strconv.ParseBool(val)
				mutable.FieldByName(key).SetBool(val)
			default:
				mutable.FieldByName(key).SetString(val)
			}
		}
	}

	return nil
}

// Creates a new Kinesis stream (uses existing stream if exists) and waits for it to become available.
func CreateAndWaitForStreamToBecomeAvailable(k *kinesis.Kinesis, streamName string, shardCount int) {
	if !StreamExists(k, streamName) {
		err := k.CreateStream(streamName, shardCount)

		if err != nil {
			fmt.Printf("CreateStream ERROR: %v\n", err)
			return
		}
	}

	resp := &kinesis.DescribeStreamResp{}
	timeout := make(chan bool, 30)

	for {
		args := kinesis.NewArgs()
		args.Add("StreamName", streamName)
		resp, _ = k.DescribeStream(args)
		streamStatus := resp.StreamDescription.StreamStatus
		fmt.Printf("Stream [%v] is %v\n", streamName, streamStatus)

		if streamStatus != "ACTIVE" {
			time.Sleep(4 * time.Second)
			timeout <- true
		} else {
			break
		}
	}
}

// Check if a Kinesis stream exists.
func StreamExists(k *kinesis.Kinesis, streamName string) bool {
	args := kinesis.NewArgs()
	resp, _ := k.ListStreams(args)
	for _, s := range resp.StreamNames {
		if s == streamName {
			return true
		}
	}
	return false
}

// Delete a Kinesis stream.
func DeleteStream(k *kinesis.Kinesis, streamName string) {
	err := k.DeleteStream("test")

	if err != nil {
		fmt.Printf("DeleteStream ERROR: %v\n", err)
		return
	}

	fmt.Printf("Stream [%v] is DELETING\n", streamName)
}
