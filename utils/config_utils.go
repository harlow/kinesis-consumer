package utils

import (
	"bufio"
	"log"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unicode"
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

func LoadConfig(config interface{}, filename string) error {
	lines, err := readLines(filename)

	if err != nil {
		log.Fatalf("Load error: %s", err)
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
			fieldType := mutable.FieldByName(key).Type()

			switch fieldType.Name() {
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
