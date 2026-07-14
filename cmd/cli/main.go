package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/VaidikKindaCodes/VoltCache/pkg/resp"
)

const defaultAddress = "127.0.0.1:6379"

type respValue struct {
	kind  byte
	text  string
	num   int64
	items []respValue
	null  bool
}

func main() {
	host := flag.String("host", "127.0.0.1", "Redis server host")
	port := flag.String("port", "6379", "Redis server port")
	flag.Parse()

	address := net.JoinHostPort(*host, *port)
	if address == ":" {
		address = defaultAddress
	}

	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not connect to %s: %v\n", address, err)
		os.Exit(1)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	args := flag.Args()
	if len(args) > 0 {
		args = normalizeCommand(args)
		switch strings.ToUpper(args[0]) {
		case "SUBSCRIBE":
			if err := sendCommand(conn, args); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			for {
				value, err := readRESPValue(reader)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				printRESPValue(value, 0)
			}
		default:
			if err := sendAndPrint(conn, reader, args); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}
		return
	}

	fmt.Printf("Connected to %s\n", address)
	fmt.Println("Type HELP for commands, QUIT to exit.")
	if err := runInteractive(conn, reader, address); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runInteractive(conn net.Conn, reader *bufio.Reader, address string) error {
	messageErr := make(chan error, 1)
	go func() {
		for {
			value, err := readRESPValue(reader)
			if err != nil {
				messageErr <- err
				return
			}
			printRESPValue(value, 0)
		}
	}()

	input := bufio.NewReader(os.Stdin)
	for {
		select {
		case err := <-messageErr:
			return err
		default:
		}

		fmt.Printf("%s> ", address)
		line, err := input.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				fmt.Println()
				return nil
			}
			return err
		}

		parts, err := splitCommandLine(strings.TrimSpace(line))
		if err != nil {
			fmt.Printf("(error) %v\n", err)
			continue
		}
		if len(parts) == 0 {
			continue
		}

		parts = normalizeCommand(parts)
		switch strings.ToUpper(parts[0]) {
		case "QUIT", "EXIT":
			return nil
		case "HELP":
			printHelp()
			continue
		case "SUBSCRIBE":
			if err := sendCommand(conn, parts); err != nil {
				fmt.Printf("(error) %v\n", err)
			}
			continue
		default:
			if err := sendAndPrint(conn, reader, parts); err != nil {
				fmt.Printf("(error) %v\n", err)
			}
		}
	}
}

func sendCommand(conn net.Conn, parts []string) error {
	if _, err := conn.Write([]byte(resp.EncodeRESPArray(parts))); err != nil {
		return fmt.Errorf("could not send command: %w", err)
	}
	return nil
}

func sendAndPrint(conn net.Conn, reader *bufio.Reader, parts []string) error {
	if _, err := conn.Write([]byte(resp.EncodeRESPArray(parts))); err != nil {
		return fmt.Errorf("could not send command: %w", err)
	}

	value, err := readRESPValue(reader)
	if err != nil {
		return fmt.Errorf("could not read response: %w", err)
	}
	printRESPValue(value, 0)
	return nil
}

func normalizeCommand(parts []string) []string {
	if len(parts) == 0 {
		return parts
	}
	cmd := strings.ToUpper(parts[0])
	if cmd == "PUB" {
		parts[0] = "PUBLISH"
	} else if cmd == "SUB" {
		parts[0] = "SUBSCRIBE"
	} else if cmd == "UNSUB" {
		parts[0] = "UNSUBSCRIBE"
	}
	return parts
}

func readRESPValue(reader *bufio.Reader) (respValue, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return respValue{}, err
	}

	switch prefix {
	case '+':
		line, err := readLine(reader)
		return respValue{kind: prefix, text: line}, err
	case '-':
		line, err := readLine(reader)
		return respValue{kind: prefix, text: line}, err
	case ':':
		line, err := readLine(reader)
		if err != nil {
			return respValue{}, err
		}
		n, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return respValue{}, fmt.Errorf("invalid integer response %q", line)
		}
		return respValue{kind: prefix, num: n}, nil
	case '$':
		line, err := readLine(reader)
		if err != nil {
			return respValue{}, err
		}
		length, err := strconv.Atoi(line)
		if err != nil {
			return respValue{}, fmt.Errorf("invalid bulk string length %q", line)
		}
		if length == -1 {
			return respValue{kind: prefix, null: true}, nil
		}
		buf := make([]byte, length+2)
		if _, err := io.ReadFull(reader, buf); err != nil {
			return respValue{}, err
		}
		return respValue{kind: prefix, text: string(buf[:length])}, nil
	case '*':
		line, err := readLine(reader)
		if err != nil {
			return respValue{}, err
		}
		count, err := strconv.Atoi(line)
		if err != nil {
			return respValue{}, fmt.Errorf("invalid array length %q", line)
		}
		if count == -1 {
			return respValue{kind: prefix, null: true}, nil
		}
		items := make([]respValue, 0, count)
		for i := 0; i < count; i++ {
			item, err := readRESPValue(reader)
			if err != nil {
				return respValue{}, err
			}
			items = append(items, item)
		}
		return respValue{kind: prefix, items: items}, nil
	default:
		return respValue{}, fmt.Errorf("unknown RESP response type %q", prefix)
	}
}

func readLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r"), nil
}

func printRESPValue(value respValue, indent int) {
	prefix := strings.Repeat("  ", indent)
	switch value.kind {
	case '+':
		fmt.Println(prefix + value.text)
	case '-':
		fmt.Println(prefix + "(error) " + value.text)
	case ':':
		fmt.Printf("%s(integer) %d\n", prefix, value.num)
	case '$':
		if value.null {
			fmt.Println(prefix + "(nil)")
			return
		}
		fmt.Println(prefix + value.text)
	case '*':
		if value.null {
			fmt.Println(prefix + "(nil)")
			return
		}
		if len(value.items) == 0 {
			fmt.Println(prefix + "(empty array)")
			return
		}
		for i, item := range value.items {
			fmt.Printf("%s%d) ", prefix, i+1)
			if item.kind == '*' {
				fmt.Println()
				printRESPValue(item, indent+1)
				continue
			}
			printRESPValue(item, 0)
		}
	}
}

func splitCommandLine(line string) ([]string, error) {
	var parts []string
	var current strings.Builder
	var quote rune
	escaped := false

	for _, r := range line {
		if escaped {
			current.WriteRune(r)
			escaped = false
			continue
		}

		if r == '\\' {
			escaped = true
			continue
		}

		if quote != 0 {
			if r == quote {
				quote = 0
				continue
			}
			current.WriteRune(r)
			continue
		}

		if r == '"' || r == '\'' {
			quote = r
			continue
		}

		if r == ' ' || r == '\t' {
			if current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
			continue
		}

		current.WriteRune(r)
	}

	if escaped {
		current.WriteRune('\\')
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated quoted string")
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts, nil
}

func printHelp() {
	fmt.Println("Supported server commands:")
	fmt.Println("  PING")
	fmt.Println("  ECHO <message>")
	fmt.Println("  SET <key> <value> [PX milliseconds]")
	fmt.Println("  GET <key>")
	fmt.Println("  INFO")
	fmt.Println("  SUBSCRIBE <channel>")
	fmt.Println("  UNSUBSCRIBE <channel>")
	fmt.Println("  PUBLISH <channel> <message>")
	fmt.Println("  PUB <channel> <message>")
	fmt.Println("  SUB <channel>")
	fmt.Println("  UNSUB <channel>")
	fmt.Println("  WAIT <replicas> <timeout-ms>")
	fmt.Println("  REPLCONF <option> [value]")
	fmt.Println("  PSYNC <replication-id> <offset>")
	fmt.Println()
	fmt.Println("The CLI sends any command you type, so newer server commands will also work.")
}
