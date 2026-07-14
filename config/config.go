package config

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type Config struct {
	Port           string
	ReplicaOf      string
	Dir            string
	DBFileName     string
	AppendOnly     bool
	AppendFileName string
}

func NewConfig() *Config {
	return &Config{
		Port:           "6379",
		Dir:            ".",
		DBFileName:     "dump.rdb",
		AppendOnly:     false,
		AppendFileName: "appendonly.aof",
	}
}

func LoadConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	cfg := NewConfig()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}

		key := strings.ToLower(parts[0])
		switch key {
		case "port":
			cfg.Port = parts[1]
		case "dir":
			cfg.Dir = parts[1]
		case "dbfilename":
			cfg.DBFileName = parts[1]
		case "appendonly":
			cfg.AppendOnly = strings.EqualFold(parts[1], "yes") || strings.EqualFold(parts[1], "true")
		case "appendfilename":
			cfg.AppendFileName = parts[1]
		case "replicaof":
			if len(parts) >= 3 {
				cfg.ReplicaOf = parts[1] + " " + parts[2]
			} else {
				cfg.ReplicaOf = parts[1]
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	return cfg, nil
}
