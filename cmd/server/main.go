package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/VaidikKindaCodes/VoltCache/config"
	"github.com/VaidikKindaCodes/VoltCache/internals/domain"
	"github.com/VaidikKindaCodes/VoltCache/internals/handler"
	"github.com/VaidikKindaCodes/VoltCache/internals/rdb"
	"github.com/VaidikKindaCodes/VoltCache/internals/replication"
	"github.com/VaidikKindaCodes/VoltCache/internals/storage"
	"github.com/VaidikKindaCodes/VoltCache/pkg/resp"
)

func main() {
	configPath := flag.String("config", "", "Path to Redis-style configuration file")
	port := flag.String("port", "6379", "Port to run the Redis server on")
	replicaof := flag.String("replicaof", "", "Replicate another Redis server")
	rdbFileDir := flag.String("dir", "", "Directory to store RDB file")
	rdbFileName := flag.String("dbfilename", "", "Name of the RDB file")
	appendOnly := flag.Bool("appendonly", false, "Enable AOF persistence")
	appendFileName := flag.String("appendfilename", "", "AOF filename")

	flag.Parse()

	cfg := config.NewConfig()
	if *configPath != "" {
		loaded, err := config.LoadConfig(*configPath)
		if err != nil {
			fmt.Printf("Failed to load config file: %v\n", err)
			os.Exit(1)
		}
		cfg = loaded
	}

	if *port != "6379" {
		cfg.Port = *port
	}
	if *replicaof != "" {
		cfg.ReplicaOf = *replicaof
	}
	if *rdbFileDir != "" {
		cfg.Dir = *rdbFileDir
	}
	if *rdbFileName != "" {
		cfg.DBFileName = *rdbFileName
	}
	if *appendOnly {
		cfg.AppendOnly = true
	}
	if *appendFileName != "" {
		cfg.AppendFileName = *appendFileName
	}

	if cfg.Dir == "" {
		cfg.Dir = "."
	}

	store := storage.NewInMemoryStore()
	rdbFilePath := filepath.Join(cfg.Dir, cfg.DBFileName)
	aofFilePath := filepath.Join(cfg.Dir, cfg.AppendFileName)

	if cfg.AppendOnly {
		if _, err := os.Stat(aofFilePath); err == nil {
			if err := loadAOF(aofFilePath, store); err != nil {
				fmt.Printf("Error loading AOF file: %v\n", err)
			}
		}
	} else if _, err := os.Stat(rdbFilePath); err == nil {
		if err := rdb.LoadRDBFile(rdbFilePath, store); err != nil {
			fmt.Printf("Error loading RDB file: %v\n", err)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	listener, err := net.Listen("tcp", "0.0.0.0:"+cfg.Port)
	if err != nil {
		fmt.Println("Failed to bind to port", cfg.Port)
		os.Exit(1)
	}

	var leaderMgr domain.LeaderManager
	if cfg.ReplicaOf == "" {
		fmt.Println("Starting as Leader")
		leaderMgr = replication.NewLeader()
	} else {
		fmt.Println("Starting as Follower")
	}

	commandHandler := handler.NewCommandHandler(store, leaderMgr, cfg)
	go func() {
		<-ctx.Done()
		fmt.Println("Shutting down server...")
		listener.Close()
		commandHandler.Shutdown()
		if cfg.AppendOnly {
			if err := appendFlush(aofFilePath); err != nil {
				fmt.Printf("Error flushing AOF: %v\n", err)
			}
		}
		if !cfg.AppendOnly {
			if err := saveRDB(rdbFilePath, store); err != nil {
				fmt.Printf("Error saving RDB: %v\n", err)
			}
		}
		os.Exit(0)
	}()

	if cfg.ReplicaOf == "" {
		startServer(listener, commandHandler)
	} else {
		leaderInfo := strings.Fields(cfg.ReplicaOf)
		leaderHost, leaderPort := leaderInfo[0], leaderInfo[1]
		followerManager := replication.NewFollower(store, cfg.Port, leaderHost, leaderPort, commandHandler)

		if err := followerManager.ConnectToLeader(); err != nil {
			fmt.Printf("Failed to connect to leader: %v\n", err)
		}

		go followerManager.ReceiveAndProcessCommands()
		startServer(listener, commandHandler)
	}
}

func startServer(listener net.Listener, handler domain.CommandHandler) {
	fmt.Printf("Server listening on port %d\n", listener.Addr().(*net.TCPAddr).Port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && opErr.Err.Error() == "use of closed network connection" {
				return
			}
			fmt.Println("Error:", err)
			continue
		}
		go handler.HandleClient(conn)
	}
}

func saveRDB(path string, store domain.Store) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	entries := store.Entries()
	if _, err := file.Write([]byte("REDIS0009\n")); err != nil {
		return err
	}
	for key, entry := range entries {
		if entry.Expiration != nil && time.Now().After(*entry.Expiration) {
			continue
		}
		if _, err := file.Write([]byte{0x00}); err != nil {
			return err
		}
		if err := writeString(file, key); err != nil {
			return err
		}
		if err := writeString(file, entry.Value); err != nil {
			return err
		}
	}
	if _, err := file.Write([]byte{0xFF}); err != nil {
		return err
	}
	return nil
}

func loadAOF(path string, store domain.Store) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	for {
		parts, err := resp.ParseRESP(reader)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		switch strings.ToUpper(parts[0]) {
		case "SET":
			if len(parts) >= 3 {
				expiration := time.Duration(-1)
				if len(parts) == 5 && strings.ToUpper(parts[3]) == "PX" {
					px, err := strconv.Atoi(parts[4])
					if err == nil {
						expiration = time.Duration(px) * time.Millisecond
					}
				}
				store.Set(parts[1], parts[2], expiration)
			}
		}
	}
}

func appendFlush(path string) error {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	return file.Close()
}

func writeString(file *os.File, value string) error {
	length := uint64(len(value))
	b := []byte{byte((length >> 56) & 0xFF), byte((length >> 48) & 0xFF), byte((length >> 40) & 0xFF), byte((length >> 32) & 0xFF), byte((length >> 24) & 0xFF), byte((length >> 16) & 0xFF), byte((length >> 8) & 0xFF), byte(length & 0xFF)}
	if _, err := file.Write(b); err != nil {
		return err
	}
	if _, err := file.Write([]byte(value)); err != nil {
		return err
	}
	return nil
}
