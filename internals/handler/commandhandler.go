package handler

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/VaidikKindaCodes/VoltCache/config"
	"github.com/VaidikKindaCodes/VoltCache/internals/domain"
	"github.com/VaidikKindaCodes/VoltCache/pkg/resp"
)

type CommandHandler struct {
	store       domain.Store
	leaderMgr   domain.LeaderManager
	enabledAOF  bool
	aofPath     string
	aofMu       sync.Mutex
	subscribers map[string]map[net.Conn]struct{}
	connSubs    map[net.Conn]map[string]struct{}
	subMu       sync.RWMutex
	connMu      sync.Mutex
	connWriteMu map[net.Conn]*sync.Mutex
	activeConns map[net.Conn]struct{}
	prevWrite   bool
}

func debugLog(format string, v ...interface{}) {
	fmt.Printf("[DEBUG] "+format+"\n", v...)
}

// NewCommandHandler creates a new command handler.
func NewCommandHandler(store domain.Store, leaderMgr domain.LeaderManager, cfg *config.Config) domain.CommandHandler {
	return &CommandHandler{
		store:       store,
		leaderMgr:   leaderMgr,
		enabledAOF:  cfg.AppendOnly,
		aofPath:     filepath.Join(cfg.Dir, cfg.AppendFileName),
		subscribers: make(map[string]map[net.Conn]struct{}),
		connSubs:    make(map[net.Conn]map[string]struct{}),
		connWriteMu: make(map[net.Conn]*sync.Mutex),
		activeConns: make(map[net.Conn]struct{}),
		prevWrite:   false,
	}
}

func (ch *CommandHandler) HandleClient(conn net.Conn) {
	ch.registerConn(conn)
	defer ch.unregisterConn(conn)

	reader := bufio.NewReader(conn)

	for {
		request, err := resp.ParseRESP(reader)
		if err != nil {
			if err == io.EOF {
				fmt.Printf("Connection closed by client: %s\n", conn.RemoteAddr())
			} else {
				fmt.Printf("Error reading from %s: %s\n", conn.RemoteAddr(), err.Error())
			}
			return
		}

		ch.ProcessCommand(request, conn)
	}
}

func (ch *CommandHandler) ProcessCommand(parts []string, conn net.Conn) {
	if len(parts) == 0 {
		ch.writeToConn(conn, []byte(resp.EncodeRESPError("empty command provided")))
		return
	}
	switch strings.ToUpper(parts[0]) {
	case "PING":
		conn.Write([]byte(resp.EncodeRESPSimpleString("PONG")))
	case "ECHO":
		if len(parts) < 2 {
			conn.Write([]byte(resp.EncodeRESPError("wrong number of arguments for 'echo' command")))
			return
		}
		conn.Write([]byte(resp.EncodeRESPString(parts[1])))
	case "SET":
		if len(parts) < 3 {
			conn.Write([]byte(resp.EncodeRESPError("wrong number of arguments for 'set' command")))
			return
		}
		key, value := parts[1], parts[2]
		expiration := time.Duration(-1)
		fmt.Println("parts", parts)
		if len(parts) == 5 && strings.ToUpper(parts[3]) == "PX" {
			px, err := strconv.Atoi(parts[4])
			if err != nil {
				conn.Write([]byte(resp.EncodeRESPError("invalid PX value")))
				return
			}

			expiration = time.Duration(px) * time.Millisecond
		}
		debugLog("SET command received, key: %s, value: %s, expiration: %d", key, value, expiration)
		ch.store.Set(key, value, expiration)
		ch.appendAOF(parts)
		ch.writeToConn(conn, []byte(resp.EncodeRESPSimpleString("OK")))
		if ch.leaderMgr != nil {
			ch.leaderMgr.PropagateCommand(parts)
		}
		ch.prevWrite = true
		debugLog("SET command executed, prevWrite set to true")
	case "GET":
		if len(parts) != 2 {
			conn.Write([]byte(resp.EncodeRESPError("wrong number of arguments for 'get' command")))
			return
		}
		value, exists := ch.store.Get(parts[1])
		if !exists {
			conn.Write([]byte(resp.EncodeRESPNull()))
			return
		}
		conn.Write([]byte(resp.EncodeRESPString(value)))
	case "INFO":
		ch.writeToConn(conn, []byte(ch.handleInfo(parts)))
	case "SUBSCRIBE":
		ch.handleSubscribe(parts, conn)
	case "UNSUBSCRIBE":
		ch.handleUnsubscribe(parts, conn)
	case "PUBLISH":
		ch.handlePublish(parts, conn)
	case "REPLCONF":
		if parts[1] == "ACK" {
			return
		}
		conn.Write([]byte(resp.EncodeRESPSimpleString("OK")))
	case "PSYNC":
		ch.handlePSync(parts, conn)
	case "WAIT":
		ch.handleWait(parts, conn)
	default:
		ch.writeToConn(conn, []byte(resp.EncodeRESPError("unknown command '"+parts[0]+"'")))
	}
}

func (ch *CommandHandler) handlePSync(parts []string, conn net.Conn) {
	if ch.leaderMgr == nil {
		conn.Write([]byte(resp.EncodeRESPError("PSYNC only supported by leader")))
		return
	}
	if err := ch.leaderMgr.SendFullResync(conn); err != nil {
		conn.Write([]byte(resp.EncodeRESPError(err.Error())))
		return
	}
	ch.leaderMgr.AddFollower(conn)

}

func (ch *CommandHandler) handleInfo(parts []string) string {
	var info strings.Builder
	if ch.leaderMgr != nil {
		info.WriteString("role:leader\n")
		info.WriteString(fmt.Sprintf("leader_replid:%s\n", ch.leaderMgr.GetLeaderReplID()))
		info.WriteString(fmt.Sprintf("leader_repl_offset:%d\n", ch.leaderMgr.GetLeaderReplOffset()))
	} else {
		info.WriteString("role:follower\n")
	}
	return resp.EncodeRESPString(info.String())
}

func (ch *CommandHandler) handleWait(parts []string, conn net.Conn) {
	if ch.leaderMgr == nil {
		ch.writeToConn(conn, []byte(resp.EncodeRESPError("WAIT only supported by leader")))
		return
	}

	if !ch.prevWrite {
		ch.writeToConn(conn, []byte(resp.EncodeRESPInteger(int64(ch.leaderMgr.GetFollowerCount()))))
		return
	}

	if len(parts) < 3 {
		ch.writeToConn(conn, []byte(resp.EncodeRESPError("wrong number of arguments for 'wait' command")))
		return
	}

	numReplicas, err := strconv.Atoi(parts[1])
	if err != nil {
		ch.writeToConn(conn, []byte(resp.EncodeRESPError("invalid number of replicas")))
		return
	}

	timeout, err := strconv.Atoi(parts[2])
	if err != nil {
		ch.writeToConn(conn, []byte(resp.EncodeRESPError("invalid timeout")))
		return
	}

	acks, err := ch.leaderMgr.WaitForAcknowledgments(numReplicas, timeout)
	if err != nil {
		ch.writeToConn(conn, []byte(resp.EncodeRESPError(err.Error())))
		return
	}

	ch.writeToConn(conn, []byte(resp.EncodeRESPInteger(int64(acks))))
	ch.prevWrite = false // Reset prevWrite after WAIT
	debugLog("WAIT command completed, prevWrite reset to false")
}

func (ch *CommandHandler) appendAOF(parts []string) {
	if !ch.enabledAOF {
		return
	}
	ch.aofMu.Lock()
	defer ch.aofMu.Unlock()

	file, err := os.OpenFile(ch.aofPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		debugLog("AOF append failed: %v", err)
		return
	}
	defer file.Close()

	if _, err := file.WriteString(resp.EncodeRESPArray(parts)); err != nil {
		debugLog("AOF write failed: %v", err)
	}
}

func (ch *CommandHandler) writeToConn(conn net.Conn, data []byte) {
	ch.connMu.Lock()
	m, ok := ch.connWriteMu[conn]
	if !ok {
		m = &sync.Mutex{}
		ch.connWriteMu[conn] = m
	}
	ch.connMu.Unlock()

	m.Lock()
	defer m.Unlock()
	conn.Write(data)
}

func (ch *CommandHandler) registerConn(conn net.Conn) {
	ch.connMu.Lock()
	defer ch.connMu.Unlock()
	ch.activeConns[conn] = struct{}{}
	if _, ok := ch.connWriteMu[conn]; !ok {
		ch.connWriteMu[conn] = &sync.Mutex{}
	}
}

func (ch *CommandHandler) unregisterConn(conn net.Conn) {
	ch.connMu.Lock()
	delete(ch.activeConns, conn)
	delete(ch.connWriteMu, conn)
	ch.connMu.Unlock()

	ch.subMu.Lock()
	defer ch.subMu.Unlock()
	channels, ok := ch.connSubs[conn]
	if ok {
		for channel := range channels {
			if subscribers, found := ch.subscribers[channel]; found {
				delete(subscribers, conn)
				if len(subscribers) == 0 {
					delete(ch.subscribers, channel)
				}
			}
		}
		delete(ch.connSubs, conn)
	}
}

func (ch *CommandHandler) handleSubscribe(parts []string, conn net.Conn) {
	if len(parts) < 2 {
		ch.writeToConn(conn, []byte(resp.EncodeRESPError("wrong number of arguments for 'subscribe' command")))
		return
	}

	ch.subMu.Lock()
	defer ch.subMu.Unlock()

	for _, channel := range parts[1:] {
		if ch.subscribers[channel] == nil {
			ch.subscribers[channel] = make(map[net.Conn]struct{})
		}
		ch.subscribers[channel][conn] = struct{}{}

		if ch.connSubs[conn] == nil {
			ch.connSubs[conn] = make(map[string]struct{})
		}
		ch.connSubs[conn][channel] = struct{}{}
		ch.writeToConn(conn, []byte(resp.EncodeRESPArray([]string{"subscribe", channel, fmt.Sprint(len(ch.subscribers[channel]))})))
	}
}

func (ch *CommandHandler) handleUnsubscribe(parts []string, conn net.Conn) {
	if len(parts) < 2 {
		ch.writeToConn(conn, []byte(resp.EncodeRESPError("wrong number of arguments for 'unsubscribe' command")))
		return
	}

	ch.subMu.Lock()
	defer ch.subMu.Unlock()

	for _, channel := range parts[1:] {
		if subs, ok := ch.subscribers[channel]; ok {
			delete(subs, conn)
			if len(subs) == 0 {
				delete(ch.subscribers, channel)
			}
		}
		if chans, ok := ch.connSubs[conn]; ok {
			delete(chans, channel)
			if len(chans) == 0 {
				delete(ch.connSubs, conn)
			}
		}
		ch.writeToConn(conn, []byte(resp.EncodeRESPArray([]string{"unsubscribe", channel, fmt.Sprint(len(ch.subscribers[channel]))})))
	}
}

func (ch *CommandHandler) handlePublish(parts []string, conn net.Conn) {
	if len(parts) != 3 {
		ch.writeToConn(conn, []byte(resp.EncodeRESPError("wrong number of arguments for 'publish' command")))
		return
	}

	channel, message := parts[1], parts[2]

	ch.subMu.RLock()
	subscribers, ok := ch.subscribers[channel]
	if !ok || len(subscribers) == 0 {
		ch.subMu.RUnlock()
		ch.writeToConn(conn, []byte(resp.EncodeRESPInteger(0)))
		return
	}

	copySubs := make([]net.Conn, 0, len(subscribers))
	for subscriber := range subscribers {
		copySubs = append(copySubs, subscriber)
	}
	ch.subMu.RUnlock()

	for _, subscriber := range copySubs {
		sub := subscriber
		go func() {
			payload := resp.EncodeRESPArray([]string{"message", channel, message})
			ch.writeToConn(sub, []byte(payload))
			select {
			case <-time.After(500 * time.Millisecond):
				// do not block publisher on slow subscribers
			default:
			}
			if _, err := sub.Write([]byte(payload)); err != nil {
				ch.removeSubscriber(channel, sub)
			}
		}()
	}

	ch.writeToConn(conn, []byte(resp.EncodeRESPInteger(int64(len(copySubs)))))
}

func (ch *CommandHandler) removeSubscriber(channel string, conn net.Conn) {
	ch.subMu.Lock()
	defer ch.subMu.Unlock()
	if subs, ok := ch.subscribers[channel]; ok {
		delete(subs, conn)
		if len(subs) == 0 {
			delete(ch.subscribers, channel)
		}
	}
	if chans, ok := ch.connSubs[conn]; ok {
		delete(chans, channel)
		if len(chans) == 0 {
			delete(ch.connSubs, conn)
		}
	}
}

func (ch *CommandHandler) Shutdown() {
	ch.connMu.Lock()
	for conn := range ch.activeConns {
		conn.Close()
	}
	ch.connMu.Unlock()
}
