package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TODO: log reconciliation
// TODO: shift all constants, specific to cluster to config.json
// TODO: make one timer function, use reflection of a function -> cleaner code?

// TODO: instead of passing Meta object through a bunch of routines, use channels to pass pointer to meta struct?
type Meta struct {
	identity        string
	value           int
	leader          string
	state           string   // whether node is LEADER, CANDIDATE or FOLLOWER
	connection_pool sync.Map // map[string]*Connection
	commit_log      *CommitLog
	quorum          *Quorum
	timeout_ch      chan string
	close_quorum_ch chan string
	new_leader_ch   chan string //notify probe_beat and timeout routines of new leader
	heart_beat      *HeartBeat
	term            int
	voted           bool //represents if node voted this term
}

type Connection struct {
	conn          net.Conn
	port          string
	last_response int  // how many heartbeats, it last responded
	dead          bool // is it dead?
}

type HeartBeat struct {
	original int
	timeout  int
}

type Quorum struct {
	quorum_type string //log_replication, election( LOGREPL, ELECTION )
	votes       int
	timeout     int
}

type CommitLog struct {
	staged     string // the current uncommited command
	commits    []*Log
	prev_index int
	prev_term  int
}

type Log struct {
	index   int
	term    int // which term was this commited?
	command string
}

func (cl *CommitLog) commit(meta *Meta) {
	// only if a change is staged, perform commit
	if cl.staged != "" {
		parts := strings.Fields(cl.staged)
		value, _ := strconv.Atoi(parts[1])
		value, err := meta.apply_command(parts[0], value)
		if err != nil {
			fmt.Printf("Error Commiting:", err)
			return
		}

		log := &Log{index: cl.prev_index + 1, term: meta.term, command: cl.staged}
		cl.commits = append(cl.commits, log)
		meta.value = value
		cl.prev_index += 1
		cl.staged = ""
	}
}

func (cl *CommitLog) write_log() {
	for _, v := range cl.commits {
		fmt.Printf("{T:%d} {I:%d} %s", v.index, v.term, v.command)
	}
	fmt.Print("\n")
}

func (meta *Meta) apply_command(command string, value int) (int, error) {
	switch command {
	case "SET":
		return value, nil
	case "ADD":
		return meta.value + value, nil
	case "SUB":
		return meta.value - value, nil
	default:
	}

	return value, fmt.Errorf("Invalid Command")
}

func (meta *Meta) debug_node() {
	fmt.Println("-----NODE INFO-----")
	fmt.Printf("whoami:%s\n", meta.identity)
	fmt.Printf("leader:%s\n", meta.leader)
	fmt.Printf("state:%s\n", meta.state)
	fmt.Printf("term:%d\n", meta.term)
	fmt.Printf("hearbeat:%d\n", meta.heart_beat.original)
	meta.connection_pool.Range(func(k, v interface{}) bool {
		key := k.(string)
		val := v.(*Connection)
		fmt.Println("IS CONN GOOD?", key, val.dead, val.last_response, val.conn)
		return true
	})
	fmt.Println("-------------------")
}

func (q *Quorum) handle_quorum_timeout(close_quorum_ch chan string) {
	var millis int = 10 // WARNING: right now it decrements 10 milliseconds every 10 ms
	for {
		if q.timeout < millis {

			if q.timeout > 0 {
				time.Sleep(time.Duration(q.timeout) * time.Millisecond)
			}
			// TODO: persist commit log and throw panic or gracefully shutdown
			return
		}
		select {
		case msg := <-close_quorum_ch:
			if msg == "CLOSE" {
				fmt.Println("Gracefully closing quorum")
				fmt.Printf("QUORUM %s: got %d votes with %dms remaining\n", q.quorum_type, q.votes, q.timeout)
				return
			}
			fmt.Println("B")
		default:
			// fmt.Println("C")
		}
		q.timeout -= millis
		time.Sleep(time.Duration(millis) * time.Millisecond)
	}
}

// WARNING: the random timeout generator makes numbers in increments of 10, so I can use millis=10
func handle_timeout(meta *Meta) {
	if meta.state == "FOLLOWER" {
		fmt.Println("inside timeout!")
		var millis int = 10
		for {
			if meta.heart_beat.timeout <= 0 {
				// timeout resets, node becomes candidate, starts a new quorum
				fmt.Println("Leader is down, starting election!")
				fmt.Printf("My current timeout %d\n", meta.heart_beat.timeout)
				if v, ok := meta.connection_pool.Load(meta.leader); ok {
					leader := v.(*Connection)
					leader.conn = nil
					leader.dead = true
				}

				go conduct_election(meta)
				return
			}
			// apparently channels are blocking, only read from channel is there is something
			select {
			case msg := <-meta.timeout_ch:
				if msg == "ACK" {
					meta.heart_beat.timeout = meta.heart_beat.original
				}
			case <-meta.new_leader_ch:
				// fmt.Println("NEW SIGNAL(): handle_timeout()", meta.identity, meta.heart_beat.timeout)
				return
			default:
				// No message on the channel; continue
			}
			meta.heart_beat.timeout -= millis // TODO: better if we use time_now and time_prev, subtract the difference
			time.Sleep(time.Duration(millis) * time.Millisecond)
		}
	}
}

func probe_beat(meta *Meta) {
	if meta.state == "LEADER" {

		var millis int = 30
		for {
			broadcast_command(meta, "HEARTBEAT")
			// leader increments last_response, down below, it is zeroed when follower responds.
			// if last_response > thresh, follower assumed dead until next heartbeat response
			meta.connection_pool.Range(func(k, v interface{}) bool {
				k = k.(string)
				if k != meta.leader {
					if e, ok := meta.connection_pool.Load(k); ok {
						entry := e.(*Connection)
						entry.last_response += 1
						meta.connection_pool.Store(k, entry)
					}
				}
				return true
			})
			select {
			case <-meta.new_leader_ch:
				// fmt.Println("NEW SIGNAL: probe_beat()")
				return
			default:
			}
			time.Sleep(time.Duration(millis) * time.Millisecond)
		}
	}
}

func new_quorum(quorum_type string) *Quorum {
	return &Quorum{quorum_type: quorum_type, votes: 1, timeout: 1000}
}

func main() {

	argv := os.Args[1:]

	listen_port := argv[1]
	listener, err := net.Listen("tcp", "localhost:"+listen_port)
	if err != nil {
		fmt.Println("Listen Error:", err)
		return
	}
	defer listener.Close()

	fmt.Println("Server is listening on port " + listen_port)
	node_identity := argv[0]

	// WARNING / TODO: the idea is to use a longer heartbeat on startup once, subsequent heartbeats will be throttled down to 150-300ms
	var init_timeout int = (rand.Intn((3000-1500)/10+1) * 10) + 1500
	var node_heart_beat *HeartBeat = &HeartBeat{original: init_timeout, timeout: init_timeout}
	var node_state string = "FOLLOWER"

	meta := Meta{identity: node_identity, value: 0, leader: "", state: node_state,
		connection_pool: sync.Map{}, commit_log: &CommitLog{prev_index: 0, prev_term: 0}, quorum: nil,
		timeout_ch: make(chan string), close_quorum_ch: make(chan string), new_leader_ch: make(chan string),
		heart_beat: node_heart_beat, term: 0}

	init_discover_nodes(&meta, 10) // WARNING: first discover on startup, is blocking, if subsequent connections fail, handle them async?

	go probe_beat(&meta)             // works only for leader
	go handle_connection_pool(&meta) // works only for leader
	go handle_timeout(&meta)         // works only for followers

	// WARNING: does it matter if the nodes are not up? because the heart beat is only meant to reset timeout of followers
	// WANRING: do not run probe beat until connection pool has enough? ...
	for {
		conn, err := listener.Accept() // WARNING: this is blocking? prevents nodes from "discovering"?
		if err != nil {
			fmt.Println("Connection Acception Error:", err)
			continue
		}

		go handle_connection(&meta, conn)
		// go discover_nodes(&meta)
	}
}

func handle_connection(meta *Meta, conn net.Conn) {
	defer conn.Close()

	buffer := make([]byte, 1024)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			if err == io.EOF {
				continue
			}
		}

		// if meta.state == "FOLLOWER" {
		// 	fmt.Printf("node timeout: %d\n", meta.heart_beat.timeout)
		// }

		interpret_command(meta, buffer[:n])
	}
}

// if SET, set value, commit tx
// if GET, return value
func interpret_command(meta *Meta, message_buffer []byte) {
	buffer_parts := strings.Split(string(message_buffer[:]), ";")

	for _, buffer_part := range buffer_parts {
		if buffer_part == "" {
			continue
		}
		message := string(buffer_part)
		parts := strings.Fields(message)
		command := parts[0]

		switch command := command; command {
		case "GET":
			fmt.Printf("VALUE> %d\n", meta.value)
		case "SET", "ADD", "SUB":
			// replicate_log sends command of type - COMMAND value leader_term prevLogIndex prevLogTerm
			meta.commit_log.staged = fmt.Sprintf("%s %s", command, parts[1]) // stage the command no matter what
			if meta.state == "LEADER" {
				fmt.Printf("Received From Client: %s\n", message)
				replicate_log(meta, message)
			}

			// this is for setting values - different to "REQVOTE" which is used exclusively for leader election
			if meta.state == "FOLLOWER" {
				fmt.Println("Received Request From Leader")
				v, _ := meta.connection_pool.Load(meta.leader)
				leader := v.(*Connection)
				leader_term, _ := strconv.Atoi(parts[2])
				prevLogIndex, _ := strconv.Atoi(parts[3])
				prevLogTerm, _ := strconv.Atoi(parts[4])

				if prevLogIndex == meta.commit_log.prev_index && prevLogTerm == meta.commit_log.prev_term {
					if meta.voted == false && leader_term == meta.term {
						meta.voted = true
						response(leader.conn, "VOTE")
						fmt.Println("Ready to commit")
					}
				} else {
					response(leader.conn, fmt.Sprintf("RECONCIL %s %d %d", meta.identity, prevLogIndex, prevLogTerm))
					fmt.Printf("Detected mismatch in self, informed leader -> Expected: %d, %d Actual: %d, %d\n", prevLogIndex, prevLogTerm, meta.commit_log.prev_index, meta.commit_log.prev_term)
				}
			}

		case "PERSIST":
			fmt.Printf("Trying to interpret message%s\n", message)
			meta.commit_log.write_log()
		case "VOTE":
			// as long as quorum is not closed. it is assured that the quorum will not close until it majority is reached or majority is reached
			if meta.quorum != nil {
				// two cases, if node is candidate or leader
				if meta.state == "LEADER" {
					meta.quorum.votes += 1
				}
				if meta.state == "CANDIDATE" {
					fmt.Println("received vote!")
					meta.quorum.votes += 1
				}
			}
		case "COMMIT":
			if meta.voted {
				fmt.Println("Commiting staged value! voted:", meta.voted)
				meta.commit_log.commit(meta)
				meta.voted = false
			}
		case "HEARTBEAT":
			if meta.state == "FOLLOWER" {
				meta.timeout_ch <- "ACK"
				if v, ok := meta.connection_pool.Load(meta.leader); ok {
					leader := v.(*Connection)
					response(leader.conn, fmt.Sprintf("HBACK %s", meta.identity))
				}
			}
		case "HBACK":
			if meta.state == "LEADER" {
				node_id := parts[1]
				if v, ok := meta.connection_pool.Load(node_id); ok {
					follower := v.(*Connection)

					// if a dead nodes comes to life, sends a HBACK	regardless of receiving a HEARTBEAT probe
					// if the previously thought dead node does this, try to form connection( as a leader to follower )
					if follower.conn == nil {
						conn, _ := get_connection(node_id)
						follower.conn = conn
					}
					follower.last_response = 0
					meta.connection_pool.Store(node_id, follower)
				}
			}
		case "CPSYNC":
			// connection pool sync - the leader sends this, the state every follower should be replicating
			if meta.state == "FOLLOWER" {

				var alive map[string]bool
				fmt.Println("RECEIVED ALIVE STR:", parts[1])
				err := json.Unmarshal([]byte(parts[1]), &alive)
				if err != nil {
					fmt.Println("Unmarshall Error:", err)
				}

				// fmt.Printf("ALIVE: %v\n", alive)
				for node_id, is_alive := range alive { // is_alive should be read as "should be alive"
					// fmt.Printf("CPSYNC: %s is %s\n", node_id, is_alive)
					if v, ok := meta.connection_pool.Load(node_id); ok {
						val := v.(*Connection) // what meta(self) thinks of the node connecting to

						// if connection parity is same, don't do anything
						if is_alive && !val.dead || !is_alive && val.dead {
							continue

						} else if is_alive && val.dead {
							conn, err := get_connection(val.port)
							if err != nil {
								fmt.Println("Connection Sync: Couldn't get connection to", node_id)
								continue
							}
							val.conn = conn
							val.dead = false
							val.last_response = 0

						} else if !is_alive && !val.dead {
							val.conn = nil
							val.dead = true
						}

						meta.connection_pool.Store(node_id, val)
					}

				}
			}

		case "REQVOTE":
			candidate_identity := parts[1]
			candidate_term := parts[2] // what happens if a candidate send a vote to a node with higher term?

			// if there is a functioning leader and a node asks for votes, it  is clearly a new node trying to join the network which timed out
			// other followers will not try to form connection since they cannot know for sure if leader is dead or not
			// leader triggers a CPSYNC
			if meta.state == "LEADER" {
				if v, ok := meta.connection_pool.Load(candidate_identity); ok {
					val := v.(*Connection)
					conn, err := get_connection(val.port)
					if err != nil {
						break
					}
					val.conn = conn
					val.dead = false
					val.last_response = 0
					meta.connection_pool.Store(candidate_identity, val)
					response(conn, fmt.Sprintf("IMLEADER %s", meta.identity)) // if a follower sends a REQVOTE asks for votes to leader, it willr respdond that it is leader
				}
				fmt.Printf("I'm %s and the leader is %s and I triggered CPSYNC\n", meta.identity, meta.leader)
				fmt.Printf("TRIGGERING CPSYNC: REQVOTE : %s\n", meta.state)
				trigger_cpsync(meta)
			}

			term, _ := strconv.Atoi(candidate_term)
			if v, ok := meta.connection_pool.Load(candidate_identity); ok {
				leader := v.(*Connection)
				if meta.voted == false && meta.state == "FOLLOWER" && meta.term < term {
					response(leader.conn, "VOTE")
					fmt.Printf("%s voted for %s\n", meta.identity, candidate_identity)
					meta.voted = true
				}
			}
		case "IMLEADER":
			new_leader_identity := parts[1]
			fmt.Printf("Received news that %s is new LEADER\n", new_leader_identity)
			// set connection to older leader( now killed ) as nil - the same thing is done even when a node times out
			if v, ok := meta.connection_pool.Load(meta.leader); ok {
				follower := v.(*Connection)
				follower.conn = nil
				meta.connection_pool.Store(meta.leader, follower)
			}

			meta.leader = new_leader_identity
			meta.term += 1
			meta.commit_log.prev_term = meta.term
			meta.voted = false
			meta.heart_beat.timeout = meta.heart_beat.original
			if meta.state == "CANDIDATE" {
				meta.state = "FOLLOWER" // if this node became candidate during quorum
				meta.term -= 1          // follower increments its term first and then becoems candidate to start asking for votes.
				meta.commit_log.prev_term = meta.term
			} else {
				meta.new_leader_ch <- "NEWLEADER" // a candidate exits all go routines which consume any message from new_leader_ch. so it clocks this new_leader_ch, so don't call if it is a candidate
			}
			meta.debug_node()
			go handle_timeout(meta)

		case "RECONCIL":
			if meta.state == "LEADER" {
				follower := parts[1]
				logIndex, _ := strconv.Atoi(parts[2])
				logIndex -= 1
				fmt.Println("Decrementing log index!")
				prevLog := meta.commit_log.commits[logIndex]
				logSlice := meta.commit_log.commits[logIndex:]
				fmt.Println("My current LOG: ", meta.commit_log.commits)
				var logs strings.Builder
				for _, log := range logSlice {
					command := strings.Fields(log.command)
					logs.WriteString(fmt.Sprintf("%s$%s^", command[0], command[1])) // can't use " ", ref - $ - " ", ^ - separator b/w commands
				}
				if v, ok := meta.connection_pool.Load(follower); ok {
					follower := v.(*Connection)
					fmt.Printf("Decremented and sent logs %d, %d, %s\n", logIndex, prevLog.term, logs.String())
					response(follower.conn, fmt.Sprintf("RECONCIL %d %d %s", logIndex, prevLog.term, logs.String()))
				}
			}

			if meta.state == "FOLLOWER" {
				prevLogIndex, _ := strconv.Atoi(parts[1])
				prevLogTerm, _ := strconv.Atoi(parts[2])
				if v, ok := meta.connection_pool.Load(meta.leader); ok {
					leader := v.(*Connection)
					if prevLogIndex != meta.commit_log.prev_index || prevLogTerm != meta.commit_log.prev_term {
						response(leader.conn, fmt.Sprintf("RECONCIL %s %d %d", meta.identity, prevLogIndex, prevLogTerm)) // indicate mismatch, ask leader to send prev index and term
						fmt.Printf("Detected mismatch in self, informed leader -> Expected: %d, %d Actual: %d, %d\n", prevLogIndex, prevLogTerm, meta.commit_log.prev_index, meta.commit_log.prev_term)
					} else {
						// apply logs from prevLogIndex
						// TODO: apply/set log term and then commit
						fmt.Printf("Found point where logs diverged! index: %d, term: %d\n", prevLogIndex, prevLogTerm)
						meta.commit_log.commits = meta.commit_log.commits[:prevLogIndex]
						log := parts[3]
						logs := strings.Split(log, "^")
						for _, log := range logs {
							command = strings.Replace(log, "$", " ", 1)
							meta.commit_log.staged = command
							meta.commit_log.commit(meta)
						}
						meta.commit_log.prev_term = prevLogTerm // index increments iteself in commit(), so it's taken care of
						fmt.Println("Applied all logs, synced with leader!")
					}
				}
			}

		case "NODE":
			meta.debug_node()
		case "LOG":
			meta.commit_log.write_log()
		default:
			fmt.Println("Incorrect Command: " + message)
		}
	}
}

func init_discover_nodes(meta *Meta, timeout int) {
	var connected int = 1 // exclude connection to self
	ports, num_nodes, err := read_config(meta.identity, "config.json")
	if err != nil {
		// error and panic, we print for now
		fmt.Println(err)
	}
	for {
		if connected == num_nodes {
			fmt.Println("Connected to all nodes")
			return // discovered all nodes, continuing
		}
		if timeout <= 0 {
			fmt.Println("Couldn't get all connections to cluster")
			// TODO: persist any commit logs and shutdown
			return
		}
		for node_id, port := range ports {
			// if meta.connection_pool[port] is True, don't try to connect
			_, exists := meta.connection_pool.Load(node_id)
			if exists != true {
				conn, err := get_connection(port)
				if err != nil {
					continue
				}
				meta.connection_pool.Store(node_id, &Connection{conn: conn, port: port, last_response: 0, dead: false})
				connected += 1
			}
		}
		timeout -= 1
		time.Sleep(time.Second)
	}
}

func get_connection(port string) (net.Conn, error) {
	address := fmt.Sprintf("127.0.0.1:%s", port)
	fmt.Println("Trying to connect to:", address)
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// if last_heart_beat of any connection > thresh: mark as dead
func handle_connection_pool(meta *Meta) {
	var millis int = 10
	if meta.state == "LEADER" {
		var thresh int = 3
		// go sync_connection_pool(meta)
		for {
			meta.connection_pool.Range(func(k, v interface{}) bool {
				node_id := k.(string)
				node := v.(*Connection)
				// fmt.Println("C: ", v.dead, v.last_response)

				// if the connection pool still considers a node alive and has crossed the max heartbeat requests
				if node.dead == false && node.last_response >= thresh {
					node.conn = nil
					node.dead = true
					meta.connection_pool.Store(node_id, node)
					fmt.Println("TRIGGERING CPSYNC: HANDLE CONNECTION POOL :", meta.state)
					trigger_cpsync(meta)
					fmt.Printf("OPE, %s is dead! - unrespsonsive for - %d beats\n", node_id, node.last_response)
				}
				return true
			})
			select {
			case <-meta.new_leader_ch:
				return
			default:
			}
			time.Sleep(time.Duration(millis) * time.Millisecond)
		}
	}
}

// occasionally synchronise connection pool state
// func sync_connection_pool(meta *Meta) {
// 	var millis int = 10000
// 	for {
// 		fmt.Println("TRIGGERING CPSYNC: SYNC CONNECTION POOL :", meta.state)
// 		trigger_cpsync(meta)

// 		select {
// 		case <-meta.new_leader_ch:
// 			return
// 		default:
// 		}
// 		time.Sleep(time.Duration(millis) * time.Millisecond)
// 	}
// }

func trigger_cpsync(meta *Meta) {
	alive := make(map[string]bool)
	meta.connection_pool.Range(func(k, v interface{}) bool {
		node_id := k.(string)
		node := v.(*Connection)
		fmt.Println("MARSHALL CPSYNC:", node_id, node.dead)
		alive[node_id] = !node.dead

		return true
	})

	alive_str, err := json.Marshal(alive)
	// fmt.Println("ALIVE STR:", alive_str)
	if err != nil {
		fmt.Println("Error marshalling connection pool: ", err)
	}
	broadcast_command(meta, "CPSYNC "+string(alive_str))
}

func read_config(identity string, filename string) (map[string]string, int, error) {
	plan, err := os.ReadFile(filename)
	if err != nil {
		return nil, 0, err
	}
	var num_nodes int = 0
	var data map[string]interface{}
	ports := make(map[string]string)

	json.Unmarshal(plan, &data)
	if nodes, ok := data["nodes"].([]interface{}); ok {
		for _, node := range nodes {
			if node, ok := node.(string); ok {
				parts := strings.Split(node, ":")
				node_id := parts[0]
				if node_id != identity {
					ports[node_id] = parts[1] // parts[1] contains the port
				}
			}
			num_nodes += 1
		}
	}

	return ports, num_nodes, nil
}

// broadcast command to followers
func broadcast_command(meta *Meta, command string) {
	// fmt.Println("Broadcasting command: ", command)
	to_broadcast := []byte(command + ";") // add a delimiter
	meta.connection_pool.Range(func(k, v interface{}) bool {
		node := v.(*Connection)
		// fmt.Printf("connection exists? %v\n", conn)
		// check if connection to that node exists, only then broadcast
		if node.conn != nil {
			_, err := node.conn.Write(to_broadcast)

			// TODO: technically, you might wanna buffer them and keep retrying?
			// TODO: leader occaisionally does a full_sync( commit logs and connection pools( the leader will coordinate connection pool state sync ) mostly )
			if err != nil {
				return true // basically, continues the iteration
			}
		}
		return true
	})
}

func response(conn net.Conn, message string) {
	// fmt.Println(message)
	response := []byte(message + ";")
	if conn == nil {
		fmt.Println("Invalid connection")
		return
	}
	if response == nil {
		fmt.Println("Invalid response")
		return
	}
	conn.Write(response) // i don't think it's necessary to retry until success, it either fails or succeeds.
}

// tries to bring all nodes to consensus for a given command
func replicate_log(meta *Meta, message string) {
	// get command from client
	// start a quorum
	fmt.Println("Inside replicate_log")
	meta.quorum = new_quorum("LOGREPL")
	// broadcast command to followers

	// WARNING: technically, this should be sending an AppendEntries RPC, but we're doing something similar
	// message here is technically entries[], but there is never more than one command
	broadcast_command(meta, fmt.Sprintf("%s %d %d %d", message, meta.term, meta.commit_log.prev_index, meta.commit_log.prev_term))
	// broadcast_command(meta, message)

	go meta.quorum.handle_quorum_timeout(meta.close_quorum_ch)
	go manage_logrepl_votes(meta)
}

// followers respond with ACK
// leader commits log/value only when majority of nodes respond with ACK (question: how will i know at any given points, how many nodes in cluster, especially when a network partition happens)
func manage_logrepl_votes(meta *Meta) {
	var quorum bool = false
	for {
		// if quorum reached
		if meta.quorum.votes >= 3 {
			quorum = true
			break
		}
		// also check if ack not received after some timeout
		if meta.quorum.timeout <= 0 {
			break
		}
	}

	if quorum {
		// check if followers ready for commit?
		meta.commit_log.commit(meta)
		broadcast_command(meta, "COMMIT")
		meta.close_quorum_ch <- "CLOSE"
	}
}

func conduct_election(meta *Meta) {
	// broadcast for votes
	meta.quorum = new_quorum("ELECTION")
	meta.term += 1
	meta.commit_log.prev_term = meta.term
	meta.state = "CANDIDATE"
	broadcast_command(meta, fmt.Sprintf("REQVOTE %s %d", meta.identity, meta.term))
	fmt.Printf("asking for votes as a candidate %s\n", meta.identity)
	// fmt.Printf("current quorum: %s - votes: %d, timeout: %d\n", meta.quorum.quorum_type, meta.quorum.votes, meta.quorum.timeout)
	go meta.quorum.handle_quorum_timeout(meta.close_quorum_ch)
	go func() {
		var quorum bool = false
		for {
			// if quorum reached
			if meta.quorum.votes >= 3 {
				quorum = true
				break
			}
			// also check if ack not received after some timeout
			// TODO: trigger log reconcilation here too
			if meta.quorum.timeout <= 0 {
				fmt.Println("quorum expired!")
				break
			}
		}

		if quorum {
			broadcast_command(meta, fmt.Sprintf("IMLEADER %s", meta.identity))
			meta.state = "LEADER"
			meta.leader = meta.identity
			meta.close_quorum_ch <- "CLOSE"
			meta.debug_node()

			go probe_beat(meta)
			go handle_connection_pool(meta)
			fmt.Println("The new elected leader is: ", meta.identity)
		}
	}()
}
