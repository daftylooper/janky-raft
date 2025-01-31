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
	connection_pool sync.Map // map[string]Connection
	commit_log      *CommitLog
	quorum          *Quorum
	timeout_ch      chan string
	close_quorum_ch chan string
	new_leader_ch   chan *Meta //notify probe_beat and timeout routines of new leader
	heart_beat      *HeartBeat
	term            int
	voted           bool //represents if node voted this term
}

type Connection struct {
	// mu            sync.Mutex
	conn          net.Conn
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

func (meta *Meta) debug_node() {
	fmt.Println("-----NODE INFO-----")
	fmt.Printf("whoami:%s\n", meta.identity)
	fmt.Printf("leader:%s\n", meta.leader)
	fmt.Printf("state:%s\n", meta.state)
	fmt.Printf("term:%d\n", meta.term)
	fmt.Println("-------------------")
}

func (q *Quorum) handle_quorum_timeout(close_quorum_ch chan string) {
	var millis int = 10 // WARNING: right now it decrements 10 milliseconds every 10 ms
	for {
		if q.timeout < millis {
			if q.timeout > 0 {
				time.Sleep(time.Duration(q.timeout) * time.Millisecond)
			}
			fmt.Printf("Quorum expired( %s ): Nodes taking too long to respond( are possibly dead ), cannot continue as a cluster\n", q.quorum_type)
			fmt.Println("Persisting commit log and shutting down...")
			// TODO: persist commit log and throw panic or gracefully shutdown
			break
		}
		select {
		case msg := <-close_quorum_ch:
			if msg == "CLOSE" {
				fmt.Println("Gracefully closing quorum")
				fmt.Printf("QUORUM %s: got %d votes with %dms remaining\n", q.quorum_type, q.votes, q.timeout)
				return
			}
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

				meta.state = "CANDIDATE"
				go conduct_election(meta)
				break
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
	return &Quorum{quorum_type: quorum_type, votes: 1, timeout: 10000}
}

type CommitLog struct {
	staged  string // the current uncommited command
	commits []string
}

func (cl *CommitLog) commit(meta *Meta) {
	// only if a change is staged, perform commit
	if cl.staged != "" {
		parts := strings.Fields(cl.staged)
		cl.commits = append(cl.commits, cl.staged)
		value, _ := strconv.Atoi(parts[1])
		meta.value = value
		cl.staged = ""
	}
}

func (cl *CommitLog) write_log() {
	for _, commit := range cl.commits {
		fmt.Printf("%s;", commit)
	}
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

	// TODO: these few lines become irrelevant once leader election is implemented
	var init_timeout int = (rand.Intn((3000-1500)/10+1) * 10) + 1500 // WARNING: using 150-300ms timeout range because apparently that is what raft uses?
	var node_heart_beat *HeartBeat = &HeartBeat{original: init_timeout, timeout: init_timeout}
	var node_state string = "FOLLOWER"

	meta := Meta{identity: node_identity, value: 0, leader: "node1", state: node_state,
		connection_pool: sync.Map{}, commit_log: &CommitLog{}, quorum: nil,
		timeout_ch: make(chan string), close_quorum_ch: make(chan string), new_leader_ch: make(chan *Meta),
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
	message := string(message_buffer[:])
	parts := strings.Fields(message)
	command := parts[0]

	switch command := command; command {
	case "GET":
		fmt.Printf("VALUE> %d\n", meta.value)
	case "SET":
		meta.commit_log.staged = message // stage the command no matter what
		if meta.state == "LEADER" {
			fmt.Printf("Received From Client: %s\n", message)
			replicate_log(meta, message)
		}
		if meta.state == "FOLLOWER" {
			fmt.Println("Recieved Request From Leader")
			v, _ := meta.connection_pool.Load(meta.leader)
			leader := v.(*Connection)
			response(leader.conn, "VOTE") // TODO: Don't vote if already cast vote for a particular quorum
			fmt.Println("Ready to commit")
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
		fmt.Println("Commiting staged value")
		meta.commit_log.commit(meta)
	case "HEARTBEAT":
		if meta.state == "FOLLOWER" {
			meta.timeout_ch <- "ACK"
			v, _ := meta.connection_pool.Load(meta.leader)
			leader := v.(*Connection)
			response(leader.conn, fmt.Sprintf("HBACK %s", meta.identity))
		}
	case "HBACK":
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
	// case "CPSYNC":
	// 	// connection pool sync
	case "REQVOTE":
		candidate_identity := parts[1]
		candidate_term := parts[2] // what happens if a candidate send a vote to a node with higher term?
		term, _ := strconv.Atoi(candidate_term)
		if v, ok := meta.connection_pool.Load(candidate_identity); ok {
			leader := v.(*Connection)
			if meta.voted == false && meta.state == "FOLLOWER" && meta.term < term {
				response(leader.conn, "VOTE")
				fmt.Printf("%s voted for %s\n", meta.identity, candidate_identity)
				meta.voted = true
			} else if meta.term > term {
				response(leader.conn, "RECONCIL")
				// TODO: candidate node is stale, do something( log reconciliation )
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
		meta.state = "FOLLOWER" // if this node became candidate during quorum
		meta.voted = false
		meta.heart_beat.timeout = meta.heart_beat.original
		meta.new_leader_ch <- meta
		meta.debug_node()
		go handle_timeout(meta)
	case "RECONCIL":
		meta.term -= 1
		meta.state = "FOLLOWER"
		meta.heart_beat.timeout = meta.heart_beat.original
		// log reconciliation?
	case "LEADER":
		fmt.Println(meta.leader, meta.identity)
	default:
		fmt.Println("Incorrect Command: " + message)
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
				meta.connection_pool.Store(node_id, &Connection{conn: conn, last_response: 0, dead: false})
				connected += 1
			}
		}
		timeout -= 1
		time.Sleep(time.Second)
	}
}

// well not exactly discover peers, but tries to connect to statistically defined nodes
// TODO: add max retries, if doesn't respond, consider dead( panic and error out that system not stable )
// TODO: try connecting with nodes in connection_pool whose connections are nil( nodes can connect and disconnect ), but does this make sense? wouldn't raft handle this?
// func discover_nodes(meta *Meta) {
// 	ports, _, err := read_config(meta.identity, "config.json")
// 	if err != nil {
// 		// error and panic, we print for now
// 		fmt.Println(err)
// 	}
// 	for {
// 		for node_id, port := range ports {
// 			// if meta.connection_pool[port] is True, don't try to connect
// 			if v, ok := meta.connection_pool.Load(node_id); ok {
// 				node := v.(*Connection)
// 				if node.dead == true {
// 					conn, err := get_connection(port)
// 					if err != nil {
// 						continue
// 					}
// 					node.conn = conn
// 					node.last_response = 0
// 					node.dead = false
// 				}
// 				meta.connection_pool.Store(node_id, node)
// 			}
// 		}
// 		time.Sleep(time.Second)
// 	}
// }

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
func sync_connection_pool(meta *Meta) {
	var millis int = 1000
	for {
		alive := make(map[string]bool)
		meta.connection_pool.Range(func(k, v interface{}) bool {
			node_id := k.(string)
			node := v.(*Connection)
			alive[node_id] = node.dead
			// alive[k.(string)] = v.(*Connection).dead
			return true
		})
		meta.connection_pool.Range(func(_, v interface{}) bool {
			alive_str, err := json.Marshal(v)
			if err != nil {
				fmt.Println("Error marshalling connection pool: ", err)
			}
			broadcast_command(meta, "CPSYNC "+string(alive_str))
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
	to_broadcast := []byte(command)
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
	response := []byte(message)
	if conn == nil {
		fmt.Println("Invalid connection")
	}
	if response == nil {
		fmt.Println("Invalid response")
	}
	conn.Write(response) // i don't think it's necessary to retry until success, it either fails or succeeds.
}

// tries to bring all nodes to consensus for a given command
func replicate_log(meta *Meta, message string) {
	// get command from client
	// start a quorum
	fmt.Println("Inside replciate_log")
	meta.quorum = new_quorum("LOGREPL")
	// broadcast command to followers
	broadcast_command(meta, message)

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

	// close the quorum
	if quorum {
		meta.commit_log.commit(meta)
		broadcast_command(meta, "COMMIT")
		meta.close_quorum_ch <- "CLOSE"
	}
}

func conduct_election(meta *Meta) {
	// broadcast for votes
	meta.quorum = new_quorum("ELECTION")
	meta.term += 1
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
			if meta.quorum.timeout <= 0 {
				fmt.Println("Quorum expired before majority")
				break
			}
		}

		if quorum {
			meta.commit_log.commit(meta)
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
