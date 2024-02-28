package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

// RingNode represents a single node in the cluster, providing the
// necessary methods to handle RPCs.
type RingNode struct {
	mutex    sync.Mutex
	selfID   int
	leaderID int
	myPort   string
	//peerConnections ServerConnections
	nominatedSelf   bool
	electionTimeout *time.Timer
	peerConnections map[int]*rpc.Client
}

// RingVote contains the candidate's information that is needed for the
// node receiving the RingVote to decide whether to vote for the candidate
// in question.
type PeerMessage struct {
	messengerID int
	//IsTerminalLeader:  false,
	confirmedNotLeader bool
}

// ServerConnection represents a connection to another node in the Raft cluster.
type ServerConnection struct {
	serverID       int
	rpcConnections []*rpc.Client
}

// -----------------------------------------------------------------------------
// Leader Election
// -----------------------------------------------------------------------------

// RequestVote handles incoming RequestVote RPCs from other servers.
// It checks its own logs to confirm eligibility for leadership
// if the candidateID received is higher that server's own ID, then vote is accepted and passed
// if less than server's own ID, the vote is ignored
// if candidateID is the same as serverID, then election won, and server can confirm its leadership
func (node *RingNode) RequestVote(receivedMessage PeerMessage, acknowledge *string) error {
	// when request is received
	// send OK acknowledgement
	// contact higher nodes
	prevID := receivedMessage.messengerID
	arguments := PeerMessage{
		messengerID:        node.selfID,
		confirmedNotLeader: false, //if a node's timer runs out and this is false, that node must be the leader
		//message
	}

	ackReply := "nil"

	fmt.Println("alert message received for ", prevID)
	if prevID < node.selfID {
		//send confirmation back
		go func(serverConnection *rpc.Client) {
			err := node.peerConnections[prevID].Call("RingNode.RequestVote", arguments, &ackReply)
			if err != nil {
				return
			}
		}(node.peerConnections[prevID])

		/*//alert higher nodes
		for nodeID, serverConnection := range node.peerConnections {
			if nodeID > node.selfID {
				// fmt.Println("server connection outside go call ", serverConnection)
				fmt.Println("the node id ", nodeID, "is higher than my id ", node.selfID)
				fmt.Println("Alerting higher node ", nodeID)
				go func(serverConnection *rpc.Client) {
					fmt.Println("server connection: ", node.peerConnections[nodeID])
					err := serverConnection.Call("RingNode.RequestVote", arguments, &ackReply)
					if err != nil {
						fmt.Println("error: ", err)
						return
					}
				}(serverConnection)*/

	} else {
		//if the node we received a request from is higher, that means we have received confirmation from a higher vote, so we know that this
		//node cannot be the leader
	}
	node.mutex.Lock()
	defer node.mutex.Unlock()

	//ackReply := "nil"

	// Leader has been identified
	var wg sync.WaitGroup
	wg.Add(1)

	return nil
}

func (node *RingNode) LeaderElection() { // we don't need to pass p2pConnection
	// This is an option to limit who can start the leader election
	// Recommended if you want only a specific process to start the election
	if node.selfID != 1 {
		return
	}

	arguments := PeerMessage{
		messengerID:        node.selfID,
		confirmedNotLeader: false, //if a node's timer runs out and this is false, that node must be the leader
	}
	ackReply := "nil"

	fmt.Println("peerConnections ", node.peerConnections)
	//for nodeID, serverConnection := range p2pConnection{
	for nodeID, serverConnection := range node.peerConnections {
		if nodeID > node.selfID {
			// fmt.Println("server connection outside go call ", serverConnection)
			fmt.Println("the node id ", nodeID, "is higher than my id ", node.selfID)
			fmt.Println("Alerting higher node ", nodeID)
			go func(serverConnection *rpc.Client) {
				fmt.Println("server connection: ", node.peerConnections[nodeID])
				err := serverConnection.Call("RingNode.RequestVote", arguments, &ackReply)
				if err != nil {
					fmt.Println("error: ", err)
					return
				}
			}(serverConnection)
		}
	}

	//return nil
	//for {
	// Wait for election timeout
	// Uncomment this if you decide to do periodic leader election

	// <-node.electionTimeout.C
	/*
		// Check if node is already leader so loop does not continue
		if node.leaderID == node.selfID {
			fmt.Println("Ending leader election because I am now leader")
			return
		}

		// Initialize election by incrementing term and voting for self
		arguments := RingVote{
			CandidateID: node.selfID,
			IsTerminal:  false,
		}
		ackReply := "nil"

		// Sending nomination message

		fmt.Println("Requesting votes from ", node.nextNode.serverID, node.nextNode.Address)
		go func(server ServerConnection) {
			err := server.rpcConnection.Call("RingNode.RequestVote", arguments, &ackReply)
			if err != nil {
				return
			}
		}(node.nextNode)

		// If you want leader election to be restarted periodically,
		// Uncomment the next line
		// I do not recommend when debugging

		// node.resetElectionTimeout()
		//}*/

}

// resetElectionTimeout resets the election timeout to a new random duration.
// This function should be called whenever an event occurs that prevents the need for a new election,
// such as receiving a heartbeat from the leader or granting a vote to a candidate.
func (node *RingNode) resetElectionTimeout() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	duration := time.Duration(r.Intn(150)+151) * time.Millisecond
	node.electionTimeout.Stop()          // Use Reset method only on stopped or expired timers
	node.electionTimeout.Reset(duration) // Resets the timer to new random value
}

// -----------------------------------------------------------------------------
func main() {
	// The assumption here is that the command line arguments will contain:
	// This server's ID (zero-based), location and name of the cluster configuration file
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide cluster information.")
		return
	}

	// --- Read the values sent in the command line
	// Get this server's ID (same as its index for simplicity)
	myID, _ := strconv.Atoi(arguments[1])

	// Get the information of the cluster configuration file containing information on other servers
	file, err := os.Open(arguments[2])
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	node := &RingNode{
		selfID:   myID,
		leaderID: -1,
		//nextNode:      ServerConnection{},
		nominatedSelf: false,
		mutex:         sync.Mutex{},
	}

	// --- Read the IP:port info from the cluster configuration file
	scanner := bufio.NewScanner(file)
	lines := make([]string, 0)
	index := 0
	for scanner.Scan() {
		// Get server IP:port
		text := scanner.Text()
		log.Printf(text, index)
		if index == myID {
			node.myPort = text
			index++
			//continue
		}
		// Save that information as a string for now
		lines = append(lines, text)
		index++
	}
	// If anything wrong happens with reading the file, simply exit
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// --- Register the RPCs of this object of type RaftNode
	err = rpc.Register(node)
	if err != nil {
		log.Fatal("Error registering the RPCs", err)
	}
	rpc.HandleHTTP()
	go http.ListenAndServe(node.myPort, nil)
	log.Printf("serving rpc on port" + node.myPort)

	// fmt.Println("index stopped at ", index)

	p2pConnections := make(map[int]*rpc.Client)
	// Connect to all nodes
	for id, address := range lines {
		if id == myID {
			continue
		}

		client, err := rpc.DialHTTP("tcp", address)
		// If connection is not established
		for err != nil {
			// Record it in log
			//log.Println("Trying again. Connection error: ", err)
			// Try again!
			client, err = rpc.DialHTTP("tcp", address)
		}

		p2pConnections[id] = client
		fmt.Printf("Connected to peer %d at %s\n", id, lines[id])
	}
	node.peerConnections = p2pConnections

	// Start the election using a timer
	// Uncomment the next 3 lines, if you want leader election to be initiated periodically
	// I do not recommend it during debugging

	// r := rand.New(rand.NewSource(time.Now().UnixNano()))
	// tRandom := time.Duration(r.Intn(150)+151) * time.Millisecond
	// node.electionTimeout = time.NewTimer(tRandom)

	var wg sync.WaitGroup
	wg.Add(1)
	go node.LeaderElection() // Concurrent leader election, which can be made non-stop with timers
	wg.Wait()                // Waits forever, so main process does not stop
}

/*
each node will connect to every other node:
Consider node i. For each node that i is connected t:
1. compare if its id is higher or lower
	if lwowr: continue
	if higher: send message alerting the leader failure, then for thus higher node repeat the process so it communicates to each higher node
2. node i waits for comfirmation back from all its higher nodes. Once it receives confirmation, it knows itt is not the leader
3. if node i receives no comfirmation before its timer runs out, it must be the leader
	at this point, node i sends a message to every other node, including the failed nodes, saying it is the new leader

TODOs:
* Make the node send a message to nodes with higher id's
* Make higher id's send OK
* Figure out the time out
*once node i receives confimration from a higher node, it is just waiting for election reuslts because it can't be the leader

*/
