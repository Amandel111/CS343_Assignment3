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

var nodeList []*RingNode

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
	MessengerID int
	//IsTerminalLeader:  false,
	//ConfirmedNotLeader bool
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
	node.mutex.Lock()
	defer node.mutex.Unlock()

	prevID := receivedMessage.MessengerID
	arguments := PeerMessage{
		MessengerID: node.selfID,
		// ConfirmedNotLeader: false, //if a node's timer runs out and this is false, that node must be the leader
		//message
	}

	ackReply := "nil"

	if prevID < node.selfID {
		// do we need to check if the timer has already started?
		//also do we start timer before we alert higher nodes or just after we hear from smaller nodes
		//if node.electionTimeout == nil {
		//StartTimer(node)
		/*go func() {
			//thread for each node checking for timeout
			//lock vs unlock here?
			<-node.electionTimeout.C

			// Printed when timer is fired
			fmt.Println("timer inactivated")
		}()*/
		//}
		//send confirmation back
		fmt.Print("received alert from node ", prevID)
		// go func(serverConnection *rpc.Client) {
		// 	fmt.Print("sending confirmation back")
		// 	err := node.peerConnections[prevID].Call("RingNode.RequestVote", arguments, &ackReply)
		// 	if err != nil {
		// 		return
		// 	}
		// }(node.peerConnections[prevID])

		//alert higher nodes
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
					//if there is no error, must have received confirmaiton from higher node, can't be leader
					//node.electionTimeout.Stop() //stop timer if the node hears back
				}(serverConnection)

			}
		}

	}

	//ackReply := "nil"

	// Leader has been identified
	var wg sync.WaitGroup
	wg.Add(1)

	return nil
}

func StartTimer(node *RingNode) {
	//node.mutex.Lock() //dont need to protect because it will be reset every time a node reaches out to it
	//defer node.mutex.Unlock()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	tRandom := time.Duration(r.Intn(150)+151) * time.Millisecond
	node.electionTimeout = time.NewTimer(tRandom)
	fmt.Println("Timer started")

}

func CheckNodeTimers() {
	//call this as a thread working continuosuly in the background
	for i := 0; i < len(nodeList); {
		//currNode := nodeList[i]
		//<- currNode.electionTimeout.C{
		//	fmt.Println()
		// if (currNode.electionTimeout.C){
		// }
	}
}

func (node *RingNode) LeaderElection() { 
	if node.selfID != 1 {
		return
	}

	receivedConfirmation := false

	node.mutex.Lock()
	defer node.mutex.Unlock()

	arguments := PeerMessage{
		MessengerID: node.selfID,
		// ConfirmedNotLeader: false, //if a node's timer runs out and this is false, that node must be the leader
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
				//fmt.Println("server connection: ", node.peerConnections[nodeID])
				err := serverConnection.Call("RingNode.RequestVote", arguments, &ackReply)
				if err != nil {
					fmt.Println("error: ", err)
				}else{
					//we have heard back from higher node, want to start a timer later
					receivedConfirmation = false;
				}
			}(serverConnection)
		}
	}

	if (receivedConfirmation){
		//note that we don't need to protect node because it is on a single machine, no one else accessing
		StartTimer(node)

		go func() {
			//thread for each node checking for timeout
			<-node.electionTimeout.C
	
			// Printed when timer is fired
			fmt.Println("timer inactivated")
		}()
	
	}
}
	//use a flag 

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
	nodeList = append(nodeList, node)

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
