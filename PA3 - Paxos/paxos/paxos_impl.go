package paxos

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"paxosapp/rpc/paxosrpc"
	"reflect"
	"strconv"
	//	"sync"
	"time"
	// "net/rpc"
)

var PROPOSE_TIMEOUT = 2 * time.Second
var LOGF *log.Logger

type kvPair struct {
	k string
	v uint32
}

// THIS IS ACTUALLY NOT NEEDED
// type kpPair struct {
// 	k string
// 	p int
// }

type vpPair struct {
	v uint32
	p int
}

//THE TYPE THAT CHANNELS WILL USE TO ACCESS/FETCH CURRENTLY EXISTING PROPOSALS
type proposal struct {
	k string
	v uint32
	p int
}

/*
RACE CONDITIONS:
The main shared variables that could lead to a race condition are roundNum, kvStore, acceptedStore
and maxProposals.
For each of these, we use a set get/put or increment channels that the various functions use.
A requestHandler function is used to handle the requests from all these channels.

No two functions ever access a common shared variable. So a race condition cannot exist.
*/
type paxosNode struct {
	// TODO: implement this!
	ln             net.Listener
	maxRound       int
	srvID          int
	numNodes       int
	connMap        map[int]*rpc.Client
	keyPropnum     map[string]int //////////////key and proposal number
	kvStore        map[string]uint32
	getRd          chan int
	putReq, getReq chan kvPair
	//NEW VARIABLES FOR DEADLINE 2:
	acceptedStore        map[string]uint32 //will store key value pairs which have been accepted but not yet committed
	maxProposals         map[string]int    //will store the minimum proposal number and value for each key
	getProposal          chan proposal
	putProposal          chan proposal
	putAccept, getAccept chan kvPair
	numRetries           int
	getStore             chan map[string]uint32 //only used to fetch the Key Value store and return it to a new replacement node
	//	mu                   *sync.Mutex //I actually do not use this
}

// Desc:
// NewPaxosNode creates a new PaxosNode. This function should return only when
// all nodes have joined the ring, and should return a non-nil error if this node
// could not be started in spite of dialing any other nodes numRetries times.
//
// Params:
// myHostPort: the hostport string of this new node. We use tcp in this project.
//			   	Note: Please listen to this port rather than hostMap[srvId]
// hostMap: a map from all node IDs to their hostports.
//				Note: Please connect to hostMap[srvId] rather than myHostPort
//				when this node try to make rpc call to itself.
// numNodes: the number of nodes in the ring
// numRetries: if we can't connect with some nodes in hostMap after numRetries attempts, an error should be returned
// replace: a flag which indicates whether this node is a replacement for a node which failed.
func NewPaxosNode(myHostPort string, hostMap map[int]string, numNodes, srvId, numRetries int, replace bool) (PaxosNode, error) {
	_ = reflect.TypeOf(2)
	const (
		name = "log.txt"
		flag = os.O_RDWR | os.O_CREATE
		perm = os.FileMode(0666)
	)

	file, _ := os.OpenFile(name, flag, perm)
	// if err != nil {
	// 	return
	// }
	defer file.Close()

	LOGF = log.New(file, "", log.Lshortfile|log.Lmicroseconds)

	newNode := new(paxosNode)
	newNode.putReq, newNode.getReq = make(chan kvPair), make(chan kvPair)
	newNode.getRd = make(chan int)
	newNode.keyPropnum = make(map[string]int)
	newNode.kvStore = make(map[string]uint32)
	//NEW INITIALIZATIONS
	newNode.putProposal, newNode.getProposal = make(chan proposal), make(chan proposal)
	newNode.putAccept, newNode.getAccept = make(chan kvPair), make(chan kvPair)
	newNode.acceptedStore = make(map[string]uint32)
	newNode.maxProposals = make(map[string]int)
	newNode.getStore = make(chan map[string]uint32)
	//	newNode.mu = &sync.Mutex{} //unused lol
	////////////////

	ln, err := net.Listen("tcp", myHostPort)
	if err != nil {
		fmt.Printf("error")
		return nil, err
	}
	//	fmt.Println(numNodes, " numbers of nodes!")
	/////////////////////////////////////// SETTING UP RPC CODE
	rpcServer := rpc.NewServer()
	rpcServer.Register(paxosrpc.Wrap(newNode))
	http.DefaultServeMux = http.NewServeMux()
	rpcServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	go http.Serve(ln, nil)

	/////////////////////////////// SETTING UP CONNECTIONS WTIH CLIENTS
	newNode.connMap = make(map[int]*rpc.Client)

	for id, port := range hostMap {
		var err error
		var conn *rpc.Client

		//try for  intial try + numTries and break if successful

		for i := 0; i <= numRetries; i++ {
			//fmt.Println(i)
			conn, err = rpc.DialHTTP("tcp", port)
			if err == nil {
				newNode.connMap[id] = conn
				break
			} else {
				time.Sleep(time.Second)
			}
		}
	}
	LOGF.Println("working")
	newNode.ln = ln
	newNode.srvID = srvId
	newNode.maxRound = 0
	newNode.numNodes = numNodes
	newNode.numRetries = numRetries

	go newNode.requestsHandler()

	if replace { // SETUP REPLACEMENT NODE
		for _, conn := range newNode.connMap {
			replaceArgs := &paxosrpc.ReplaceServerArgs{SrvID: srvId, Hostport: myHostPort}
			replaceReplies := &paxosrpc.ReplaceServerReply{}
			conn.Call("PaxosNode.RecvReplaceServer", replaceArgs, replaceReplies)
		}
		// LOGF.Println("recvReplace calls done")
		idRand := 0
		for idRand == newNode.srvID {
			rand.Seed(time.Now().UnixNano())
			idRand = rand.Intn(numNodes-1) + 0
		}
		catchupArgs := &paxosrpc.ReplaceCatchupArgs{}
		catchupReplies := &paxosrpc.ReplaceCatchupReply{}
		// LOGF.Println("Calling catchup", time.Now().UnixNano())
		newNode.connMap[idRand].Call("PaxosNode.RecvReplaceCatchup", catchupArgs, catchupReplies)
		// LOGF.Println("Calling catchup done", time.Now().UnixNano())
		commitedStore := make(map[string]uint32)

		err = json.Unmarshal(catchupReplies.Data, &commitedStore)
		if err != nil {
			return nil, err
		}
		newNode.kvStore = commitedStore
		LOGF.Println(commitedStore)
		// LOGF.Println(time.Now().UnixNano())
	}
	LOGF.Println("returning")
	return newNode, err
}

//Desc:
//Awaits on messages from channels to access/modify variables that may raise race flag.
func (pn *paxosNode) requestsHandler() {
	for {
		select {
		case pair := <-pn.putReq: //processput request to key value store
			pn.kvStore[pair.k] = pair.v
			delete(pn.acceptedStore, pair.k)
			delete(pn.maxProposals, pair.k)
		case p := <-pn.getReq: //process get request
			pn.getReq <- kvPair{p.k, pn.kvStore[p.k]}
		case _ = <-pn.getRd: //returns max round number
			pn.getRd <- pn.maxRound
			pn.maxRound++
		//NEW CASES ADDED FOR DEADLINE 2
		case getP := <-pn.getProposal:
			valuePid, exists := pn.maxProposals[getP.k]
			if exists {
				pn.getProposal <- proposal{getP.k, 0, valuePid}
			} else {
				pn.getProposal <- proposal{getP.k, 0, -1}
			}
		case proposal := <-pn.putProposal:
			pn.maxProposals[proposal.k] = proposal.p
		case acceptReq := <-pn.getAccept:
			val, exists := pn.acceptedStore[acceptReq.k]
			if exists {
				pn.getAccept <- kvPair{acceptReq.k, val}
			} else {
				pn.getAccept <- kvPair{acceptReq.k, 0}
			}

		case req := <-pn.putAccept:
			pn.acceptedStore[req.k] = req.v
		//FOR REPLACEMENT CATCHUP
		case _ = <-pn.getStore:
			pn.getStore <- pn.kvStore
		}
	}
}

// Desc:
// GetNextProposalNumber generates a proposal number which will be passed to
// Propose. Proposal numbers should not repeat for a key, and for a particular
// <node, key> pair, they should be strictly increasing.
//
// Params:
// args: the key to propose
// reply: the next proposal number for the given key
func (pn *paxosNode) GetNextProposalNumber(args *paxosrpc.ProposalNumberArgs, reply *paxosrpc.ProposalNumberReply) error {
	pn.getRd <- 1
	max := <-pn.getRd * 10

	nextNum := strconv.Itoa(max) + strconv.Itoa(pn.srvID)
	reply.N, _ = strconv.Atoi(nextNum)
	//	fmt.Println(nextNum)
	return nil
	//return errors.New("not implemented")
}

// Desc:
// Propose initializes proposing a value for a key, and replies with the
// value that was committed for that key. Propose should not return until
// a value has been committed, or PROPOSE_TIMEOUT seconds have passed.
//
// Params:
// args: the key, value pair to propose together with the proposal number returned by GetNextProposalNumber
// reply: value that was actually committed for the given key
func (pn *paxosNode) Propose(args *paxosrpc.ProposeArgs, reply *paxosrpc.ProposeReply) error {

	// BROADCAST PREPARE MESSAGE
	majority := 0
	prepReplies := make([]paxosrpc.PrepareReply, pn.numNodes)
	prepArgs := &paxosrpc.PrepareArgs{Key: args.Key, N: args.N, RequesterId: pn.srvID}
	i := 0
	for _, client := range pn.connMap {
		client.Call("PaxosNode.RecvPrepare", prepArgs, &prepReplies[i])
		i++
	}
	for i := 0; i < pn.numNodes; i++ {
		if prepReplies[i].Status == paxosrpc.OK {
			majority++
		}
	}
	if majority < (pn.numNodes/2)+1 { // Proposal fails essentially, so sit back and wait for commit
		reply.V = pn.getCommittedVal(args.Key)
		return nil // GAME OVER GO BACK HOME BRO
	}
	//to check if prepare message returns any accepted values
	propVal := fetchMaxValue(prepReplies)
	if propVal == 0 {
		propVal = args.V.(uint32)
	} else {
		//fmt.Println("val changed yoo")
	}
	/////////////////////////////////

	// BROADCAST ACCEPT MESSAGE
	acceptReplies := make([]paxosrpc.AcceptReply, pn.numNodes)
	acceptArgs := &paxosrpc.AcceptArgs{args.Key, args.N, propVal, pn.srvID}
	majority = 0
	i = 0
	for _, client := range pn.connMap {
		client.Call("PaxosNode.RecvAccept", acceptArgs, &acceptReplies[i])
		i++
	}
	majority = 0
	for i := 0; i < pn.numNodes; i++ {
		if acceptReplies[i].Status == paxosrpc.OK {
			majority++
		}
	}
	if majority < (pn.numNodes/2)+1 {
		// wait for someone else to Commit
		reply.V = pn.getCommittedVal(args.Key)
		return nil
	}
	/////////////////////////////////

	// IF CODE REACHES THIS POINT THE PROPOSER HAS EFFECTIVELY WON THE PROPOSAL SO YAYY
	// BROADCAST COMMIT MESSAGE
	commitReplies := new(paxosrpc.CommitReply)
	commitArgs := &paxosrpc.CommitArgs{Key: args.Key, V: propVal, RequesterId: pn.srvID}
	for _, client := range pn.connMap {
		client.Call("PaxosNode.RecvCommit", commitArgs, commitReplies)
	}
	reply.V = propVal
	return nil

}

//fetches value from max accepted proposer (if any) from a prepare function replies
func fetchMaxValue(replies []paxosrpc.PrepareReply) uint32 {
	maxProposer, corrVal := 0, uint32(0)
	for _, reply := range replies {
		if reply.N_a > maxProposer {
			maxProposer, corrVal = reply.N_a, reply.V_a.(uint32)
		}
	}
	return corrVal
}

func (pn *paxosNode) getCommittedVal(key string) uint32 {
	for {
		time.Sleep(time.Second * 1)
		pn.getReq <- kvPair{key, 0}
		response := (<-pn.getReq).v
		if response == 0 {
			continue
		}
		return response
	}
}

// Desc:
// GetValue looks up the value for a key, and replies with the value or with
// the Status KeyNotFound.
//
// Params:
// args: the key to check
// reply: the value and status for this lookup of the given key
func (pn *paxosNode) GetValue(args *paxosrpc.GetValueArgs, reply *paxosrpc.GetValueReply) error {
	rand.Seed(time.Now().UnixNano())
	t := rand.Intn(2500) //random delay incase multiple getValue RPC calls invoked simultaneuosly

	time.Sleep(time.Millisecond * time.Duration(t))
	pn.getReq <- kvPair{args.Key, 0}
	reply.V = (<-pn.getReq).v
	reply.Status = paxosrpc.KeyFound
	if reply.V == 0 {
		fmt.Println(pn.kvStore)
		reply.Status = paxosrpc.KeyNotFound
	}
	return nil
}

// Desc:
// Receive a Prepare message from another Paxos Node. The message contains
// the key whose value is being proposed by the node sending the prepare
// message. This function should respond with Status OK if the prepare is
// accepted and Reject otherwise.
//
// Params:
// args: the Prepare Message, you must include RequesterId when you call this API
// reply: the Prepare Reply Message
func (pn *paxosNode) RecvPrepare(args *paxosrpc.PrepareArgs, reply *paxosrpc.PrepareReply) error {
	pn.getProposal <- proposal{args.Key, 0, args.N}
	resp := <-pn.getProposal
	if resp.p > args.N { //OUTRIGHT REJECT
		//		fmt.Println("###########")
		reply.Status = paxosrpc.Reject
	} else { //ACCEPT BUT CHECK IF ACCEPTED VALUES EXIST ALREADY AND SEND THOSE
		pn.putProposal <- proposal{args.Key, 0, args.N}
		reply.Status, reply.N_a = paxosrpc.OK, args.N
		pn.getAccept <- kvPair{args.Key, 0}
		getA := <-pn.getAccept
		reply.N_a = resp.p
		reply.V_a = getA.v
	}
	return nil
}

// Desc:
// Receive an Accept message from another Paxos Node. The message contains
// the key whose value is being proposed by the node sending the accept
// message. This function should respond with Status OK if the prepare is
// accepted and Reject otherwise.
//
// Params:
// args: the Please Accept Message, you must include RequesterId when you call this API
// reply: the Accept Reply Message
func (pn *paxosNode) RecvAccept(args *paxosrpc.AcceptArgs, reply *paxosrpc.AcceptReply) error {
	pn.getProposal <- proposal{args.Key, 0, args.N}
	resp := <-pn.getProposal
	if resp.p > args.N { // REJECT
		//		fmt.Println("****************")
		reply.Status = paxosrpc.Reject
	} else { //ACCEPT
		pn.putAccept <- kvPair{args.Key, args.V.(uint32)}
		reply.Status = paxosrpc.OK
	}

	return nil
}

// Desc:
// Receive a Commit message from another Paxos Node. The message contains
// the key whose value was proposed by the node sending the commit
// message.
//
// Params:
// args: the Commit Message, you must include RequesterId when you call this API
// reply: the Commit Reply Message
func (pn *paxosNode) RecvCommit(args *paxosrpc.CommitArgs, reply *paxosrpc.CommitReply) error {
	// fmt.Println("=====committing")
	//	fmt.Println(args.Key, args.V)
	pn.putReq <- kvPair{args.Key, args.V.(uint32)}

	return nil
}

// Desc:
// Notify another node of a replacement server which has started up. The
// message contains the Server ID of the node being replaced, and the
// hostport of the replacement node
//
// Params:
// args: the id and the hostport of the server being replaced
// reply: no use
func (pn *paxosNode) RecvReplaceServer(args *paxosrpc.ReplaceServerArgs, reply *paxosrpc.ReplaceServerReply) error {
	//return errors.New("not implemented")
	for i := 0; i <= pn.numRetries; i++ {
		conn, err := rpc.DialHTTP("tcp", args.Hostport)
		if err == nil {
			pn.connMap[args.SrvID] = conn
			break
		} else {
			//			fmt.Println("fail :(")
			time.Sleep(time.Millisecond * 400)
		}
	}
	return nil
}

// Desc:
// Request the value that was agreed upon for a particular round. A node
// receiving this message should reply with the data (as an array of bytes)
// needed to make the replacement server aware of the keys and values
// committed so far.
//
// Params:
// args: no use
// reply: a byte array containing necessary data used by replacement server to recover
func (pn *paxosNode) RecvReplaceCatchup(args *paxosrpc.ReplaceCatchupArgs, reply *paxosrpc.ReplaceCatchupReply) error {
	store := make(map[string]uint32)
	pn.getStore <- store
	store = <-pn.getStore
	commitMarsh, _ := json.Marshal(store)
	reply.Data = commitMarsh
	return nil
}
