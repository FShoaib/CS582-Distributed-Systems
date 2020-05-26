package loadbalancer

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"net/rpc"
	"sort"
	"strconv"
	"sync"
	"tinyepc/rpcs"
)

//// Borrowed the following code from online///////
// UInts will be my interface on which I will call Sort
type UInts []uint64

func (u UInts) Len() int { return len(u) }

func (u UInts) Less(i, j int) bool { return u[i] < u[j] }

func (u UInts) Swap(i, j int) {
	u[i], u[j] = u[j], u[i]
}

////LINK: https://play.golang.org/p/gCVJwucNQc
///////////////////////////////////////////////////

// ConsistentHashing contains any fields that are required to maintain
// the consistent hash ring
type ConsistentHashing struct {
	// TODO: Implement this!
	hashVals []uint64            //sorted list of all nodes Virtual as well as Physical
	MMEPorts map[string][]uint64 //map of each nodes hashed values
	// in the case of virtual nodes, each value in MMEPorts will contain more than one elements

	MMEConns map[string]*rpc.Client //contains the RPC connections to each MME node
	rehash   bool                   //rehash initialized to false turns true at the point after which LB is
	//supposed to rehash the UE requests to new nodes
	locc sync.Mutex
}

func NewRing() *ConsistentHashing {
	ch := new(ConsistentHashing)
	ch.MMEConns = make(map[string]*rpc.Client)
	ch.hashVals = make([]uint64, 0)
	ch.MMEPorts = make(map[string][]uint64)
	ch.rehash = false
	return ch
}

// Hash calculates hash of the input key using SHA-256. DO NOT MODIFY!
func (c *ConsistentHashing) Hash(key string) uint64 {
	hasher := sha256.New()
	hasher.Write([]byte(key))
	digest := hasher.Sum(nil)
	digestAsUint64 := binary.LittleEndian.Uint64(digest)
	return digestAsUint64
}

// VirtualNodeHash returns the hash of the nth virtual node of a given
// physical node. DO NOT MODIFY!
//
// Parameters:  key - physical node's key
// 				n - nth virtual node (Possible values: 1, 2, 3, etc.)
func (c *ConsistentHashing) VirtualNodeHash(key string, n int) uint64 {
	return c.Hash(strconv.Itoa(n) + "-" + key)
}

// This function finds the MME port reponsible for a node (which may be virtual or physical)

func findMMEPort(MMEPorts map[string][]uint64, corrHash uint64) (string, error) {
	for port, nodeHashes := range MMEPorts {
		for i := 0; i < len(nodeHashes); i++ {
			if nodeHashes[i] == corrHash {
				return port, nil
			}
		}
	}

	return "", errors.New("port not found, ce n'est pas possible @ln 75 find de la MME")
}

//the request gets passed on from the Load Balancer to here
func (c *ConsistentHashing) handleUEReq(req *rpcs.UERequestArgs) error {
	c.locc.Lock()

	c.rehash = true
	hash := c.Hash(strconv.FormatUint(req.UserID, 10))
	corrHash := c.findHash(hash)
	port, err := findMMEPort(c.MMEPorts, corrHash)
	if err != nil {
		return err
	}

	UEReqArgs := &rpcs.UERequestArgs{hash, req.UEOperation}
	UEReqReply := &rpcs.UERequestReply{}
	err = c.MMEConns[port].Call("MME.RecvUERequest", UEReqArgs, UEReqReply)
	c.locc.Unlock()
	if err != nil {
		return err
	}

	return nil

}

// TODO: add additional methods/functions below!
// Inserts new MME port names and puts their hash values into the hash ring.
//
// Parameters:	port 	-	new port-name to be added
//				w 		-	Ring Weight
func (c *ConsistentHashing) AddSorted(port string, w int) error {
	hash := c.Hash(port)
	c.hashVals = append(c.hashVals, hash)
	//	fmt.Println(port, w)
	c.MMEPorts[port] = append(c.MMEPorts[port], hash)

	if w > 1 {
		for i := 1; i < w; i++ {
			hash = c.VirtualNodeHash(port, i)
			c.hashVals = append(c.hashVals, hash)
			c.MMEPorts[port] = append(c.MMEPorts[port], hash)
		}
	}

	sort.Sort(UInts(c.hashVals))
	conn, err := rpc.DialHTTP("tcp", port)
	if err != nil {
		fmt.Println("ERRORR connecting to MME node", port)
		return err
	}
	c.MMEConns[port] = conn
	if c.rehash {
		err = c.relocateUEReqs()
		if err != nil {
			return err
		}
	}
	if len(c.MMEConns) > 1 { //only implements replicase when >1 MME nodes
		err = c.assignReplicas()
		if err != nil {
			return err
		}
	}
	return nil
}

//self explanatory
func (c *ConsistentHashing) assignReplicas() error {
	replicas := make(map[string][]string) //will be filled with list of replicas for each port
	for _, hashVal := range c.hashVals {
		port, err := findMMEPort(c.MMEPorts, hashVal)
		if err != nil {
			return err
		}
		nextHash := c.findHash(hashVal + 1)
		portReplica, err := findMMEPort(c.MMEPorts, nextHash)

		//the following is to ensure that the new replica being chosen is valid
		//AND maps to a DIFFERENT MME
		for {
			if portReplica != port {
				break
			}
			nextHash = c.findHash(nextHash + 1)
			portReplica, err = findMMEPort(c.MMEPorts, nextHash)
		}
		replicas[port] = append(replicas[port], portReplica)
	}
	for port, replicaList := range replicas {
		err := c.MMEConns[port].Call("MME.RecvReplicas", &rpcs.ReplicaArgs{replicaList, false}, &rpcs.ReplicaReply{})
		if err != nil {
			return err
		}
	}
	return nil
}

//finds the corresponding primary node for a given hash value
func (c *ConsistentHashing) findHash(hash uint64) uint64 {
	var corrHash uint64
	corrHash = 0
	for i := 0; i < len(c.hashVals); i++ {
		if hash <= c.hashVals[i] {
			corrHash = c.hashVals[i]
			break
		}
	}
	if corrHash == 0 {
		corrHash = c.hashVals[0]
	}

	return corrHash
}

//self explanatory
func (c *ConsistentHashing) removeNode(port string) error {
	//retrieve data from leaving MME node
	reply := &rpcs.RetrieveReply{}
	c.locc.Lock()

	err := c.MMEConns[port].Call("MME.RetrieveState", &rpcs.RetrieveArgs{}, reply)
	// Remove all its record from hash ring
	for _, hash := range c.MMEPorts[port] {
		for i, el := range c.hashVals {
			if el == hash {
				c.hashVals = append(c.hashVals[:i], c.hashVals[i+1:]...)
				break
			}
		}
	}
	delete(c.MMEPorts, port)
	delete(c.MMEConns, port)
	err = c.ResendBalance(reply.UEsBalance)
	// IF only 1 MME node left then no replicase for anyone
	if len(c.MMEConns) > 1 {
		err = c.assignReplicas()
	} else {
		for _, conn := range c.MMEConns {
			conn.Call("MME.RecvReplicas", &rpcs.ReplicaArgs{[]string{}, true}, &rpcs.ReplicaReply{})
		}
	}
	if err != nil {
		return err
	}
	c.locc.Unlock()
	return nil
}

func (c *ConsistentHashing) relocateUEReqs() error {
	allBalances := make(map[uint64]float64)
	//retrieve all state from all MME nodes and save it in allBalances
	for _, v := range c.MMEConns {
		args := &rpcs.RetrieveArgs{}
		reply := &rpcs.RetrieveReply{}
		if err := v.Call("MME.RetrieveState", args, reply); err != nil {
			return err
		}
		for i, j := range reply.UEsBalance {
			allBalances[i] = j
		}
		if err := v.Call("MME.ResetState", &rpcs.ResetArgs{}, &rpcs.ResetReply{}); err != nil {
			return err
		}
	}
	if err := c.ResendBalance(allBalances); err != nil {
		return err
	}
	return nil
}
func (c *ConsistentHashing) ResendBalance(balances map[uint64]float64) error {
	for k, v := range balances {
		corrHash := c.findHash(k)
		port, err := findMMEPort(c.MMEPorts, corrHash)
		if err != nil {
			return err
		}
		args := &rpcs.RelocateArgs{k, v}
		reply := &rpcs.RelocateReply{}
		err = c.MMEConns[port].Call("MME.Relocate", args, reply)
		if err != nil {
			return err
		}
	}
	return nil
}
