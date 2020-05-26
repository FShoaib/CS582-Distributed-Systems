package loadbalancer

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"strconv"
	"tinyepc/rpcs"
)

/*
Used two locks at the Hash Ring and at each MME to prevent race conditions
*/
type hashSort []MMENode

func (a hashSort) Len() int           { return len(a) }
func (a hashSort) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a hashSort) Less(i, j int) bool { return a[i].hash < a[j].hash }

type MMENode struct {
	port string
	hash uint64 //it's corresponding physical
}

type loadBalancer struct {
	// TODO: Implement this!
	ln          net.Listener
	numNodes    int
	weight      int
	hashRing    *ConsistentHashing
	sortedNames []MMENode //THIS WILL MAINTAIN A LIST OF PHYSICAL NODES AS THEY APPEAR ON THE RING
}

// New returns a new instance of LoadBalancer, but does not start it
func New(ringWeight int) LoadBalancer {
	// TODO: Implement this!
	newLB := new(loadBalancer)
	newLB.sortedNames = make([]MMENode, 0)
	newLB.weight = ringWeight
	newLB.hashRing = NewRing()
	if 7 == 2 {
		fmt.Println(ringWeight)
	}
	return newLB
}

func (lb *loadBalancer) StartLB(port int) error {
	// TODO: Implement this!
	lb.numNodes = 0
	tcpAddr, err := net.ResolveTCPAddr("tcp", ":"+strconv.Itoa(port))
	lnn, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return err
	}

	//RPC CODE
	rpcServer := rpc.NewServer()
	rpcServer.Register(rpcs.WrapLoadBalancer(lb))
	http.DefaultServeMux = http.NewServeMux()
	rpcServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	go http.Serve(lnn, nil)
	////////

	lb.ln = lnn

	return nil
}

func (lb *loadBalancer) Close() {
	lb.ln.Close()

}

func (lb *loadBalancer) RecvUERequest(args *rpcs.UERequestArgs, reply *rpcs.UERequestReply) error {

	err := lb.hashRing.handleUEReq(args)
	if err != nil {
		return err
	}
	return nil
}

func (lb *loadBalancer) RecvLeave(args *rpcs.LeaveArgs, reply *rpcs.LeaveReply) error {
	_ = errors.New("RecvLeave() not implemented")
	if err := lb.hashRing.removeNode(args.HostPort); err != nil {
		return err
	}

	lb.numNodes--
	for i, el := range lb.sortedNames {
		if el.port == args.HostPort {
			lb.sortedNames = append(lb.sortedNames[:i], lb.sortedNames[i+1:]...)
			break
		}
	}
	return nil
}

// RecvLBStats is called by the tests to fetch LB state information
// To pass the tests, please follow the guidelines below carefully.
//
// <reply> (type *rpcs.LBStatsReply) fields must be set as follows:
// RingNodes:			Total number of nodes in the hash ring (physical + virtual)
// PhysicalNodes:		Total number of physical nodes ONLY in the ring
// Hashes:				Sorted List of all the nodes'(physical + virtual) hashes
//						e.g. [5655845225, 789123654, 984545574]
// ServerNames:			List of all the physical nodes' hostPort string as they appear in
// 						the hash ring. e.g. [":5002", ":5001", ":5008"]
func (lb *loadBalancer) RecvLBStats(args *rpcs.LBStatsArgs, reply *rpcs.LBStatsReply) error {

	reply.RingNodes = len(lb.hashRing.hashVals) // * lb.weight
	reply.PhysicalNodes = lb.numNodes
	reply.Hashes = lb.hashRing.hashVals
	var names []string
	for _, node := range lb.sortedNames {
		names = append(names, node.port)
	}
	reply.ServerNames = names
	return nil
}

// TODO: add additional methods/functions below!

func (lb *loadBalancer) JoinRPC(args *rpcs.JoinArgs, reply *rpcs.JoinReply) error {
	port := args.Cli
	lb.hashRing.locc.Lock()
	lb.sortedNames = append(lb.sortedNames, MMENode{port, lb.hashRing.Hash(port)})
	sort.Sort(hashSort(lb.sortedNames))
	err := lb.hashRing.AddSorted(port, lb.weight)
	lb.hashRing.locc.Unlock()
	if err != nil {
		return err
	}
	lb.numNodes++

	return nil
}
