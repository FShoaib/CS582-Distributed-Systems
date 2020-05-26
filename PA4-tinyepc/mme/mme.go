package mme

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"reflect"
	"sync"
	"tinyepc/rpcs"
)

type mme struct {
	// TODO: Implement this!
	ln          net.Listener
	lbConn      *rpc.Client
	UEsBalance  map[uint64]float64
	reqsHandled int
	replicas    []string
	locc        sync.Mutex
	myPort      string
}

// New creates and returns (but does not start) a new MME.
func New() MME {
	// TODO: Implement this!
	_ = reflect.TypeOf(0)
	newMME := new(mme)
	newMME.reqsHandled = 0
	newMME.UEsBalance = make(map[uint64]float64)
	newMME.replicas = make([]string, 0)
	return newMME
}

func (m *mme) Close() {
	// TODO: Implement this!
	m.ln.Close()
}

func (m *mme) StartMME(hostPort string, loadBalancer string) error {
	// TODO: Implement this!
	ln, err := net.Listen("tcp", hostPort)
	if err != nil {
		fmt.Printf("error")
		return err
	}
	//RPC CODE
	rpcServer := rpc.NewServer()
	rpcServer.Register(rpcs.WrapMME(m))
	http.DefaultServeMux = http.NewServeMux()
	rpcServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	go http.Serve(ln, nil)
	//////////
	m.ln = ln
	conn, err := rpc.DialHTTP("tcp", loadBalancer)
	if err != nil {
		fmt.Println("ERRORRO", loadBalancer)
		return err
	}
	m.lbConn = conn
	joinArgs := &rpcs.JoinArgs{hostPort}
	joinReply := &rpcs.JoinReply{}
	err = m.lbConn.Call("LoadBalancer.JoinRPC", joinArgs, joinReply)
	if err != nil {
		return err
	}
	//	fmt.Println("MME joined @ port",hostPort)
	m.myPort = hostPort
	return nil
}

func (m *mme) RecvUERequest(args *rpcs.UERequestArgs, reply *rpcs.UERequestReply) error {
	// TODO: Implement this!
	m.locc.Lock()
	_, ok := m.UEsBalance[args.UserID]
	if ok == false {
		m.UEsBalance[args.UserID] = 100.0
	}
	if args.UEOperation == rpcs.Call {
		m.UEsBalance[args.UserID] -= 5 //m.UEsBalance[args.UserID].Balance - 5
	} else if args.UEOperation == rpcs.SMS {
		m.UEsBalance[args.UserID] -= 1
	} else if args.UEOperation == rpcs.Load {
		m.UEsBalance[args.UserID] += 10
	} else {
		return errors.New("Invalid Operation??? ")
	}
	m.reqsHandled++
	// fmt.Println(m.myPort, m.reqsHandled)
	m.locc.Unlock()
	return nil
}

// RecvMMEStats is called by the tests to fetch MME state information
// To pass the tests, please follow the guidelines below carefully.
//
// <reply> (type *rpcs.MMEStatsReply) fields must be set as follows:
// 		Replicas: 	List of hostPort strings of replicas
// 					example: [":4110", ":1234"]
// 		NumServed: 	Number of user requests served by this MME
// 					example: 5000
// 		State: 		Map of user states with hash of UserID as key and rpcs.MMEState as value
//					example: 	{
//								"3549791233": {"Balance": 563, ...},
//								"4545544485": {"Balance": 875, ...},
//								"3549791233": {"Balance": 300, ...},
//								...
//								}
func (m *mme) RecvMMEStats(args *rpcs.MMEStatsArgs, reply *rpcs.MMEStatsReply) error {
	// TODO: Implement this!
	m.locc.Lock()
	states := make(map[uint64]rpcs.MMEState)

	for k, val := range m.UEsBalance {
		states[k] = rpcs.MMEState{val}
	}

	reply.State = states
	reply.Replicas = m.replicas

	reply.NumServed = m.reqsHandled
	//	fmt.Println("RecvMMEStats called>>  Replica: ", m.myPort, reply.Replicas)
	m.locc.Unlock()

	return nil
}

// TODO: add additional methods/functions below!
func (m *mme) RetrieveState(args *rpcs.RetrieveArgs, reply *rpcs.RetrieveReply) error {
	m.locc.Lock()
	reply.UEsBalance = m.UEsBalance
	m.locc.Unlock()
	return nil
}

func (m *mme) ResetState(args *rpcs.ResetArgs, reply *rpcs.ResetReply) error {
	m.locc.Lock()
	m.UEsBalance = make(map[uint64]float64)
	//	fmt.Println ("Resetting state -> ", m.UEsBalance)
	m.locc.Unlock()
	return nil
}

func (m *mme) Relocate(args *rpcs.RelocateArgs, reply *rpcs.RelocateReply) error {
	m.locc.Lock()
	m.UEsBalance[args.UserID] = args.Balance
	m.locc.Unlock()
	return nil
}
func (m *mme) RecvReplicas(args *rpcs.ReplicaArgs, reply *rpcs.ReplicaReply) error {
	//	fmt.Println(m.myPort, "recieving replicas")
	m.replicas = make([]string, 0)
	if args.SingleNode {
		return nil
	}
	for _, port := range args.Replicas {
		if m.validReplica(port) {
			m.replicas = append(m.replicas, port)
		}
	}
	return nil
}
func (m *mme) validReplica(port string) bool {
	if port == m.myPort {
		return false
	}
	for _, replica := range m.replicas {
		if replica == port {
			return false
		}
	}
	return true
}
