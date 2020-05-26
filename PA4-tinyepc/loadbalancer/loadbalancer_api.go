// This file contains the exported, functions implemented by
// a Load Balancer. RPC functions implemented by the Load Balancer
// are declared in the RemoteLoadBalancer struct in rpcs/rpc.go
//
// DO NOT MODIFY!

package loadbalancer

// LoadBalancer implements a simple consistent hashing-based load balancer
// to distribute UE requests amongst the MMEs
type LoadBalancer interface {

	// StartLB starts the Load Balancer on a distinct port, starts a TCP listener, embeds
	// the RemoteLoadBalancer functions into the LoadBalancer struct and registers
	// RemoteMME methods
	// for RPC-access. StartLB returns an error if there was a problem listening on the
	// specified port or if the Load Balancer did not assign the MME an ID and provide
	// the list of replicas.
	//
	// This method should NOT block. Instead, it should spawn one or more
	// goroutines (to handle things like accepting RPC calls, etc) and then return.
	StartLB(port int) error

	// Closes the Load Balancer
	Close()
}
