// This file contains the exported, functions implemented by
// a MME. RPC functions implemented by the MME
// are declared in the RemoteMME struct in rpcs/rpc.go
//
// DO NOT MODIFY!

package mme

// MME implements a simple cellular control plane's Mobility Management Entity.
type MME interface {

	// StartMME starts the MME on the given hostPort, starts a TCP listener, embeds
	// the RemoteMME functions into the MME struct and registers RemoteMME methods
	// for RPC-access. StartMME returns an error if there was a problem listening on the
	// specified port or if the Load Balancer did not assign the MME an ID and provide
	// the list of replicas.
	//
	// This method should NOT block. Instead, it should spawn one or more
	// goroutines (to handle things like accepting RPC calls, etc) and then return.
	StartMME(hostPort string, loadBalancer string) error

	// Close shuts down the MME. All connections should be closed immediately
	// and all goroutines should be signaled to return.
	Close()
}
