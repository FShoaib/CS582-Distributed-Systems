package rpcs

// RemoteMME - Students should not use this interface in their code. Use WrapMME() instead.
type RemoteMME interface {
	RecvUERequest(args *UERequestArgs, reply *UERequestReply) error
	RecvMMEStats(args *MMEStatsArgs, reply *MMEStatsReply) error
	// TODO: add additional RPC signatures below!
	RetrieveState(args *RetrieveArgs, reply *RetrieveReply) error
	ResetState(args *ResetArgs, reply *ResetReply) error
	Relocate(args *RelocateArgs, reply *RelocateReply) error
	RecvReplicas(args *ReplicaArgs, reply *ReplicaReply) error
}

// RemoteLoadBalancer - Students should not use this interface in their code. Use WrapLB() instead.
type RemoteLoadBalancer interface {
	RecvUERequest(args *UERequestArgs, reply *UERequestReply) error
	RecvLeave(args *LeaveArgs, reply *LeaveReply) error
	RecvLBStats(args *LBStatsArgs, reply *LBStatsReply) error
	// TODO: add additional RPC signatures below!
	JoinRPC(args *JoinArgs, reply *JoinReply) error
}

// MME ...
type MME struct {
	// Embed all methods into the struct. See the Effective Go section about
	// embedding for more details: golang.org/doc/effective_go.html#embedding
	RemoteMME
}

// LoadBalancer ...
type LoadBalancer struct {
	// Embed all methods into the struct. See the Effective Go section about
	// embedding for more details: golang.org/doc/effective_go.html#embedding
	RemoteLoadBalancer
}

// WrapMME wraps t in a type-safe wrapper struct to ensure that only the desired
// methods are exported to receive RPCs. Any other methods already in the
// input struct are protected from receiving RPCs.
func WrapMME(t RemoteMME) RemoteMME {
	return &MME{t}
}

// WrapLoadBalancer wraps t in a type-safe wrapper struct to ensure that only the desired
// methods are exported to receive RPCs. Any other methods already in the
// input struct are protected from receiving RPCs.
func WrapLoadBalancer(t RemoteLoadBalancer) RemoteLoadBalancer {
	return &LoadBalancer{t}
}
