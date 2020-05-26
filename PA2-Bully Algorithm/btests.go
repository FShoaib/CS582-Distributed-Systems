package main

import (
	"bully"
	"fmt"
	"os"
	"strings"
	"time"
)

const waitBetweenRounds = 1 // number of seconds to wait between rounds
var noNode []int

func main() {

	noNode = []int{}

	testsToRun := []func(){basicTest1, basicTest2, basicTest3, basicTest4, basicTest5, advancedTest1, advancedTest2}
	flags := []string{"basic1", "basic2", "basic3", "basic4", "basic5", "basadvanced1", "basadvanced2"}

	testSet := "no"

	if len(os.Args) == 1 {
		testSet = "bas"
	} else {
		if os.Args[1] == "basic" {
			testSet = "basic"
		} else if os.Args[1] == "advanced" {
			testSet = "advanced"
		} else {
			testSet = os.Args[1]
		}
	}

	for i := 0; i < len(testsToRun); i++ {
		if strings.Contains(flags[i], testSet) {
			testsToRun[i]()
		}
	}
}

func setUp(numNodes int) (chan int, []chan bool, []chan bool, chan int, map[int]chan bully.Message) {
	start := make(chan int, numNodes)
	var quit []chan bool
	var checkLeader []chan bool
	result := make(chan int, numNodes)
	commChannels := make(map[int]chan bully.Message)
	return start, quit, checkLeader, result, commChannels
}

func startUp(numNodes int, quit *[]chan bool, checkLeader *[]chan bool, commChannels map[int]chan bully.Message, start chan int, result chan int) {
	for i := 0; i < numNodes; i++ {
		*quit = append(*quit, make(chan bool, 1))
		*checkLeader = append(*checkLeader, make(chan bool, 1))
		commChannels[i] = make(chan bully.Message, numNodes-1)
	}
	for i := 0; i < numNodes; i++ {
		go bully.Bully(i, numNodes-1, (*checkLeader)[i], commChannels, start, (*quit)[i], result)
	}
}

func crashAndStart(nodes []int, quit []chan bool, start chan int, crashNode []int, roundNum int) {
	for _, i := range nodes {
		// send crashNode quit signal and start the rest of the nodes
		if inSlice(crashNode, i) {
			quit[i] <- true
		} else {
			quit[i] <- false
			start <- roundNum
		}
	}
}

func stallAndStart(nodes []int, quit []chan bool, start chan int, stallNode []int, roundNum int) {
	for _, i := range nodes {
		// skip stallNode and start the rest of the nodes
		if !inSlice(stallNode, i) {
			quit[i] <- false
			start <- roundNum
		}
	}
}

func inSlice(slice []int, item int) bool {
	for _, val := range slice {
		if item == val {
			return true
		}
	}
	return false
}

func emptyChannel(comm map[int]chan bully.Message, pid int) {
	for len(comm[pid]) > 0 {
		<-comm[pid]
	}
}

func emptyDeadChannels(comm map[int]chan bully.Message, deadNodes []int) {
	for _, node := range deadNodes {
		emptyChannel(comm, node)
	}
}

func maxInt(ints []int) int {
	m := ints[0]
	for _, e := range ints {
		if e > m {
			m = e
		}
	}
	return m
}

func checkResult(num int, result chan int, newLeader int) {
	if len(result) < num {
		fmt.Println("\nDid not receive PID of the newly elected leader from all active nodes. Ideally, election should've concluded by now.\n")
		fmt.Println("Test failed.\n")
	} else if len(result) > num {
		fmt.Println("\nReceived PID of the newly elected leader more than once from all active nodes.\n")
		fmt.Println("Test failed.\n")
	} else {
		for i := 0; i < num; i++ {
			if <-result != newLeader {
				fmt.Println("\nAt least one node has not elected the correct node as the leader.\n")
				fmt.Println("Test failed.\n")
				return
			}
		}
		fmt.Println("\nTest passed.\n")
	}
	for len(result) > 0 {
		<-result
	}
}

func shutDown(nodes []int, quit []chan bool) {
	for _, i := range nodes {
		quit[i] <- true
	}
}

func basicTest1() {
	fmt.Println("\n========== Basic Test 1: 3 nodes, 6 rounds ==========")
	fmt.Println("PID of 3 nodes: 0, 1 and 2\n")
	fmt.Println("Description:")
	fmt.Println("1. Leader node (PID 2) stops responding at the start of round 1.")
	fmt.Println("2. A node (PID 1) is asked to check the leader in round 1.\n")

	// setting up the parameters
	const numNodes = 3
	start, quit, checkLeader, result, commChannels := setUp(numNodes)

	// starting up the nodes
	fmt.Println("Starting up the nodes...")
	startUp(numNodes, &quit, &checkLeader, commChannels, start, result)

	// round 1
	fmt.Println("Starting round 1...")
	// ask node 1 to check for the leader
	checkLeader[1] <- true
	// crash the leader and start the round
	deadNodes := []int{2}
	crashAndStart([]int{0, 1, 2}, quit, start, deadNodes, 1)
	aliveNodes := []int{0, 1}
	time.Sleep(time.Second * waitBetweenRounds)

	// round 2-6
	for i := 2; i <= 6; i++ {
		fmt.Printf("Starting round %d...\n", i)
		emptyChannel(commChannels, 2)
		// start the round
		crashAndStart(aliveNodes, quit, start, noNode, i)
		time.Sleep(time.Second * waitBetweenRounds)
	}

	// check result
	checkResult(len(aliveNodes), result, maxInt(aliveNodes))

	// shutdown all nodes
	shutDown(aliveNodes, quit)
	time.Sleep(time.Second * waitBetweenRounds)
}

func basicTest2() {
	fmt.Println("\n\n========== Basic Test 2: 5 nodes, 7 rounds ==========")
	fmt.Println("PID of 5 nodes: 0, 1, 2, 3 and 4\n")
	fmt.Println("Description:")
	fmt.Println("1. Leader node (PID 4) stops responding at the start of round 1.")
	fmt.Println("2. A node (PID 3) stops responding at the start of round 1.")
	fmt.Println("3. A node (PID 1) is asked to check the leader in round 1.")
	fmt.Println("4. A node (PID 0) is asked to check the leader in round 2.\n")

	// setting up the parameters
	const numNodes = 5
	start, quit, checkLeader, result, commChannels := setUp(numNodes)

	// starting up the nodes
	fmt.Println("Starting up the nodes...")
	startUp(numNodes, &quit, &checkLeader, commChannels, start, result)

	// round 1
	fmt.Println("Starting round 1...")
	// ask node 1 to check for the leader
	checkLeader[1] <- true
	// crash the leader and the other node, and start the round
	deadNodes := []int{3, 4}
	crashAndStart([]int{0, 1, 2, 3, 4}, quit, start, deadNodes, 1)
	aliveNodes := []int{0, 1, 2}
	time.Sleep(time.Second * waitBetweenRounds)

	// round 2
	fmt.Println("Starting round 2...")
	emptyDeadChannels(commChannels, deadNodes)
	// ask node 0 to check for the leader
	checkLeader[0] <- true
	// start the round
	crashAndStart(aliveNodes, quit, start, noNode, 2)
	time.Sleep(time.Second * waitBetweenRounds)

	// round 3-7
	for i := 3; i <= 7; i++ {
		fmt.Printf("Starting round %d...\n", i)
		emptyDeadChannels(commChannels, deadNodes)
		// start the round
		crashAndStart(aliveNodes, quit, start, noNode, i)
		time.Sleep(time.Second * waitBetweenRounds)
	}

	// check result
	checkResult(len(aliveNodes), result, maxInt(aliveNodes))

	// shutdown all nodes
	shutDown(aliveNodes, quit)
	time.Sleep(time.Second * waitBetweenRounds)
}

func basicTest3() {
	fmt.Println("\n\n========== Basic Test 3: 5 nodes, 11 rounds ==========")
	fmt.Println("PID of 5 nodes: 0, 1, 2, 3 and 4\n")
	fmt.Println("Description:")
	fmt.Println("1. Leader node (PID 4) stops responding at the start of round 1.")
	fmt.Println("2. A node (PID 2) is asked to check the leader in round 1.")
	fmt.Println("3. A node (PID 3) stops responding at the start of round 5.")
	fmt.Println("4. A node (PID 0) is asked to check the leader in round 5.\n")

	// setting up the parameters
	const numNodes = 5
	start, quit, checkLeader, result, commChannels := setUp(numNodes)

	// starting up the nodes
	fmt.Println("Starting up the nodes...")
	startUp(numNodes, &quit, &checkLeader, commChannels, start, result)

	// round 1
	fmt.Println("Starting round 1...")
	// ask node 2 to check for the leader
	checkLeader[2] <- true
	// crash the leader and start the round
	deadNodes := []int{4}
	crashAndStart([]int{0, 1, 2, 3, 4}, quit, start, deadNodes, 1)
	aliveNodes := []int{0, 1, 2, 3}
	time.Sleep(time.Second * waitBetweenRounds)

	// round 2-4
	for i := 2; i <= 4; i++ {
		fmt.Printf("Starting round %d...\n", i)
		emptyDeadChannels(commChannels, deadNodes)
		// start the round
		crashAndStart(aliveNodes, quit, start, noNode, i)
		time.Sleep(time.Second * waitBetweenRounds)
	}

	// round 5
	fmt.Println("Starting round 5...")
	emptyDeadChannels(commChannels, deadNodes)
	// ask node 0 to check for the leader
	checkLeader[0] <- true
	// crash node with PID 3 and start the round
	deadNodes = append(deadNodes, 3)
	crashAndStart(aliveNodes, quit, start, []int{3}, 5)
	aliveNodes = aliveNodes[:len(aliveNodes)-1]
	time.Sleep(time.Second * waitBetweenRounds)

	// round 6-11
	for i := 6; i <= 11; i++ {
		fmt.Printf("Starting round %d...\n", i)
		emptyDeadChannels(commChannels, deadNodes)
		// start the round
		crashAndStart(aliveNodes, quit, start, noNode, i)
		time.Sleep(time.Second * waitBetweenRounds)
	}

	// check result
	checkResult(len(aliveNodes), result, maxInt(aliveNodes))

	// shutdown all nodes
	shutDown(aliveNodes, quit)
	time.Sleep(time.Second * waitBetweenRounds)
}

func basicTest4() {
	fmt.Println("\n\n========== basic Test 4: 3 nodes, 4 rounds ==========")
	fmt.Println("PID of 3 nodes: 0, 1, and 2\n")
	fmt.Println("Description:")
	fmt.Println("1. Leader node (PID 2) stops responding at the start of round 1.")
	fmt.Println("2. A node (PID 0) is asked to check the leader in round 1.")
	fmt.Println("3. Leader Node (PID 2) comes back online at the start of round 3.\n")

	// setting up the parameters
	const numNodes = 3
	start, quit, checkLeader, result, commChannels := setUp(numNodes)

	// starting up the nodes
	fmt.Println("Starting up the nodes...")
	startUp(numNodes, &quit, &checkLeader, commChannels, start, result)

	// round 1
	fmt.Println("Starting round 1...")
	// ask node 0 to check for the leader
	checkLeader[0] <- true
	// stall the leader and start the round
	aliveNodes := []int{0, 1, 2}
	stallAndStart(aliveNodes, quit, start, []int{2}, 1)
	time.Sleep(time.Second * waitBetweenRounds)

	// round 2
	fmt.Println("Starting round 2...")
	emptyDeadChannels(commChannels, []int{2})
	// stall the leader and start the round
	stallAndStart(aliveNodes, quit, start, []int{2}, 2)
	time.Sleep(time.Second * waitBetweenRounds)

	// round 3
	fmt.Println("Starting round 3...")
	emptyDeadChannels(commChannels, []int{2})
	// start the round
	crashAndStart(aliveNodes, quit, start, noNode, 3)
	time.Sleep(time.Second * waitBetweenRounds)

	// round 4
	fmt.Println("Starting round 4...")
	// start the round
	crashAndStart(aliveNodes, quit, start, noNode, 4)
	time.Sleep(time.Second * waitBetweenRounds)

	// check result
	checkResult(len(aliveNodes), result, maxInt(aliveNodes))

	// shutdown all nodes
	shutDown(aliveNodes, quit)
	time.Sleep(time.Second * waitBetweenRounds)
}

func basicTest5() {
	fmt.Println("\n\n========== Basic Test 5: 3 nodes, 7 rounds ==========")
	fmt.Println("PID of 3 nodes: 0, 1 and 2\n")
	fmt.Println("Description:")
	fmt.Println("1. Leader node (PID 2) stops responding at the start of round 1.")
	fmt.Println("2. A node (PID 1) is asked to check the leader in round 1.")
	fmt.Println("3. Node with PID 2 comes back online at the start of round 6.\n")

	// setting up the parameters
	const numNodes = 3
	start, quit, checkLeader, result, commChannels := setUp(numNodes)

	// starting up the nodes
	fmt.Println("Starting up the nodes...")
	startUp(numNodes, &quit, &checkLeader, commChannels, start, result)

	// round 1
	fmt.Println("Starting round 1...")
	// ask node 1 to check for the leader
	checkLeader[1] <- true
	// stall the leader and start the round
	stalledNodes := []int{2}
	aliveNodes := []int{0, 1, 2}
	stallAndStart(aliveNodes, quit, start, stalledNodes, 1)
	time.Sleep(time.Second * waitBetweenRounds)

	// round 2-5
	for i := 2; i <= 5; i++ {
		fmt.Printf("Starting round %d...\n", i)
		emptyChannel(commChannels, 2)
		// start the round
		stallAndStart(aliveNodes, quit, start, stalledNodes, i)
		time.Sleep(time.Second * waitBetweenRounds)
	}

	// round 6
	fmt.Println("Starting round 6...")
	emptyChannel(commChannels, 2)
	// start the round
	stallAndStart(aliveNodes, quit, start, noNode, 6)
	time.Sleep(time.Second * waitBetweenRounds)

	// check result
	checkResult(len(aliveNodes[:2]), result, maxInt(aliveNodes[:2]))

	// round 7
	fmt.Println("Starting round 7...")
	// start the round
	stallAndStart(aliveNodes, quit, start, noNode, 7)
	time.Sleep(time.Second * waitBetweenRounds)

	// check result
	checkResult(len(aliveNodes), result, maxInt(aliveNodes))

	// shutdown all nodes
	shutDown(aliveNodes, quit)
	time.Sleep(time.Second * waitBetweenRounds)
}

func advancedTest1() {
	fmt.Println("\n\n========== Advanced Test 1: 10 nodes, 12 rounds ==========")
	fmt.Println("PID of 10 nodes: 0, 1, ..., 8 and 9\n")
	fmt.Println("Description:")
	fmt.Println("1. Leader node (PID 9) stops responding at the start of round 1.")
	fmt.Println("2. A node (PID 4) is asked to check the leader in round 1.")
	fmt.Println("3. A node (PID 1) is asked to check the leader in round 6.")
	fmt.Println("4. A node (PID 7) is asked to check the leader in round 7.")
	fmt.Println("5. New leader node (PID 8) stops responding at the start of round 8.\n")

	// setting up the parameters
	const numNodes = 10
	start, quit, checkLeader, result, commChannels := setUp(numNodes)

	// starting up the nodes
	fmt.Println("Starting up the nodes...")
	startUp(numNodes, &quit, &checkLeader, commChannels, start, result)

	// round 1
	fmt.Println("Starting round 1...")
	// ask node 4 to check for the leader
	checkLeader[4] <- true
	// crash the leader and start the round
	deadNodes := []int{9}
	aliveNodes := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	crashAndStart(aliveNodes, quit, start, deadNodes, 1)
	aliveNodes = aliveNodes[:len(aliveNodes)-1]
	time.Sleep(time.Second * waitBetweenRounds)

	// round 2-5
	for i := 2; i <= 5; i++ {
		fmt.Printf("Starting round %d...\n", i)
		emptyDeadChannels(commChannels, deadNodes)
		// start the round
		crashAndStart(aliveNodes, quit, start, noNode, i)
		time.Sleep(time.Second * waitBetweenRounds)
	}

	// round 6
	fmt.Println("Starting round 6...")
	emptyDeadChannels(commChannels, deadNodes)
	// ask node 1 to check for the leader
	checkLeader[1] <- true
	// start the round
	crashAndStart(aliveNodes, quit, start, noNode, 6)
	time.Sleep(time.Second * waitBetweenRounds)

	// round 7
	fmt.Println("Starting round 7...")
	emptyDeadChannels(commChannels, deadNodes)
	// ask node 7 to check for the leader
	checkLeader[7] <- true
	// start the round
	crashAndStart(aliveNodes, quit, start, noNode, 7)
	time.Sleep(time.Second * waitBetweenRounds)

	// check result
	checkResult(len(aliveNodes), result, maxInt(aliveNodes))

	// round 8
	fmt.Println("Starting round 8...")
	deadNodes = append(deadNodes, 8)
	emptyDeadChannels(commChannels, deadNodes)
	// crash the new leader and start the round
	crashAndStart(aliveNodes, quit, start, []int{8}, 8)
	aliveNodes = aliveNodes[:len(aliveNodes)-1]
	time.Sleep(time.Second * waitBetweenRounds)

	// round 9-12
	for i := 9; i <= 12; i++ {
		fmt.Printf("Starting round %d...\n", i)
		emptyDeadChannels(commChannels, deadNodes)
		// start the round
		crashAndStart(aliveNodes, quit, start, noNode, i)
		time.Sleep(time.Second * waitBetweenRounds)
	}

	// check result
	checkResult(len(aliveNodes), result, maxInt(aliveNodes))

	// shutdown all nodes
	shutDown(aliveNodes, quit)
	time.Sleep(time.Second * waitBetweenRounds)
}

func advancedTest2() {
	fmt.Println("\n\n========== Advanced Test 2: 10 nodes, 18 rounds ==========")
	fmt.Println("PID of 10 nodes: 0, 1, ..., 8 and 9\n")
	fmt.Println("Description:")
	fmt.Println("1. Leader node (PID 9) stops responding at the start of round 1.")
	fmt.Println("2. A node (PID 4) is asked to check the leader in round 1.")
	fmt.Println("3. A node (PID 1) is asked to check the leader in round 6.")
	fmt.Println("4. A node (PID 7) is asked to check the leader in round 7.")
	fmt.Println("5. New leader node (PID 8) stops responding at the start of round 8.")
	fmt.Println("6. A node (PID 2) stops responding at the start of round 8.")
	fmt.Println("7. New leader node (PID 7) stops responding at the start of round 12.")
	fmt.Println("8. Nodes (PID 4, 5 and 6) stop responding at the start of round 12.")
	fmt.Println("9. A node (PID 0) is asked to check the leader in round 12.\n")

	// setting up the parameters
	const numNodes = 10
	start, quit, checkLeader, result, commChannels := setUp(numNodes)

	// starting up the nodes
	fmt.Println("Starting up the nodes...")
	startUp(numNodes, &quit, &checkLeader, commChannels, start, result)

	// round 1
	fmt.Println("Starting round 1...")
	// ask node 4 to check for the leader
	checkLeader[4] <- true
	// crash the leader and start the round
	deadNodes := []int{9}
	aliveNodes := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	crashAndStart(aliveNodes, quit, start, deadNodes, 1)
	aliveNodes = aliveNodes[:len(aliveNodes)-1]
	time.Sleep(time.Second * waitBetweenRounds)

	// round 2-5
	for i := 2; i <= 5; i++ {
		fmt.Printf("Starting round %d...\n", i)
		emptyDeadChannels(commChannels, deadNodes)
		// start the round
		crashAndStart(aliveNodes, quit, start, noNode, i)
		time.Sleep(time.Second * waitBetweenRounds)
	}

	// round 6
	fmt.Println("Starting round 6...")
	emptyDeadChannels(commChannels, deadNodes)
	// ask node 1 to check for the leader
	checkLeader[1] <- true
	// start the round
	crashAndStart(aliveNodes, quit, start, noNode, 6)
	time.Sleep(time.Second * waitBetweenRounds)

	// round 7
	fmt.Println("Starting round 7...")
	emptyDeadChannels(commChannels, deadNodes)
	// ask node 7 to check for the leader
	checkLeader[7] <- true
	// start the round
	crashAndStart(aliveNodes, quit, start, noNode, 7)
	time.Sleep(time.Second * waitBetweenRounds)

	// check result
	checkResult(len(aliveNodes), result, maxInt(aliveNodes))

	// round 8
	fmt.Println("Starting round 8...")
	deadNodes = append(deadNodes, 2, 8)
	emptyDeadChannels(commChannels, deadNodes)
	// crash the new leader and node 2, and start the round
	crashAndStart(aliveNodes, quit, start, []int{2, 8}, 8)
	aliveNodes = aliveNodes[:len(aliveNodes)-1]
	aliveNodes = append(aliveNodes[:2], aliveNodes[3:]...)
	time.Sleep(time.Second * waitBetweenRounds)

	// round 9-11
	for i := 9; i <= 11; i++ {
		fmt.Printf("Starting round %d...\n", i)
		emptyDeadChannels(commChannels, deadNodes)
		// start the round
		crashAndStart(aliveNodes, quit, start, noNode, i)
		time.Sleep(time.Second * waitBetweenRounds)
	}

	// round 12
	fmt.Println("Starting round 12...")
	deadNodes = append(deadNodes, 4, 5, 6, 7)
	emptyDeadChannels(commChannels, deadNodes)
	// ask node 0 to check for the leader
	checkLeader[0] <- true
	// crash the new leader and nodes 4, 5 and 6, and start the round
	crashAndStart(aliveNodes, quit, start, []int{4, 5, 6, 7}, 12)
	aliveNodes = aliveNodes[:3]
	time.Sleep(time.Second * waitBetweenRounds)

	// check result
	checkResult(len(aliveNodes), result, 7)

	// round 13-18
	for i := 13; i <= 18; i++ {
		fmt.Printf("Starting round %d...\n", i)
		emptyDeadChannels(commChannels, deadNodes)
		// start the round
		crashAndStart(aliveNodes, quit, start, noNode, i)
		time.Sleep(time.Second * waitBetweenRounds)
	}

	// check result
	checkResult(len(aliveNodes), result, maxInt(aliveNodes))

	// shutdown all nodes
	shutDown(aliveNodes, quit)
	time.Sleep(time.Second * waitBetweenRounds)
}
