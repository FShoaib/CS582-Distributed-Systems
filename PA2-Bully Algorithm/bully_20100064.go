/*defined 4 types of messages - election, OK, WINNER and ALIVE
wait is used to check the t+2 condition. It waits for max 2 rounds to recieve reply
if msg type is ELECTION, it checks if pid is greater than the current pid in loop. If yes,
then pid sends OK to current loop pid.
pid then checks if it is the highest node or not. IF it is, then it becomes the leader and sends the WINNER
msg to all.
otherwie, it checks for pids higher than itself and sends them an ELECTION msg
*/

package bully

import (
	"sync"
)

// msgType represents the type of messages that can be passed between nodes
type msgType int

// customized messages to be used for communication between nodes
const (
	ELECTION msgType = iota
	OK
	WINNER
	ALIVE
	// TODO: add / change message types as needed
)

// Message is used to communicate with the other nodes
// DO NOT MODIFY THIS STRUCT
type Message struct {
	Pid   int
	Round int
	Type  msgType
}

// Bully runs the bully algorithm for leader election on a node
func Bully(pid int, leader int, checkLeader chan bool, comm map[int]chan Message, startRound <-chan int, quit <-chan bool, electionResult chan<- int) {

	// TODO: initialization code
	startElec := false
	reply := false
	lastAlive := 1
	wait := 0
	ifLeader := 0
	var mutex = &sync.Mutex{}

	for {

		// quit / crash the program
		if <-quit {
			return
		}
		// start the round
		roundNum := <-startRound
		// TODO: bully algorithm code
		if roundNum-lastAlive >= 2 {
			highest := checkHighest(comm, pid)
			if highest {
				//send WINNER msg
				for i := range comm {
					comm[i] <- Message{Pid: pid, Round: roundNum, Type: WINNER}
				}
			} else {
				startElec = true
				wait = roundNum
				electionMsg(comm, pid, roundNum)
			}
		}
		lastAlive = roundNum

		if len(checkLeader) > 0 {
			//check if leader responsive
			<-checkLeader
			comm[leader] <- Message{Pid: pid, Round: roundNum, Type: ALIVE}
			wait = roundNum
			ifLeader = 1
		}

		msgList := getMessages(comm[pid], roundNum-1)

		for ind := range msgList {

			//conduct election
			if msgList[ind].Type == ELECTION {
				//check if pid greater than current loop pid,send ok if yes
				if pid > msgList[ind].Pid {

					if ifLeader == 1 {
						ifLeader = 0
					}
					if startElec {
						mutex.Lock()
						comm[msgList[ind].Pid] <- Message{Pid: pid, Round: roundNum, Type: OK}
						mutex.Unlock()
						continue
					}
					mutex.Lock()
					comm[msgList[ind].Pid] <- Message{Pid: pid, Round: roundNum, Type: OK}
					mutex.Unlock()

					//find all nodes greater than this one
					ifHighest := checkHighest(comm, pid)

					if ifHighest {
						leader = pid
						//send WINNER msg to all
						for i := range comm {
							comm[i] <- Message{Pid: pid, Round: roundNum, Type: WINNER}
						}
						continue
					}
					startElec = true
					reply = false
					wait = roundNum

					mutex.Lock()
					comm[msgList[ind].Pid] <- Message{Pid: pid, Round: roundNum, Type: OK}
					mutex.Unlock()

					electionMsg(comm, pid, roundNum)
				}
			} else if msgList[ind].Type == WINNER {

				leader = msgList[ind].Pid
				electionResult <- msgList[ind].Pid

			} else if msgList[ind].Type == OK {
				startElec = false
				reply = true
			}

		}

		//node dead
		diff1 := roundNum - wait
		if diff1 >= 2 && ifLeader == 1 {
			startElec = true
			ifLeader = 0
			wait = roundNum
			electionMsg(comm, pid, roundNum)
		}

		diff2 := roundNum - wait
		if !reply && startElec && diff2 >= 2 {
			leader = pid
			startElec = false
			//send winner msg
			for i := range comm {
				comm[i] <- Message{Pid: pid, Round: roundNum, Type: WINNER}
			}
		}

	}
}

// assumes messages from previous rounds have been read already
func getMessages(msgs chan Message, round int) []Message {
	var result []Message

	// check if there are messages to be read
	if len(msgs) > 0 {
		var m Message

		// read all messages belonging to the corresponding round
		for m = <-msgs; m.Round == round; m = <-msgs {
			result = append(result, m)
			if len(msgs) == 0 {
				break
			}
		}

		// insert back message from any other round
		if m.Round != round {
			msgs <- m
		}
	}
	return result
}

// TODO: helper functions
func checkHighest(comm map[int]chan Message, pid int) bool {

	for i := range comm {
		if i > pid {
			return false
		}
	}
	return true
}

func nodesGreater(comm map[int]chan Message, pid int) []int {
	var greater []int
	for i := range comm {
		if pid < i {
			greater = append(greater, i)
		}
	}
	return greater
}

func electionMsg(comm map[int]chan Message, pid int, roundNum int) {
	greater := nodesGreater(comm, pid)
	for ind := range greater {
		comm[greater[ind]] <- Message{Pid: pid, Round: roundNum, Type: ELECTION}
	}
}
