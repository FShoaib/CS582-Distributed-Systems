// Implementation of a KeyValueServer.

package pa1

import (
	"DS_PA1/rpcs"
	"bufio"
	"bytes"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
)

const maxQueue = 500

type keyValueServer struct {
	// TODO: implement this!
	clientList []*client
	ln         net.Listener
	connChan   chan net.Conn
	msgChan    chan []byte
	states     chan *client
	kvchan     chan *kvmstore
	kvSend     chan *kvmstore
	mainEnd    chan bool
	acceptEnd  chan bool
	toCount    chan bool
	count      chan int
	//RPC
	putkv chan string
	// putv      chan []byte
	getv      chan string
	sendReply chan []byte
}
type client struct {
	connClient net.Conn
	chanQue    chan []byte
	wri        chan bool
	rea        chan bool
}
type kvmstore struct {
	key string
	val []byte
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {
	// TODO: implement this!
	kvs := &keyValueServer{
		clientList: nil,
		ln:         nil,
		connChan:   make(chan net.Conn),
		msgChan:    make(chan []byte),
		states:     make(chan *client),
		kvchan:     make(chan *kvmstore),
		kvSend:     make(chan *kvmstore),
		mainEnd:    make(chan bool),
		acceptEnd:  make(chan bool),
		toCount:    make(chan bool),
		count:      make(chan int),
	}
	return kvs
}

func (kvs *keyValueServer) StartModel1(port int) error {
	// TODO: implement this!
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return err
	}
	initDB()
	kvs.ln = ln
	go handleConn(kvs)

	//accept routine
	go func() {
		for {
			select {
			case <-kvs.acceptEnd:
				return
			default:
				conn, err := kvs.ln.Accept()
				if err == nil {
					kvs.connChan <- conn
				}
			}
		}
	}()
	return nil
}

func (kvs *keyValueServer) Close() {
	// TODO: implement this!
	kvs.ln.Close()
	kvs.mainEnd <- true
	kvs.acceptEnd <- true
}

func (kvs *keyValueServer) Count() int {
	// TODO: implement this!
	kvs.toCount <- true
	return <-kvs.count

}

func (kvs *keyValueServer) StartModel2(port int) error {
	// TODO: implement this!
	//
	// Do not forget to call rpcs.Wrap(...) on your kvs struct before
	// passing it to <sv>.Register(...)
	//
	// Wrap ensures that only the desired methods (RecvGet and RecvPut)
	// are available for RPC access. Other KeyValueServer functions
	// such as Close(), StartModel1(), etc. are forbidden for RPCs.
	//
	// Example: <sv>.Register(rpcs.Wrap(kvs))
	//

	ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	kvs.ln = ln
	if err != nil {
		return err
	}
	kvs.putkv = make(chan string)
	kvs.getv = make(chan string)
	kvs.sendReply = make(chan []byte)
	initDB()
	go kvs.gpRPC()

	rpcServer := rpc.NewServer()
	rpcServer.Register(rpcs.Wrap(kvs))

	http.DefaultServeMux = http.NewServeMux()
	rpcServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)

	go http.Serve(ln, nil)
	return nil

}

func (kvs *keyValueServer) RecvGet(args *rpcs.GetArgs, reply *rpcs.GetReply) error {
	kvs.getv <- args.Key

	reply.Value = <-kvs.sendReply
	return nil
}

func (kvs *keyValueServer) RecvPut(args *rpcs.PutArgs, reply *rpcs.PutReply) error {
	kvs.putkv <- args.Key + "," + string(args.Value)
	return nil
}

func (kvs *keyValueServer) gpRPC() {
	for {
		select {
		case toPut := <-kvs.putkv:
			output := bytes.Split([]byte(toPut), []byte(","))
			putKey := string(output[0])
			putVal := []byte(output[1])

			put(putKey, putVal)

		case toGet := <-kvs.getv:
			getVal := get(toGet)
			kvs.sendReply <- getVal
		}
	}
}
func handleConn(kvs *keyValueServer) {

	for {
		select {
		//add new connection to clientlist
		case conNew := <-kvs.connChan:
			newClient := &client{
				connClient: conNew,
				chanQue:    make(chan []byte, maxQueue),
				rea:        make(chan bool),
				wri:        make(chan bool)}
			kvs.clientList = append(kvs.clientList, newClient)

			//read routine
			go read(kvs, newClient)
			//write to clients
			go func() {
				for {
					select {
					case <-newClient.wri:
						return
					case msg := <-newClient.chanQue:
						newClient.connClient.Write(msg)
					}
				}
			}()
			//run request
		case reqClient := <-kvs.kvchan:
			//get
			if reqClient.val == nil {
				valget := get(reqClient.key)
				tosend := &kvmstore{
					key: reqClient.key,
					val: valget}
				kvs.kvSend <- tosend
			} else {
				put(reqClient.key, reqClient.val)
			}
		//new msg to send to client
		case newMsg := <-kvs.msgChan:
			for _, clientl := range kvs.clientList {
				if len(clientl.chanQue) < maxQueue {
					//continue
					clientl.chanQue <- newMsg
				} else {
					<-clientl.chanQue
				}
			}
			//remove client
		case delClient := <-kvs.states:
			for s, cl := range kvs.clientList {
				if cl == delClient {
					kvs.clientList[s] = kvs.clientList[len(kvs.clientList)-1]
					kvs.clientList = kvs.clientList[:len(kvs.clientList)-1]
					break
				}
			}
		case <-kvs.mainEnd:
			for _, client := range kvs.clientList {
				client.connClient.Close()
				client.rea <- true
				client.wri <- true
			}
			return
		case <-kvs.toCount:
			kvs.count <- len(kvs.clientList)
		}
	}
}

func read(kvs *keyValueServer, newClient *client) {

	rw := connToR(newClient.connClient)
	for {
		select {
		case <-newClient.rea:
			return
		default:
			// get client message
			msg, err := rw.ReadBytes('\n')
			if err != nil {
				kvs.states <- newClient
			} else {
				out1 := parse(msg)

				if string(out1[0]) == "put" {
					newStore := &kvmstore{
						key: string(out1[1]),
						val: out1[2]}
					kvs.kvchan <- newStore
				} else {
					newStore := &kvmstore{
						key: string(out1[1]),
						val: nil}
					kvs.kvchan <- newStore
					final1 := resp(kvs)
					kvs.msgChan <- final1
				}
			}
		}

	}
}

func parse(input []byte) [][]byte {
	out := bytes.Split(input[:len(input)-1], []byte(","))
	return out
}

func connToR(conn net.Conn) *bufio.Reader {
	return bufio.NewReader(conn)
}

func resp(kvs *keyValueServer) []byte {
	respSend := <-kvs.kvSend
	concat := bytes.Join([][]byte{[]byte(respSend.key), respSend.val}, []byte(","))
	final1 := bytes.Join([][]byte{concat, []byte("\n")}, []byte(""))
	return final1
}
