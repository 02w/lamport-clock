package main

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"
)

/*
1. To request the resource, process Pi sends the message Tm:Pi requests resource to every other
   process, and puts that message on its request queue, where Tm is the timestamp of the message.
2. When process Pj receives the message Tm:Pi requests resource, it places it on its request
   queue and sends a (timestamped) acknowledgment message to Pi.
3. To release the resource, process Pi removes any Tm:Pi requests resource message from its
   request queue and sends a (timestamped) Pi "releases resource" message to every other process.
4. When process Pj receives a Pi releases resource message, it removes any Tm:Pi requests
   resource message from its request queue.
5. Process Pi is granted the resource when the following two conditions are satisfied:
	(i) There is a Tm:Pi requests resource message in its request queue which is ordered before any
	    other request in its queue by the relation ⇒ (To define the relation "⇒" for messages, we
			identify a message with the event of sending it.)
	(ii) Pi has received a message from every other process timestamped later than Tm.
	Note that conditions (i) and (ii) of rule 5 are tested locally by Pi.
*/
var procs []*Process

type Request struct {
	Process   int
	Timestamp int
	Release   bool
	Ack       bool
}

func (r *Request) String() string {
	if r.Release {
		return fmt.Sprintf("%d: PID %d (Release)", r.Timestamp, r.Process)
	}
	if r.Ack {
		return fmt.Sprintf("%d: PID %d (Ack)", r.Timestamp, r.Process)
	}
	return fmt.Sprintf("%d: PID %d (Request)", r.Timestamp, r.Process)
}

type Process struct {
	ID          int
	Queue       []*Request
	In          chan *Request
	CurrentTime int
}

func NewProcess(id int) *Process {
	return &Process{
		ID: id,
		In: make(chan *Request, 100),
	}
}

func (p *Process) time() int {
	p.CurrentTime++
	return p.CurrentTime
}

func (p *Process) Start() {
	for {
		r := <-p.In
		time.Sleep(time.Duration(100+rand.Intn(900)) * time.Millisecond)
		if r.Timestamp > p.CurrentTime {
			p.CurrentTime = r.Timestamp
		}
		if r.Release {
			p.removeFromQueue(r.Process)
		} else if r.Ack {
			p.Queue = append(p.Queue, r)
		} else {
			p.Queue = append(p.Queue, r)
			procs[r.Process].In <- &Request{Process: p.ID, Timestamp: p.time(), Ack: true}
		}
		p.checkOwnership()
	}
}

func (p *Process) RequestResource() {
	fmt.Printf("%d: Process %d requesting the resource\n", p.CurrentTime, p.ID)
	r := &Request{Process: p.ID, Timestamp: p.time()}
	for i, c := range procs {
		if i != p.ID {
			c.In <- r
		}
	}
	p.Queue = append(p.Queue, r)
}

func (p *Process) ReleaseResource() {
	fmt.Printf("%d: Process %d releasing the resource\n", p.CurrentTime, p.ID)
	r := &Request{Process: p.ID, Timestamp: p.time(), Release: true}
	for i, c := range procs {
		if i != p.ID {
			c.In <- r
		}
	}
	p.removeFromQueue(p.ID)
}

func (p *Process) removeFromQueue(id int) {
	var indices []int
	for i, r := range p.Queue {
		if r.Process == id && !r.Ack {
			indices = append(indices, i)
		}
	}
	for i, index := range indices {
		p.Queue = append(p.Queue[:index-i], p.Queue[index-i+1:]...)
	}
}

func (p *Process) checkOwnership() {
	var request *Request
	for _, r := range p.Queue {
		if r.Process == p.ID {
			request = r
			break
		}
	}
	if request == nil {
		return
	}
	fmt.Printf("%d: PID %d has a pending request\n", p.CurrentTime, p.ID)
	earliestRequest := true
	rcpt := make(map[int]bool)
	for _, r := range p.Queue {
		if r.Timestamp < request.Timestamp {
			earliestRequest = false
		}
		if r.Timestamp > request.Timestamp {
			rcpt[r.Process] = true
		}
	}
	if !earliestRequest {
		fmt.Printf("%d: PID %d doesn't have the earliest request\n", p.CurrentTime, p.ID)
	}
	if len(rcpt) < len(procs)-1 {
		fmt.Printf("%d: PID %d hasn't heard from all peers\n", p.CurrentTime, p.ID)
	}
	if earliestRequest && len(rcpt) == len(procs)-1 {
		fmt.Printf("%d: PID %d has the resource\n", p.CurrentTime, p.ID)
	}
}

func main() {
	for i := 0; i < 10; i++ {
		procs = append(procs, NewProcess(i))
	}
	for i := 0; i < 10; i++ {
		go procs[i].Start()
	}

	var command, pid string
	for {
		fmt.Print("> ")
		if _, err := fmt.Scanln(&command, &pid); err != nil {
			log.Fatal(err)
		}
		if command == "exit" {
			return
		}
		i, err := strconv.Atoi(pid)
		if err != nil {
			log.Fatal(err)
		}
		switch command {
		case "request":
			procs[i].RequestResource()
		case "release":
			procs[i].ReleaseResource()
		case "debug":
			for _, r := range procs[i].Queue {
				fmt.Println(r)
			}
		}
		time.Sleep(1 * time.Millisecond)
	}
}
