package main

import (
	"fmt"
	"log"
	"math/rand"
	"sort"
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

const ProcNum = 10

type MsgType int

const (
	Req = iota
	Rel
	Ack
)

type Message struct {
	Process   int
	Timestamp int
	Type      MsgType
}

func (r *Message) String() string {
	switch r.Type {
	case Rel:
		return fmt.Sprintf("%d: PID %d (Release)", r.Timestamp, r.Process)
	case Ack:
		return fmt.Sprintf("%d: PID %d (Ack)", r.Timestamp, r.Process)
	case Req:
		return fmt.Sprintf("%d: PID %d (Request)", r.Timestamp, r.Process)
	}
	return ""
}

type Process struct {
	ID          int
	Queue       []*Message
	In          chan *Message
	CurrentTime int
	SentTime    []int
	RecvTime    []int
}

func NewProcess(id int) *Process {
	return &Process{
		ID:       id,
		In:       make(chan *Message, 100),
		SentTime: make([]int, ProcNum),
		RecvTime: make([]int, ProcNum),
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

		p.RecvTime[r.Process] = r.Timestamp
		if r.Timestamp > p.CurrentTime {
			p.CurrentTime = r.Timestamp
		}

		switch r.Type {
		case Rel:
			p.removeFromQueue(r.Process)
		case Req:
			p.Queue = append(p.Queue, r)

			// This acknowledgment message need not be sent
			// if Pj has already sent a message to Pi timestamped later than Tm.
			if p.SentTime[r.Process] <= r.Timestamp {
				ts := p.time()
				procs[r.Process].In <- &Message{Process: p.ID, Timestamp: ts, Type: Ack}
				p.SentTime[r.Process] = ts
			}
		}
		p.checkOwnership()
	}
}

func (p *Process) RequestResource() {
	fmt.Printf("%d: Process %d requesting the resource\n", p.CurrentTime, p.ID)
	ts := p.time()
	r := &Message{Process: p.ID, Timestamp: ts, Type: Req}
	for i, c := range procs {
		if i != p.ID {
			c.In <- r
			p.SentTime[i] = ts
		}
	}
	p.Queue = append(p.Queue, r)
}

func (p *Process) ReleaseResource() {
	fmt.Printf("%d: Process %d releasing the resource\n", p.CurrentTime, p.ID)
	ts := p.time()
	r := &Message{Process: p.ID, Timestamp: ts, Type: Rel}
	for i, c := range procs {
		if i != p.ID {
			c.In <- r
			p.SentTime[i] = ts
		}
	}
	p.removeFromQueue(p.ID)
}

func (p *Process) removeFromQueue(id int) {
	var indices []int
	for i, r := range p.Queue {
		if r.Process == id {
			indices = append(indices, i)
		}
	}
	for i, index := range indices {
		p.Queue = append(p.Queue[:index-i], p.Queue[index-i+1:]...)
	}
}

func (p *Process) checkOwnership() {
	if len(p.Queue) == 0 {
		fmt.Printf("%d: No request in PID %d\n", p.CurrentTime, p.ID)
		return
	}

	// Total ordering
	sort.Slice(p.Queue, func(i, j int) bool {
		if p.Queue[i].Timestamp != p.Queue[j].Timestamp {
			return p.Queue[i].Timestamp < p.Queue[j].Timestamp
		}
		// Break tie through ordering processes by PID
		return p.Queue[i].Process < p.Queue[j].Process
	})

	request := p.Queue[0]
	if request == nil || request.Process != p.ID {
		return
	}

	fmt.Printf("%d: PID %d has a pending request\n", p.CurrentTime, p.ID)

	ReceivedFromAll := true
	for Proc, LastRecv := range p.RecvTime {
		if Proc == p.ID {
			continue
		}
		// If Pi ≺ Pj , then Pi need only have received a message timestamped ≥ Tm from Pj .
		if p.ID < Proc && LastRecv < request.Timestamp {
			ReceivedFromAll = false
		}
		if p.ID > Proc && LastRecv <= request.Timestamp {
			ReceivedFromAll = false
		}
	}
	if ReceivedFromAll {
		fmt.Printf("%d: PID %d has the resource\n", p.CurrentTime, p.ID)
	} else {
		fmt.Printf("%d: PID %d hasn't heard from all peers\n", p.CurrentTime, p.ID)
	}
}

func main() {
	for i := 0; i < ProcNum; i++ {
		procs = append(procs, NewProcess(i))
	}
	for i := 0; i < ProcNum; i++ {
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
		case "demo":
			for id := ProcNum - 1; id >= 0; id-- {
				procs[id].RequestResource()
			}
			time.Sleep(5 * time.Second)
			for id := 0; id <= i; id++ {
				procs[id].ReleaseResource()
			}
		case "debug":
			fmt.Printf("Current Time: %d\n", procs[i].CurrentTime)
			fmt.Println("Message Queue")
			for _, r := range procs[i].Queue {
				fmt.Println(r)
			}
			fmt.Println("Message Receive Time")
			for id, t := range procs[i].RecvTime {
				fmt.Printf("PID %d @ %d\n", id, t)
			}
			fmt.Println("Message Sent Time")
			for id, t := range procs[i].SentTime {
				fmt.Printf("PID %d @ %d\n", id, t)
			}
		}
		time.Sleep(1 * time.Millisecond)
	}
}
