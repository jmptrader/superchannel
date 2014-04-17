package main

import (
	"fmt"
	"log"

	"github.com/garyburd/redigo/redis"
)

/*

	Concept: SuperChannel - a Redis backed channel

	This idea is to provide a backing store for a channel for the send/receive command.  This in effect allows you to build channels
	that can communicate via inter-process over localhost through redis or even over the network with added latency introduced by
	the given network.

	If built properly, you should be able to build an application and swap out a regular channel with a SuperChannel and all the logic
	and plumbing can *largley* stay the same.  This idea still has a few caveats: It requires a centralized broker (redis) and if your
	redis instance goes down, obviously your channel communication goes down as well.

   - Demonstrate the use of IPC WITH channels but using Redis as a broker (obviously not high-availability and Single Point of Failure "for now")
   - Build solution for closing of the channel, which can send along a special close payload packet
   - Show, how it works within app boundaries as well as across network boundaries
   - Point out how obviously your app will become network bound
   - Send messages pipelined through Redis within a given window
   - Preserve order of messages sent, pointing out a channel in Go is basically a FIFO queue.
   - Create an interface around the channel, which allows for using a third-party solution like IronQueue (IronChannel)
   - TODO: Figure out what interface is common, looks like this may match the Closer interface
   - ISSUE: can't use an interface cause we lose the awesome channel send/receive syntax....hmmmmmm.
   - MAYBE: struct embedding or something will work for this.
   - Purge the queue wihin 40 millisecond window as an example, (the time it takes a TCP roundtrip to occur)
   - TODO: unit test this bitch.
   - FUTURE: consider abstracting out an interface that allows for encoding/decoding methods allowing for sending various payloads in custom formats
   - CONSIDER: what we have currently is the send portion, still need to build something to represent the receive portion from the redis queue
   - TODO: probably add a feature to delete an item from the queue so we can assumme processing is completed on it. (Necessary???)
           if processing fails...the item should maybe go back into the queue?  Something along those lines of logic.
   - FEATURE: we pipeline to write quickly to the queue, consider reading batches from the queue where possible.

*/

func main() {

	rch := Make("gameStream", 5)

	fmt.Println("Result: ", <-rch)
	fmt.Println("Result: ", <-rch)
	fmt.Println("Result: ", <-rch)

	rch.Close()

	//rch <- "It's closed for business!"

	//var s string
	//fmt.Scanln(&s)
	fmt.Println("End of program")
	//log.Panic("See how many goroutines are still alive...")
}

const CLOSE_TOKEN = "closePACKET"

type RedisChannel chan string

func Make(channelName string, bufferSize int) RedisChannel {

	ch := make(RedisChannel, bufferSize)
	ch.process(func() {
		//NOTE: do we really need to grow the buffer and allocate a new one everytime we flush?  Room for optimization?
		//buffer := make([]string, 0, bufferSize)

		//setup Redis, we should probably move this somewhere else, like load it up from a local config file.
		c, err := redis.Dial("tcp", "csp.loopnet.com:6379")
		if err != nil {
			// handle error
			log.Fatal("Couldn't connect to Redis, is Redis running and listening??????")
		}
		defer c.Close()

		//SANITY: do key exists check here to make sure we can read from something that exists???? Perhaps not necessary.
		for {

			//BLPOP: block indefinately until a value becomes available to consume!
			//will block even if the channelName doesn't exist!
			//fmt.Println("waiting...")
			rep, err := redis.Values(c.Do("BLPOP", channelName, 0))

			if err != nil {
				log.Println(err)
				log.Fatalf("Uh no!")

			}

			//fmt.Println("Reply: ")
			//fmt.Println(rep)

			var result string
			var key string
			if _, err = redis.Scan(rep, &key, &result); err != nil {
				log.Println(err)
				log.Fatal("Couldn't parse reply")
			}

			//fmt.Println("Result: ", result)
			ch <- result
		}

		fmt.Println("Closing redis connection from defer statement")
	})
	return ch
}

func (ch RedisChannel) Close() {
	//sends along a special payload that is a message to close
	ch <- CLOSE_TOKEN
}

//NOTE: doing this as a closure to capture some default properties up-top
func (ch RedisChannel) process(runner func()) {
	go func() {
		runner()
	}()
}
