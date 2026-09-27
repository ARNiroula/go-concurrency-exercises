//////////////////////////////////////////////////////////////////////
//
// Given is a producer-consumer scenario, where a producer reads in
// tweets from a mockstream and a consumer is processing the
// data. Your task is to change the code so that the producer as well
// as the consumer can run concurrently
//

// Solution:
// Create a broker channel with *Tweet: Use that to stream data between Consumer and Producer
// Add 2 WaitGroup for Producer and Consumer
// Then simply send the data from producer to consumer
// NOTE: close the broker in the producer after its consumed all the data
//

package main

import (
	"fmt"
	"sync"
	"time"
)

var broker = make(chan *Tweet)

func producer(stream Stream, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(broker)
	for {
		tweet, err := stream.Next()
		if err == ErrEOF {
			return
		}
		broker <- tweet
	}
}

func consumer(wg *sync.WaitGroup) {
	defer wg.Done()
	for t := range broker {
		if t.IsTalkingAboutGo() {
			fmt.Println(t.Username, "\ttweets about golang")
		} else {
			fmt.Println(t.Username, "\tdoes not tweet about golang")
		}
	}
}

func main() {
	start := time.Now()

	stream := GetMockStream()
	var wg sync.WaitGroup

	// Producer
	// tweets := producer(stream)
	wg.Add(1)
	go producer(stream, &wg)

	// Consumer
	wg.Add(1)
	go consumer(&wg)

	wg.Wait()
	fmt.Printf("Process took %s\n", time.Since(start))
}
