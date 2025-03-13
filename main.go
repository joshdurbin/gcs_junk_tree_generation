package main

import (
	"cloud.google.com/go/storage"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const maxCharSizeFile = 32
const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
const workerCount = 250
const queueSize = 100_000
const printGeneratorActivity = false
const printWorkerActivity = false
const lowProbabilityFileCount = 50_000_000
const lowProbabilityFileCountUpperBoundForRandGenerator = 1_000
const createObjects = true
const progressTickerDuration = 30 * time.Second

func main() {

	startTime := time.Now()
	var totalObjectsCreated int64
	var totalObjectsSkipped int64

	ctx, cancel := context.WithCancel(context.Background())

	// propagate context cancellation
	term := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-term
		_, _ = fmt.Fprintf(os.Stderr, "shutting down...\n")
		cancel()
	}()

	go func(totalObjectsCreated, totalObjectsSkipped *int64) {
		ticker := time.NewTicker(progressTickerDuration)
		for {
			select {
			case <-ctx.Done():
				return
			case _ = <-ticker.C:
				fmt.Printf("created %v objects and skipped %v objects\n", atomic.LoadInt64(totalObjectsCreated), atomic.LoadInt64(totalObjectsSkipped))
			}
		}
	}(&totalObjectsCreated, &totalObjectsSkipped)

	// create a storage client
	client, err := storage.NewClient(ctx)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error creating storage client: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	bucket := client.Bucket("gcs-load-testing")

	// create a queue and a wait group
	queue := make(chan string, queueSize)
	var wg sync.WaitGroup
	wg.Add(workerCount)

	// start workers
	for i := 0; i < workerCount; i++ {
		go worker(ctx, i+1, queue, &wg, bucket, &totalObjectsCreated, &totalObjectsSkipped)
	}

	// start the tree loop
	treeloop(ctx, queue, 0, 4, "")

	// close the queue and wait for the workers to finish
	close(queue)

	// wait for the workers to finish
	wg.Wait()

	// cancel the context
	cancel()

	fmt.Printf("created %v objects and skipped %v objects in %v\n", atomic.LoadInt64(&totalObjectsCreated), atomic.LoadInt64(&totalObjectsSkipped), time.Since(startTime))
}

func treeloop(ctx context.Context, queue chan<- string, depth, maxDepth int, prefix string) {

	// base condition: Reached the maximum depth, do something with the current prefix
	if depth == maxDepth {

		// low probability of creating a large number of files
		if rand.Intn(lowProbabilityFileCountUpperBoundForRandGenerator) < 1 {
			for i := 0; i < lowProbabilityFileCount; i++ {
				path := fmt.Sprintf("%v/%d", prefix, i)
				select {
				case queue <- path:
					if printGeneratorActivity {
						fmt.Printf("queued object %v\n", path)
					}
				case <-ctx.Done():
					return
				}
			}
		}

		// queue objects for the current prefix
		select {
		case queue <- prefix:
			if printGeneratorActivity {
				fmt.Printf("queued object %v\n", prefix)
			}
			return
		case <-ctx.Done():
			return
		}
	}

	for i := 'a'; i <= 'z'; i++ {
		// recurse with the new prefix
		if len(prefix) == 0 {
			treeloop(ctx, queue, depth+1, maxDepth, fmt.Sprintf("%c", i))
		} else {
			treeloop(ctx, queue, depth+1, maxDepth, fmt.Sprintf("%v/%c", prefix, i))
		}
	}
}

func worker(ctx context.Context, id int, queue <-chan string, wg *sync.WaitGroup, bucket *storage.BucketHandle, totalObjectsCreated, totalObjectsSkipped *int64) {
	defer wg.Done()
	for {
		select {
		case msg, ok := <-queue:
			// if the channel is closed, return
			if !ok {
				_, _ = fmt.Fprintf(os.Stderr, "worker %d shutting down due to channel closure\n", id)
				return
			}

			if createObjects {

				// get object handle
				object := bucket.Object(msg)

				// check if the object exists
				_, err := object.Attrs(ctx)
				if err != nil && errors.Is(err, storage.ErrObjectNotExist) {

					// if the object does not exist, create a new object
					writer := object.NewWriter(ctx)
					// write random data to the object
					b := make([]byte, rand.Intn(maxCharSizeFile))
					for i := range b {
						b[i] = charset[rand.Intn(len(charset))]
					}
					// write the data to the object
					_, err := fmt.Fprintf(writer, string(b), charset)
					// if there was an error, print it and return
					if err != nil {
						_, _ = fmt.Fprintf(os.Stderr, "worker %d encountered error creating object %v: %v\n", id, msg, err)
						return
					}
					// close the writer
					err = writer.Close()
					// if there was an error, print it and return
					if err != nil {
						_, _ = fmt.Fprintf(os.Stderr, "worker %d encountered error closing writer while creating object %v: %v\n", id, msg, err)
						return
					}

					// increment the total objects created
					atomic.AddInt64(totalObjectsCreated, 1)

					// print that the object was created
					if printWorkerActivity {
						fmt.Printf("worker %d created object %v\n", id, msg)
					}
				} else {

					// increment the total objects created
					atomic.AddInt64(totalObjectsSkipped, 1)

					// print that the object was created
					if printWorkerActivity {
						fmt.Printf("worker %d skipped object %v\n", id, msg)
					}
				}
			}

		// if the context is done, return
		case <-ctx.Done():
			_, _ = fmt.Fprintf(os.Stderr, "worker %d shutting down due to context cancellation\n", id)
			return
		}
	}
}
