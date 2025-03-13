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

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Config holds all the configuration values
type Config struct {
	MaxCharSizeFile                               int
	Charset                                       string
	WorkerCount                                   int
	QueueSize                                     int
	PrintGeneratorActivity                        bool
	PrintWorkerActivity                           bool
	LowProbabilityFileCount                       int
	LowProbabilityFileCountUpperBoundForRandGenerator int
	CreateObjects                                 bool
	ProgressTickerDuration                        time.Duration
	BucketName                                    string
	MaxDepth                                      int
}

var (
	cfgFile string
	config  Config
)

func initConfig() {
	// Set default values
	viper.SetDefault("maxCharSizeFile", 32)
	viper.SetDefault("charset", "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	viper.SetDefault("workerCount", 250)
	viper.SetDefault("queueSize", 100000)
	viper.SetDefault("printGeneratorActivity", false)
	viper.SetDefault("printWorkerActivity", false)
	viper.SetDefault("lowProbabilityFileCount", 50000000)
	viper.SetDefault("lowProbabilityFileCountUpperBoundForRandGenerator", 1000)
	viper.SetDefault("createObjects", true)
	viper.SetDefault("progressTickerDuration", 30*time.Second)
	viper.SetDefault("bucketName", "gcs-load-testing")
	viper.SetDefault("maxDepth", 4)

	if cfgFile != "" {
		// Use config file from the flag
		viper.SetConfigFile(cfgFile)
	} else {
		// Search config in home directory with name ".gcs-load-testing" (without extension)
		viper.AddConfigPath(".")
		viper.SetConfigName(".gcs-load-testing")
	}

	// Read in environment variables that match
	viper.AutomaticEnv()

	// If a config file is found, read it in
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}

	// Bind the current command's flags to viper
	config = Config{
		MaxCharSizeFile:                               viper.GetInt("maxCharSizeFile"),
		Charset:                                       viper.GetString("charset"),
		WorkerCount:                                   viper.GetInt("workerCount"),
		QueueSize:                                     viper.GetInt("queueSize"),
		PrintGeneratorActivity:                        viper.GetBool("printGeneratorActivity"),
		PrintWorkerActivity:                           viper.GetBool("printWorkerActivity"),
		LowProbabilityFileCount:                       viper.GetInt("lowProbabilityFileCount"),
		LowProbabilityFileCountUpperBoundForRandGenerator: viper.GetInt("lowProbabilityFileCountUpperBoundForRandGenerator"),
		CreateObjects:                                 viper.GetBool("createObjects"),
		ProgressTickerDuration:                        viper.GetDuration("progressTickerDuration"),
		BucketName:                                    viper.GetString("bucketName"),
		MaxDepth:                                      viper.GetInt("maxDepth"),
	}
}

func main() {
	// rootCmd represents the base command when called without any subcommands
	var rootCmd = &cobra.Command{
		Use:   "gcs-load-testing",
		Short: "A tool for load testing Google Cloud Storage",
		Long: `A CLI application that generates load on Google Cloud Storage 
by creating objects in a specified bucket with configurable parameters.`,
		Run: func(cmd *cobra.Command, args []string) {
			runLoadTest()
		},
	}

	// Add persistent flags for configuration
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./.gcs-load-testing.yaml)")
	rootCmd.PersistentFlags().IntP("worker-count", "w", 250, "Number of concurrent workers")
	rootCmd.PersistentFlags().IntP("queue-size", "q", 100000, "Size of the work queue")
	rootCmd.PersistentFlags().BoolP("print-generator", "g", false, "Print generator activity")
	rootCmd.PersistentFlags().BoolP("print-worker", "p", false, "Print worker activity")
	rootCmd.PersistentFlags().StringP("bucket", "b", "bc-gcs-load-testing", "GCS bucket name")
	rootCmd.PersistentFlags().IntP("max-depth", "d", 4, "Maximum depth of the directory tree")
	rootCmd.PersistentFlags().BoolP("dry-run", "n", false, "Don't create objects, just simulate")
	rootCmd.PersistentFlags().DurationP("progress-interval", "i", 30*time.Second, "Progress reporting interval")

	// Bind flags to viper
	viper.BindPFlag("workerCount", rootCmd.PersistentFlags().Lookup("worker-count"))
	viper.BindPFlag("queueSize", rootCmd.PersistentFlags().Lookup("queue-size"))
	viper.BindPFlag("printGeneratorActivity", rootCmd.PersistentFlags().Lookup("print-generator"))
	viper.BindPFlag("printWorkerActivity", rootCmd.PersistentFlags().Lookup("print-worker"))
	viper.BindPFlag("bucketName", rootCmd.PersistentFlags().Lookup("bucket"))
	viper.BindPFlag("maxDepth", rootCmd.PersistentFlags().Lookup("max-depth"))
	viper.BindPFlag("createObjects", rootCmd.PersistentFlags().Lookup("dry-run"))
	viper.BindPFlag("progressTickerDuration", rootCmd.PersistentFlags().Lookup("progress-interval"))

	// Initialize config
	cobra.OnInitialize(initConfig)

	// Execute the root command
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func runLoadTest() {
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
		ticker := time.NewTicker(config.ProgressTickerDuration)
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

	bucket := client.Bucket(config.BucketName)

	// create a queue and a wait group
	queue := make(chan string, config.QueueSize)
	var wg sync.WaitGroup
	wg.Add(config.WorkerCount)

	// start workers
	for i := 0; i < config.WorkerCount; i++ {
		go worker(ctx, i+1, queue, &wg, bucket, &totalObjectsCreated, &totalObjectsSkipped)
	}

	// start the tree loop
	treeloop(ctx, queue, 0, config.MaxDepth, "")

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
		if rand.Intn(config.LowProbabilityFileCountUpperBoundForRandGenerator) < 1 {
			for i := 0; i < config.LowProbabilityFileCount; i++ {
				path := fmt.Sprintf("%v/%d", prefix, i)
				select {
				case queue <- path:
					if config.PrintGeneratorActivity {
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
			if config.PrintGeneratorActivity {
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

			if config.CreateObjects {
				// get object handle
				object := bucket.Object(msg)

				// check if the object exists
				_, err := object.Attrs(ctx)
				if err != nil && errors.Is(err, storage.ErrObjectNotExist) {
					// if the object does not exist, create a new object
					writer := object.NewWriter(ctx)
					// write random data to the object
					b := make([]byte, rand.Intn(config.MaxCharSizeFile))
					for i := range b {
						b[i] = config.Charset[rand.Intn(len(config.Charset))]
					}
					// write the data to the object
					_, err := fmt.Fprintf(writer, string(b), config.Charset)
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
					if config.PrintWorkerActivity {
						fmt.Printf("worker %d created object %v\n", id, msg)
					}
				} else {
					// increment the total objects skipped
					atomic.AddInt64(totalObjectsSkipped, 1)

					// print that the object was skipped
					if config.PrintWorkerActivity {
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
