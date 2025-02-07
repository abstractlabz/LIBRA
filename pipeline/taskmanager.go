// pipeline/taskmanager.go
package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/0xPCDefenders/LIBRA/utils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// BucketResult defines a struct to match the aggregation output.
type BucketResult struct {
	ID     bson.M               `bson:"_id"`
	DocIDs []primitive.ObjectID `bson:"docIds"`
}

// Job struct now holds full documents.
type Job struct {
	ID   int
	Docs []bson.M
}

// Worker struct remains the same.
type Worker struct {
	ID         int
	JobQueue   <-chan Job
	WorkerPool chan<- chan Job
	QuitChan   chan bool
}

// Dispatcher manages the pool of workers and dispatches jobs.
type Dispatcher struct {
	WorkerPool chan chan Job
	MaxWorkers int
	JobQueue   chan Job
	WaitGroup  *sync.WaitGroup
	Workers    []*Worker // Track workers
}

// NewWorker creates a new Worker.
func NewWorker(id int, workerPool chan<- chan Job) *Worker {
	return &Worker{
		ID:         id,
		JobQueue:   nil, // will be assigned when a job is dispatched
		WorkerPool: workerPool,
		QuitChan:   make(chan bool),
	}
}

// Start begins the worker's job processing loop.
func (w *Worker) Start(wg *sync.WaitGroup) {
	go func() {
		defer wg.Done()
		for {
			// Register the worker's job queue to the worker pool.
			jobQueue := make(chan Job)
			w.JobQueue = jobQueue
			w.WorkerPool <- jobQueue

			select {
			case job, ok := <-w.JobQueue:
				if !ok {
					// Job queue closed, terminate the worker.
					fmt.Printf("Worker %d stopping\n", w.ID)
					return
				}
				// Process the job.
				fmt.Printf("Worker %d processing job %d with %d documents\n", w.ID, job.ID, len(job.Docs))
				for _, doc := range job.Docs {
					// Instead of fetching the document here, we already have it.
					utils.ProduceDocument(doc["_id"].(primitive.ObjectID), doc)
				}

			case <-w.QuitChan:
				// Received quit signal, terminate the worker.
				fmt.Printf("Worker %d received quit signal\n", w.ID)
				return
			}
		}
	}()
}

// Stop signals the worker to stop processing.
func (w *Worker) Stop() {
	go func() {
		w.QuitChan <- true
	}()
}

// NewDispatcher creates a new Dispatcher.
func NewDispatcher(maxWorkers int, jobQueue chan Job, wg *sync.WaitGroup) *Dispatcher {
	return &Dispatcher{
		WorkerPool: make(chan chan Job, maxWorkers),
		MaxWorkers: maxWorkers,
		JobQueue:   jobQueue,
		WaitGroup:  wg,
		Workers:    make([]*Worker, 0, maxWorkers),
	}
}

// Run starts the dispatcher and its workers.
func (d *Dispatcher) Run() {
	for i := 1; i <= d.MaxWorkers; i++ {
		worker := NewWorker(i, d.WorkerPool)
		d.Workers = append(d.Workers, worker)
		d.WaitGroup.Add(1)
		worker.Start(d.WaitGroup)
	}

	go d.dispatch()
}

// dispatch listens for incoming jobs and assigns them to available workers.
func (d *Dispatcher) dispatch() {
	for job := range d.JobQueue {
		// Get an available worker's job queue.
		jobQueue := <-d.WorkerPool
		// Assign the job to the worker.
		jobQueue <- job
	}

	// When JobQueue is closed, signal all workers to stop.
	for _, worker := range d.Workers {
		worker.Stop()
	}
}

// TaskManager connects to MongoDB, aggregates documents into partitions,
// and then retrieves the full document for each partition.
func TaskManager() [][]bson.M {
	// 1) Connect to MongoDB.
	client, err := utils.ConnectToMongo()
	if err != nil {
		log.Fatal("Failed to connect to MongoDB:", err)
	}
	// Ensure the client disconnects when TaskManager finishes.
	defer func() {
		if err := client.Disconnect(context.Background()); err != nil {
			log.Fatal("Error disconnecting from MongoDB:", err)
		}
		log.Println("Disconnected from MongoDB")
	}()

	// 2) Target the database and collection.
	collection := client.Database("Integrations").Collection("Portfolios")

	// 3) Count the documents in the collection.
	count, err := collection.CountDocuments(context.Background(), bson.D{})
	if err != nil {
		log.Fatal("Error counting documents:", err)
	}
	log.Println("Number of documents in the collection:", count)

	// 4) Define the aggregation pipeline.
	pipeline := mongo.Pipeline{
		// Stage 1: Assign rowNumber to each document, sorted by _id ascending.
		{{Key: "$setWindowFields", Value: bson.M{
			"sortBy": bson.M{"_id": 1},
			"output": bson.M{
				"rowNumber": bson.M{"$documentNumber": bson.M{}},
			},
		}}},
		// Stage 2: Automatically bucket the documents into 5 groups based on rowNumber.
		{{Key: "$bucketAuto", Value: bson.M{
			"groupBy": "$rowNumber",
			"buckets": 5,
			"output": bson.M{
				// Collect the _id of each doc in the bucket.
				"docIds": bson.M{"$push": "$_id"},
			},
		}}},
	}

	log.Println("Running aggregation pipeline:", pipeline)

	// 5) Execute the aggregation pipeline.
	cursor, err := collection.Aggregate(context.Background(), pipeline)
	if err != nil {
		log.Fatal("Error running aggregation pipeline:", err)
	}
	defer cursor.Close(context.Background())

	// 6) Decode the aggregation results.
	var aggResults []BucketResult
	if err := cursor.All(context.Background(), &aggResults); err != nil {
		log.Fatal("Error decoding aggregation results:", err)
	}

	// 7) Transform results into a partitioned array of full documents.
	var partitionedDocs [][]bson.M
	for _, bucket := range aggResults {
		// For each partition, retrieve the full documents.
		cursor, err := collection.Find(context.Background(), bson.M{"_id": bson.M{"$in": bucket.DocIDs}})
		if err != nil {
			log.Fatal("Error fetching documents for partition: ", err)
		}
		var docs []bson.M
		if err := cursor.All(context.Background(), &docs); err != nil {
			log.Fatal("Error decoding documents for partition: ", err)
		}
		partitionedDocs = append(partitionedDocs, docs)
	}

	// 8) Optionally, print out the partitioned documents.
	log.Println("Partitioned Documents:")
	for i, partition := range partitionedDocs {
		log.Printf("Bucket %d: %v\n", i+1, partition)
	}

	return partitionedDocs
}

func main() {
	// Instead, use the Go Kafka producer.
	partitionedDocs := TaskManager()
	log.Println(partitionedDocs)

	var wg sync.WaitGroup

	// Creating the job queue.
	jobQueue := make(chan Job, 10)

	// Creating the dispatcher.
	dispatcher := NewDispatcher(5, jobQueue, &wg)
	dispatcher.Run()

	// Putting jobs into the job queue.
	for i, partition := range partitionedDocs {
		jobQueue <- Job{ID: i, Docs: partition}
	}

	// Closing the job queue to indicate no more jobs will be sent.
	close(jobQueue)

	// Waiting for all workers to finish.
	wg.Wait()

	log.Println("All workers have finished")
}
