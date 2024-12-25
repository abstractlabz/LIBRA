// pipeline/taskmanager.go
package main

import (
	"context"
	"log"

	"github.com/0xpc/LIBRA/utils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// Define a struct to match the aggregation output
type BucketResult struct {
	ID     bson.M               `bson:"_id"`
	DocIDs []primitive.ObjectID `bson:"docIds"`
}

func TaskManager() {
	// 1) Connect to MongoDB
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

	// 2) Target the database and collection
	collection := client.Database("Integrations").Collection("Portfolios")

	// 3) Count the documents in the collection
	count, err := collection.CountDocuments(context.Background(), bson.D{})
	if err != nil {
		log.Fatal("Error counting documents:", err)
	}
	log.Println("Number of documents in the collection:", count)

	// 4) Define the aggregation pipeline
	pipeline := mongo.Pipeline{
		// Stage 1: Assign rowNumber to each document, sorted by _id ascending
		{{Key: "$setWindowFields", Value: bson.M{
			"sortBy": bson.M{"_id": 1},
			"output": bson.M{
				"rowNumber": bson.M{"$documentNumber": bson.M{}},
			},
		}}},
		// Stage 2: Automatically bucket the documents into 5 groups based on rowNumber
		{{Key: "$bucketAuto", Value: bson.M{
			"groupBy": "$rowNumber",
			"buckets": 5,
			"output": bson.M{
				// Collect the _id of each doc in the bucket
				"docIds": bson.M{"$push": "$_id"},
			},
		}}},
	}

	log.Println("Running aggregation pipeline:", pipeline)

	// 5) Execute the aggregation pipeline
	cursor, err := collection.Aggregate(context.Background(), pipeline)
	if err != nil {
		log.Fatal("Error running aggregation pipeline:", err)
	}
	defer cursor.Close(context.Background())

	// 6) Decode the aggregation results
	var aggResults []BucketResult
	if err := cursor.All(context.Background(), &aggResults); err != nil {
		log.Fatal("Error decoding aggregation results:", err)
	}

	// 7) Transform results into a partitioned array matrix
	partitionedDocIDs := make([][]primitive.ObjectID, 0, len(aggResults))
	for _, bucket := range aggResults {
		partitionedDocIDs = append(partitionedDocIDs, bucket.DocIDs)
	}

	// 8) Print out the partitioned array matrix
	log.Println("Partitioned Document IDs:")
	// for i, partition := range partitionedDocIDs {
	// 	log.Printf("Bucket %d: %v\n", i+1, partition)
	// }

	log.Println(partitionedDocIDs[0][0])
}

func main() {
	TaskManager()
}
