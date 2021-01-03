package mongo

import (
	"context"
	"fmt"
	"reflect"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

/*
	顺便说说bson.A{}、bson.M{}、bson.E{}、bson.D{}的区别
	bson.A{}：等同于数组     	            用法：bson.A{"bar", "world", 3.14159, bson.D{{"qux", 12345}}}
	bson.M{}：等同于map(无序)           	用法：bson.M{"id":bson.M{"$in":ids}}
	bson.E{}：等同于struct中的field         用法：与bson.D连用 bson.D{bson.E{"pi":3.14}}
	bson.D{}：等同于struct(有序)            用法：bson.D{{"foo", "bar"}, {"pi", 3.14159}}
*/

func main() {
	db, _ := connectToMongoDB()
	collection := getCollection(db, "collectionName")
	var doc interface{}
	// ...
	insertOne(doc, collection)
	// ... 其他操作也是如此
}

func connectToMongoDB() (*mongo.Database, error) {
	clientOptions := options.Client().ApplyURI("127.0.0.1")
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		return nil, err
	}
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		return nil, err
	}
	db := client.Database("DBName")
	return db, nil
}

func getCollection(db *mongo.Database, collection string) *mongo.Collection {
	return db.Collection(collection)
}

func insertMany(documents []interface{}, collection *mongo.Collection) error {
	_, err := collection.InsertMany(context.TODO(), documents)
	if err != nil {
		return err
	}
	return nil
}

func insertOne(document interface{}, collection *mongo.Collection) error {
	_, err := collection.InsertOne(context.TODO(), document)
	if err != nil {
		return err
	}
	return nil
}

func upsert(documents []interface{}, collection *mongo.Collection) error {
	upsertFlag := true
	writeModels := []mongo.WriteModel{}
	for i := range documents {
		value := reflect.ValueOf(documents[i]).Elem()
		filter := make(bson.M, 0)
		update := make(bson.M, 0)
		filter["entity_id"] = value.FieldByName("EntityID").Interface()
		filter["entity_type"] = value.FieldByName("EntityType").Interface()
		update["$set"] = documents[i]
		updateOneModel := mongo.UpdateOneModel{
			Filter: filter,
			Update: update,
			Upsert: &upsertFlag,
		}
		writeModels = append(writeModels, &updateOneModel)
	}
	collection.BulkWrite(context.TODO(), writeModels)
	return nil
}

func deleteOne(collection *mongo.Collection) error {
	filter := bson.M{
		"id": "1",
	}
	collection.DeleteOne(context.TODO(), filter)
	return nil
}

func deleteMany(ids []string, collection *mongo.Collection) error {
	filter := bson.M{
		"id": bson.M{
			"$in": ids,
		},
	}
	collection.DeleteMany(context.TODO(), filter)
	return nil
}

func deleteCollection(collection *mongo.Collection) error {
	filter := bson.M{}
	collection.DeleteMany(context.TODO(), filter)
	return nil
}

func createIndex(collection *mongo.Collection) error {
	indexKey := mongo.IndexModel{
		Keys:    bson.D{{"entity_id", 1}},
		Options: options.Index(),
		// Options: options.Index().SetUnique(true),
	}
	opts := options.CreateIndexes()
	_, err := collection.Indexes().CreateOne(context.TODO(), indexKey, opts)
	if err != nil {
		return err
	}
	return nil
}

func findOne(collection *mongo.Collection) error {
	filter := bson.M{
		"id": "1",
	}
	var tmp struct {
		EntityID   string `bson:"entity_id" json:"entity_id"`
		EntityType string `bson:"entity_type" json:"entity_type"`
	}
	res := collection.FindOne(context.TODO(), filter)
	res.Decode(&tmp)
	fmt.Println(tmp)
	return nil
}

func getCursor(collection *mongo.Collection) *mongo.Cursor {
	filter := bson.M{
		"id": bson.M{
			"$lte": 12,
		},
	}
	projection := make(bson.M, 0)
	// 等价于 SQL 中的 select
	projection["entity_id"] = 1
	projection["entity_type"] = 1
	cursor, _ := collection.Find(context.TODO(), filter, &options.FindOptions{
		Projection: projection,
	})
	return cursor
}

func handleCursor(cursor *mongo.Cursor) error {
	for cursor.Next(context.TODO()) {
		var tmp struct {
			EntityID   string `bson:"entity_id" json:"entity_id"`
			EntityType string `bson:"entity_type" json:"entity_type"`
		}
		cursor.Decode(&tmp)
		// ...
	}
	return nil
}

func count(collection *mongo.Collection) int64 {
	filter := bson.M{}
	count, _ := collection.CountDocuments(context.TODO(), filter)
	return count
}

func page(collection *mongo.Collection, from int, size int) *mongo.Cursor {
	findOptions := &options.FindOptions{}
	if size > 0 {
		findOptions.SetLimit(int64(size))
		findOptions.SetSkip(int64(from))
	}
	cursor, _ := collection.Find(context.TODO(), bson.M{}, findOptions)
	return cursor
}

func updateOne(collection *mongo.Collection) error {
	filter := bson.M{"id": 1}
	update := bson.M{
		"$set": bson.M{
			"name": "123",
		},
	}
	_, err := collection.UpdateOne(context.TODO(), filter, update)
	if err != nil {
		return err
	}
	return nil
}

func updateMany(ids []string, collection *mongo.Collection) error {
	filter := bson.M{
		"id": bson.M{
			"$in": ids,
		},
	}
	update := bson.M{
		"$set": bson.M{
			"name": "123",
		},
	}
	_, err := collection.UpdateMany(context.TODO(), filter, update)
	if err != nil {
		return err
	}
	return nil
}
