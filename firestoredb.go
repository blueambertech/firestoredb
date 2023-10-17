package firestoredb

import (
	"context"
	"errors"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/paceperspective/db"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type FirestoreClient struct {
	client *firestore.Client
}

// NewFirestore returns a new Firestore NoSQL client
func NewFirestore(projID, dbName string) (db.NoSQLClient, error) {
	ctx, canc := context.WithTimeout(context.Background(), 5*time.Second)
	defer canc()
	fsc, err := firestore.NewClientWithDatabase(ctx, projID, dbName)
	if err != nil {
		return nil, err
	}
	return &FirestoreClient{client: fsc}, nil
}

// Read reads a record from the specified collection by ID and populates the outObj with the data
func (f *FirestoreClient) Read(ctx context.Context, collection, id string, outObj interface{}) error {
	col := f.client.Collection(collection)
	if col == nil {
		return errors.New("could not find collection: " + collection)
	}
	obj, err := col.Doc(id).Get(ctx)
	if err != nil {
		return err
	}
	return obj.DataTo(outObj)
}

// Insert inserts a new record into the specified collection with the data provided, returns the ID of the newly inserted record
func (f *FirestoreClient) Insert(ctx context.Context, collection string, data interface{}) (string, error) {
	col := f.client.Collection(collection)
	if col == nil {
		return "", errors.New("could not find collection: " + collection)
	}
	docRef, _, err := col.Add(ctx, data)
	if err != nil {
		return "", err
	}
	return docRef.ID, nil
}

func (f *FirestoreClient) InsertWithID(ctx context.Context, collection, id string, data interface{}) error {
	col := f.client.Collection(collection)
	if col == nil {
		return errors.New("could not find collection: " + collection)
	}
	err := f.client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		docRef := col.Doc(id)
		_, err := tx.Get(docRef)
		if status.Code(err) == codes.NotFound {
			if err = tx.Set(docRef, data); err != nil {
				return err
			}
			return nil
		} else if err == nil {
			return errors.New("doc already exists with id " + id)
		}
		return err
	})
	return err
}

// Where reads records from the specified collection matching a key and value and populates the outObjs with the data
func (f *FirestoreClient) Where(ctx context.Context, collection, key, value string, outObjs []map[string]interface{}) error {
	col := f.client.Collection(collection)
	if col == nil {
		return errors.New("could not find collection: " + collection)
	}
	docs, err := col.Where(key, "==", value).Documents(ctx).GetAll()
	if err != nil {
		return err
	}
	for _, d := range docs {
		outObjs = append(outObjs, d.Data())
	}
	return nil
}
