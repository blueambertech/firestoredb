package firestoredb

import (
	"context"
	"errors"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/paceperspective/logging"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type FirestoreClient struct {
	client *firestore.Client
}

// NewFirestore returns a new Firestore NoSQL client
func NewFirestore(projID, dbName string) (*FirestoreClient, error) {
	ctx, canc := context.WithTimeout(context.Background(), 5*time.Second)
	defer canc()
	fsc, err := firestore.NewClientWithDatabase(ctx, projID, dbName)
	if err != nil {
		return nil, err
	}
	return &FirestoreClient{client: fsc}, nil
}

// Close closes down the firestore client
func (f *FirestoreClient) Close() error {
	return f.client.Close()
}

// Read reads a record from the specified collection by ID and populates the outObj with the data
func (f *FirestoreClient) Read(ctx context.Context, collection, id string) (map[string]interface{}, error) {
	_, span := logging.Tracer.Start(ctx, "firestoredb-read")
	defer span.End()
	col := f.client.Collection(collection)
	if col == nil {
		return nil, errors.New("could not find collection: " + collection)
	}
	obj, err := col.Doc(id).Get(ctx)
	if err != nil {
		return nil, err
	}
	var outObj map[string]interface{}
	err = obj.DataTo(&outObj)
	if err != nil {
		return nil, err
	}
	return outObj, nil
}

// Insert inserts a new record into the specified collection with the data provided, returns the ID of the newly inserted record
func (f *FirestoreClient) Insert(ctx context.Context, collection string, data interface{}) (string, error) {
	_, span := logging.Tracer.Start(ctx, "firestoredb-insert")
	defer span.End()
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

// InsertWithID inserts a new record with an existing ID into the specified collection with the data provided, this func will check
// that a record with this ID does not already exist and return an error if it does
func (f *FirestoreClient) InsertWithID(ctx context.Context, collection, id string, data interface{}) error {
	_, span := logging.Tracer.Start(ctx, "firestoredb/insert-with-id")
	defer span.End()
	col := f.client.Collection(collection)
	if col == nil {
		return errors.New("could not find collection: " + collection)
	}
	err := f.client.RunTransaction(ctx, func(c context.Context, tx *firestore.Transaction) error {
		docRef := col.Doc(id)
		_, tErr := tx.Get(docRef)
		if status.Code(tErr) == codes.NotFound {
			if tErr = tx.Set(docRef, data); tErr != nil {
				return tErr
			}
			return nil
		} else if tErr == nil {
			return errors.New("doc already exists with id " + id)
		}
		return tErr
	})
	return err
}

// Where reads records from the specified collection matching a key and value and populates the outObjs with the data
func (f *FirestoreClient) Where(ctx context.Context, collection, key, value string) ([]map[string]interface{}, error) {
	_, span := logging.Tracer.Start(ctx, "firestoredb-where")
	defer span.End()
	col := f.client.Collection(collection)
	if col == nil {
		return nil, errors.New("could not find collection: " + collection)
	}
	docs, err := col.Where(key, "==", value).Documents(ctx).GetAll()
	if err != nil {
		return nil, err
	}
	m := make([]map[string]interface{}, len(docs))
	for i, d := range docs {
		m[i] = d.Data()
	}
	return m, nil
}
