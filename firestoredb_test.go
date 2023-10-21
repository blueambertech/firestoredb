package firestoredb

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/blueambertech/logging"
)

func TestMain(m *testing.M) {
	err := os.Setenv("FIRESTORE_EMULATOR_HOST", "localhost:8244")
	if err != nil {
		log.Println("Failed to set firestore emulator env variable")
		return
	}
	logging.Setup(context.Background(), "firestoredb/TEST")

	os.Exit(m.Run())
}

func TestRead(t *testing.T) {
	// TODO
}
