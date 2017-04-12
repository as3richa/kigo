package db

import "testing"

const pgTestingURL = "postgresql://kigotest:kigotest@localhost:5432/kigotest"

func withDB(callback func(DB)) {
	db, err := Connect(pgTestingURL)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if err = db.DropAll(); err != nil {
		panic(err)
	}

	callback(db)
}

func expectFailure(t *testing.T, value error) {
	if value == nil {
		t.Fatal("Expected failure, but found success")
	}
}

func expectSuccess(t *testing.T, value error) {
	if value != nil {
		t.Fatalf("Expected no error, but found %v", value)
	}
}
