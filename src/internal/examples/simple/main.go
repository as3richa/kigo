package main

import (
  "os"
  "fmt"
  "time"

  "github.com/as3richa/kigo"
)

const pgTestingURL = "postgresql://kigotest:kigotest@localhost:5432/kigotest"

func main() {
  switch(os.Args[1]) {
  case "worker":
    workerMain()
  case "pusher":
    pusherMain()
  case "webface":
    webfaceMain()
  default:
    panic("bad arg")
  }
}

func workerMain() {
  connection, err := kigo.Connect(pgTestingURL)
  if err != nil {
    panic(err)
  }

  if err := connection.RunWorker(nil, 5); err != nil {
    panic(err)
  }
}

func pusherMain() {
  connection, err := kigo.Connect(pgTestingURL)
  if err != nil {
    panic(err)
  }

  for i := 0;; i++ {
    id, err := connection.PerformTask([]string{"DoNothing", "FailAlways"}[i % 2], []interface{}{})
    if err != nil {
      panic(err)
    }
    fmt.Printf("ID: %d\n", id)
    time.Sleep(30 * time.Second)
  }
}

func webfaceMain() {
  connection, err := kigo.Connect(pgTestingURL)
  if err != nil {
    panic(err)
  }

  if err := connection.RunWebface("", nil); err != nil {
    panic(err)
  }
}
