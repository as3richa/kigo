package db

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestCanPopSingleJob(t *testing.T) {
	withDB(func(db DB) {
		db.EnsureQueuesExist([]string{"alpha"})

		job, err := db.BuildJob("alpha", "sweepFloors", []interface{}{1, 2, 3}, time.Now(), 0)
		expectSuccess(t, err)

		expectSuccess(t, db.PushJob(job))

		poppedJob, err := db.PopJobFrom([]string{"alpha"}, 1)
		expectSuccess(t, err)

		if poppedJob == nil || job.Name != poppedJob.Name || !bytes.Equal(job.ParamBlob, poppedJob.ParamBlob) {
			t.Error("enqueued job %v and popped job %v don't look equal", job, poppedJob)
		}
	})
}

func TestCanPopMultipleJobsFromDifferentQueues(t *testing.T) {
	withDB(func(db DB) {
		db.EnsureQueuesExist([]string{"alpha", "beta", "gamma", "delta"})

		jobA, _ := db.BuildJob("alpha", "sweepA", []interface{}{1, 2, 3}, time.Now(), 0)
		expectSuccess(t, db.PushJob(jobA))

		jobB, _ := db.BuildJob("beta", "sweepB", []interface{}{1, 2, 3}, time.Now(), 0)
		expectSuccess(t, db.PushJob(jobB))

		jobC, _ := db.BuildJob("gamma", "sweepC", []interface{}{1, 2, 3}, time.Now(), 0)
		expectSuccess(t, db.PushJob(jobC))

		jobD, _ := db.BuildJob("delta", "sweepD", []interface{}{1, 2, 3}, time.Now(), 0)
		expectSuccess(t, db.PushJob(jobD))

		poppedJob, err := db.PopJobFrom([]string{"alpha"}, 1)
		expectSuccess(t, err)
		if poppedJob.Name != "sweepA" {
			t.Errorf("expected %v, got %v", "sweepA", poppedJob.Name)
		}

		poppedJob, err = db.PopJobFrom([]string{"beta"}, 1)
		expectSuccess(t, err)
		if poppedJob.Name != "sweepB" {
			t.Errorf("expected %v, got %v", "sweepB", poppedJob.Name)
		}

		poppedJob, err = db.PopJobFrom([]string{"gamma", "delta"}, 1)
		expectSuccess(t, err)
		if poppedJob.Name != "sweepC" && poppedJob.Name != "sweepD" {
			t.Errorf("expected %v or %v, got %v", "sweepC", "sweepD", poppedJob.Name)
		}

		poppedJob, err = db.PopJobFrom([]string{"gamma", "delta"}, 1)
		expectSuccess(t, err)
		if poppedJob.Name != "sweepC" && poppedJob.Name != "sweepD" {
			t.Errorf("expected %v or %v, got %v", "sweepC", "sweepD", poppedJob.Name)
		}
	})
}

func TestPopsWorkConcurrentlyAsExpected(t *testing.T) {
	const queueCount = 500
	const jobCount = 500

	var queueNames [queueCount]string
	for i := 0; i < queueCount; i++ {
		queueNames[i] = fmt.Sprintf("%d", i)
	}

	withDB(func(db DB) {
		db.EnsureQueuesExist(queueNames[:])

		var w sync.WaitGroup
		w.Add(2 * queueCount)

		for _, q := range queueNames {
			go func(q string) {
				defer w.Done()
				for i := 0; i < jobCount; i++ {
					job, err := db.BuildJob(q, fmt.Sprintf("%d", i), []interface{}{i}, time.Now(), 0)
					expectSuccess(t, err)
					expectSuccess(t, db.PushJob(job))
					time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
				}
			}(q)
		}

		mutex := sync.Mutex{}
		dequeued := map[string]map[int]bool{}

		for _, q := range queueNames {
			go func(q string) {
				queues := []string{q}
				defer w.Done()

				for found := 0; found < jobCount; {
					job, err := db.PopJobFrom(queues, 1)
					expectSuccess(t, err)
					if err != nil {
						return
					}

					if job != nil {
						fmt.Printf("Queue %v found one", q)
						mutex.Lock()
						if _, ok := dequeued[q]; !ok {
							dequeued[q] = map[int]bool{}
						}
						dequeued[q][int(job.Params[0].(uint64))] = true
						mutex.Unlock()
						found++
					}

					fmt.Printf("Queue %v sleeping %d\n", q, found)
					time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
				}
			}(q)
		}

		w.Wait()

		for _, q := range queueNames {
			for i := 0; i < jobCount; i++ {
				if !dequeued[q][i] {
					t.Fatalf("Missing job %d on queue %s", i, q)
				}
			}
		}
	})
}
