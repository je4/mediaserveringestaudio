package ingestaudio

import (
	"emperror.dev/errors"
	"github.com/je4/utils/v2/pkg/zLogger"
	"io"
	"sync"
	"time"
)

type storageStruct struct {
	Name       string
	Filebase   string
	Datadir    string
	Subitemdir string
	Tempdir    string
}

type JobStruct struct {
	collection string
	signature  string
	Width      int64
	Height     int64
	Duration   int64
	Size       int64
	MimeType   string
	Path       string
	Storage    *storageStruct
	Missing    []string
}

func NewWorkerPool(num int, ingestTimeout time.Duration, doIt func(job *JobStruct) error, logger zLogger.ZLogger) (chan *JobStruct, io.Closer) {
	wp := &workerPool{
		jobChan:       make(chan *JobStruct),
		wg:            &sync.WaitGroup{},
		ingestTimeout: ingestTimeout,
		doIt:          doIt,
		logger:        logger,
	}
	wp.Start(num)
	return wp.jobChan, wp
}

type workerPool struct {
	jobChan       chan *JobStruct
	wg            *sync.WaitGroup
	ingestTimeout time.Duration
	doIt          func(job *JobStruct) error
	logger        zLogger.ZLogger
}

func (wp *workerPool) Start(num int) {
	for i := 0; i < num; i++ {
		wp.AddWorker()
	}
}

func (wp *workerPool) Close() error {
	close(wp.jobChan)
	c := make(chan struct{})
	go func() {
		defer close(c)
		wp.wg.Wait()
	}()
	select {
	case <-c:
		return nil // completed normally
	case <-time.After(wp.ingestTimeout):
		return errors.New("timed out") // timed out
	}
}

func (wp *workerPool) AddWorker() {
	wp.wg.Add(1)
	defer wp.wg.Done()
	go func() {
		for job := range wp.jobChan {
			// process job
			if err := wp.doIt(job); err != nil {
				wp.logger.Error().Err(err).Msg("error processing job")
			}
		}
	}()
}
