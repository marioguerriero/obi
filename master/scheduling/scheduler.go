package scheduling

import (
	"obi/master/model"
	"github.com/sirupsen/logrus"
		"obi/master/pool"
	"github.com/spf13/viper"
		"sync"
	"time"
	)

type PackingPolicy int
const (
	TimeDuration PackingPolicy = iota
	Count
)

type bin struct {
	jobs []model.Job
	cumulativeValue int32
}

type levelScheduler struct {
	bins []bin
	Policy PackingPolicy
	Timeout int32
	BinCapacity int32
	sync.RWMutex
}

type Scheduler struct {
	levels []levelScheduler
	quit chan struct{}
	submitter *pool.Submitter
}

func New(submitter *pool.Submitter) *Scheduler {
	s := &Scheduler{
		make([]levelScheduler, 0),
		make(chan struct{}),
		submitter,
	}
	return s
}

// SetupConfig function load the configuration for the scheduler
func (s *Scheduler) SetupConfig() {
	err := viper.UnmarshalKey("schedulingLevels", &s.levels)
	if err != nil {
		panic("Unable to unmarshal levels")
	}
}

// Start function starts the scheduling routine
func (s *Scheduler) Start() {
	logrus.Info("Starting scheduling routine.")

	for i := range s.levels {
		go schedulingRoutine(&s.levels[i], s.submitter, s.quit)
	}
}

// Stop function stops the scheduling routine
func (s *Scheduler) Stop() {
	logrus.Info("Stopping scheduling routine.")
	close(s.quit)
}

// ScheduleJob if for adding a new job in the bins
func (s *Scheduler) ScheduleJob(job model.Job) {
	if job.Priority >= int32(len(s.levels)) {
		go s.submitter.DeployJobs([]model.Job{job})
	} else {
		schedulerLevel := &s.levels[job.Priority]
		switch schedulerLevel.Policy {
		case TimeDuration:
			timeDurationAddJob(schedulerLevel, job)
		case Count:
			countAddJob(schedulerLevel, job)
		}
	}
	return
}

func timeDurationAddJob(ls *levelScheduler, job model.Job) {
	ls.Lock()
	defer ls.Unlock()
	for i := range ls.bins {
		if ls.bins[i].cumulativeValue + job.PredictedDuration <= ls.BinCapacity {
			ls.bins[i].jobs = append(ls.bins[i].jobs, job)
			ls.bins[i].cumulativeValue += job.PredictedDuration
			return
		}
	}
	ls.bins = append(ls.bins, bin{})
	ls.bins[len(ls.bins)-1].jobs = append(ls.bins[len(ls.bins)-1].jobs, job)
	ls.bins[len(ls.bins)-1].cumulativeValue = job.PredictedDuration
}

func countAddJob(ls *levelScheduler, job model.Job) {
	ls.Lock()
	defer ls.Unlock()
	for i := range ls.bins {
		if ls.bins[i].cumulativeValue + 1 <= ls.BinCapacity {
			ls.bins[i].jobs = append(ls.bins[i].jobs, job)
			ls.bins[i].cumulativeValue++
			return
		}
	}
	ls.bins = append(ls.bins, bin{})
	ls.bins[len(ls.bins)-1].jobs = append(ls.bins[len(ls.bins)-1].jobs, job)
	ls.bins[len(ls.bins)-1].cumulativeValue = 1
}

func flush(ls *levelScheduler, s *pool.Submitter) {
	ls.Lock()
	defer ls.Unlock()
	for i := range ls.bins {
		go s.DeployJobs(ls.bins[i].jobs)
	}
	ls.bins = nil
}

func schedulingRoutine(ls *levelScheduler, s *pool.Submitter, quit <-chan struct{}) {
	for {
		select {
		case <-quit:
			logrus.Info("Closing level-scheduler routine.")
			return
		default:
			flush(ls, s)
		}
		time.Sleep(time.Duration(ls.Timeout) * time.Second)
	}
}

