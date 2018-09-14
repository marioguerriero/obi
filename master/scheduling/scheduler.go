package scheduling

import (
	"obi/master/model"
	"github.com/sirupsen/logrus"
	"time"
	"obi/master/pool"
)

type PackingPolicy int
const (
	TimeDuration PackingPolicy = iota
	Count
)

type Bin struct {
	jobs []model.Job
	cumulativeValue int64
}

type LevelScheduler struct {
	bins []Bin
	policy PackingPolicy
	timeWindow int32
	binCapacity int64
}

type Scheduler struct {
	levels []LevelScheduler
	quit chan struct{}
	submitter *pool.Submitter
}

func New(levels int32, submitter *pool.Submitter) *Scheduler {
	s := &Scheduler{
		make([]LevelScheduler, levels),
		make(chan struct{}),
		submitter,

	}
	return s
}

func (s *Scheduler) Start() {
	logrus.Info("Starting scheduling routine.")
	for _, l := range s.levels {
		go schedulingRoutine(&l, s.submitter, s.quit)
	}
}

func (p *Scheduler) Stop() {
	logrus.Info("Stopping scheduling routine.")
	close(p.quit)
}

func (s *Scheduler) AddLevel(level int32, timeWindow int32, policy PackingPolicy, binCapacity int64) {
	s.levels[level] = LevelScheduler{
		make([]Bin, 1),
		policy,
		timeWindow,
		binCapacity,
	}
}

func (s *Scheduler) ScheduleJob(job model.Job) {
	if job.Priority >= 0 && job.Priority <= 7 {
		go s.submitter.DeployJobs([]model.Job{job})
	} else {
		schedulerLevel := s.levels[job.Priority]
		switch schedulerLevel.policy {
		case TimeDuration:
			timeDurationAddJob(&schedulerLevel, job)
		case Count:
			countAddJob(&schedulerLevel, job)
		}
	}
	return
}

func timeDurationAddJob(sl *LevelScheduler, job model.Job) {
	for _, b := range sl.bins {
		if b.cumulativeValue + job.PredictedDuration <= sl.binCapacity {
			b.jobs = append(b.jobs, job)
			b.cumulativeValue += job.PredictedDuration
			return
		}
	}
	sl.bins[len(sl.bins)-1].jobs[0] = job
	sl.bins[len(sl.bins)-1].cumulativeValue = job.PredictedDuration
}

func countAddJob(sl *LevelScheduler, job model.Job) {
	for _, b := range sl.bins {
		if b.cumulativeValue + 1 <= sl.binCapacity {
			b.jobs = append(b.jobs, job)
			b.cumulativeValue++
			return
		}
	}
	sl.bins[len(sl.bins)-1].jobs[0] = job
	sl.bins[len(sl.bins)-1].cumulativeValue = 1
}

func schedulingRoutine(ls *LevelScheduler, s *pool.Submitter, quit <-chan struct{}) {
	for {
		select {
		case <-quit:
			logrus.Info("Closing level-scheduler routine.")
			return
		default:
			for _, bin := range ls.bins {
				go s.DeployJobs(bin.jobs)
			}
			time.Sleep(time.Duration(ls.timeWindow) * time.Second)
		}
	}
}

