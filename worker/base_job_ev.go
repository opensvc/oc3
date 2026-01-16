package worker

type (
	JobEv struct {
		ev EventPublisher
	}
)

func (j *JobEv) SetEv(ev EventPublisher) {
	j.ev = ev
}
