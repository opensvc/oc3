package scheduler

import (
	"fmt"
	"time"
)

func Run() error {
	tmrHourly := time.NewTicker(time.Hour)
	defer tmrHourly.Stop()
	tmrDaily := time.NewTicker(24 * time.Hour)
	defer tmrDaily.Stop()
	tmrWeekly := time.NewTicker(7 * 24 * time.Hour)
	defer tmrWeekly.Stop()

	for {
		select {
		case <-tmrHourly.C:
			fmt.Println("xx", time.Now())
		case <-tmrDaily.C:
		case <-tmrWeekly.C:
		}
	}
	return nil
}
