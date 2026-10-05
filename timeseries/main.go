package timeseries

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/go-graphite/go-whisper"
)

var (
	DefaultRetentions = whisper.MustParseRetentionDefs("1m:30m,10m:3d,1h:90d,1d:3y")
	DailyRetentions   = whisper.MustParseRetentionDefs("1d:5y")
)

func Update(wspFilename string, value float64, timestamp int, retentions whisper.Retentions, aggregationMethod whisper.AggregationMethod, xFilesFactor float32) error {
	wsp, err := whisper.Open(wspFilename)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(filepath.Dir(wspFilename), 0o750); err != nil {
			return err
		}
		if wsp, err = whisper.Create(wspFilename, retentions, aggregationMethod, xFilesFactor); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	defer wsp.Close()
	return wsp.Update(value, timestamp)
}

// UpdateMany writes points into a whisper file, creating it with the given
// retentions, aggregation and xFilesFactor when it does not exist yet.
func UpdateMany(wspFilename string, points []*whisper.TimeSeriesPoint, retentions whisper.Retentions, aggregationMethod whisper.AggregationMethod, xFilesFactor float32) error {
	if len(points) == 0 {
		return nil
	}
	wsp, err := whisper.Open(wspFilename)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(filepath.Dir(wspFilename), 0o750); err != nil {
			return err
		}
		if wsp, err = whisper.Create(wspFilename, retentions, aggregationMethod, xFilesFactor); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	defer wsp.Close()
	return wsp.UpdateMany(points)
}
