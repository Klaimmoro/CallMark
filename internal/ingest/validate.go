package ingest

import (
	"fmt"
	"regexp"
	"time"
)

var (
	innRE = regexp.MustCompile(`^\d{10}(\d{2})?$`)

	phoneRE = regexp.MustCompile(`^\+?\d{10,15}$`)

	futureTolerance = time.Minute
)

func Validate(req CallRequest) error {
	if req.CallID == "" {
		return fmt.Errorf("call_id is required")
	}
	if !innRE.MatchString(req.CallerINN) {
		return fmt.Errorf("wrong caller INN")
	}
	if !phoneRE.MatchString(req.CalleeNumber) {
		return fmt.Errorf("wrong phone number")
	}
	if req.CallTime.IsZero() {
		return fmt.Errorf("call_time is required")
	}
	if req.CallTime.After(time.Now().Add(futureTolerance)) {
		return fmt.Errorf("call_time is in the future")
	}
	if req.DurationSec < 0 {
		return fmt.Errorf("duration_sec must not be negative")
	}
	return nil
}
