// Package segment describes the segment format sent by the agent level.
package segment

import (
	"errors"
	"fmt"
)

// Segment is one piece of a generated document structure sent by the agent.
type Segment struct {
	Timestamp                int64  `json:"timestamp"`
	TotalSegments            int    `json:"total_segments"`
	SegmentNumber            int    `json:"segment_number"`
	DocumentStructureSegment string `json:"document_structure_segment"`
}

// Validate checks the fields the assembler relies on.
// Agent numbers segments starting from 1 (1..TotalSegments).
func (s Segment) Validate() error {
	if s.Timestamp <= 0 {
		return errors.New("timestamp must be positive")
	}
	if s.TotalSegments < 1 {
		return fmt.Errorf("total_segments must be positive, got %d", s.TotalSegments)
	}
	if s.SegmentNumber < 1 || s.SegmentNumber > s.TotalSegments {
		return fmt.Errorf("segment_number %d is out of range [1, %d]", s.SegmentNumber, s.TotalSegments)
	}
	return nil
}