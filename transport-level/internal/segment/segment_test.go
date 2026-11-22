package segment

import "testing"

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		seg     Segment
		wantErr bool
	}{
		{name: "first of three", seg: Segment{Timestamp: 1715600000, TotalSegments: 3, SegmentNumber: 1}},
		{name: "last of three", seg: Segment{Timestamp: 1715600000, TotalSegments: 3, SegmentNumber: 3}},
		{name: "zero-based rejected", seg: Segment{Timestamp: 1715600000, TotalSegments: 3, SegmentNumber: 0}, wantErr: true},
		{name: "missing timestamp", seg: Segment{TotalSegments: 1, SegmentNumber: 1}, wantErr: true},
		{name: "negative timestamp", seg: Segment{Timestamp: -1, TotalSegments: 1, SegmentNumber: 1}, wantErr: true},
		{name: "zero total", seg: Segment{Timestamp: 1715600000, TotalSegments: 0, SegmentNumber: 1}, wantErr: true},
		{name: "negative number", seg: Segment{Timestamp: 1715600000, TotalSegments: 2, SegmentNumber: -1}, wantErr: true},
		{name: "number above total", seg: Segment{Timestamp: 1715600000, TotalSegments: 2, SegmentNumber: 3}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.seg.Validate(); (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}