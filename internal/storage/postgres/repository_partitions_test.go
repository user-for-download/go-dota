package postgres

import (
	"testing"
	"time"
)

func TestParsePartitionBounds(t *testing.T) {
	tests := []struct {
		name    string
		expr    string
		wantFrom int64
		wantTo   int64
		wantErr bool
	}{
		{
			name:    "valid bounds",
			expr:    "FOR VALUES FROM ('1704067200') TO ('1711929600')",
			wantFrom: 1704067200,
			wantTo:   1711929600,
			wantErr:  false,
		},
		{
			name:    "valid bounds 2025 q1",
			expr:    "FOR VALUES FROM ('1735689600') TO ('1743465600')",
			wantFrom: 1735689600,
			wantTo:   1743465600,
			wantErr:  false,
		},
		{
			name:    "invalid format",
			expr:    "FOR VALUES (1) TO (2)",
			wantErr: true,
		},
		{
			name:    "empty string",
			expr:    "",
			wantErr: true,
		},
		{
			name:    "wrong prefix",
			expr:    "PARTITION FOR VALUES FROM ('1') TO ('2')",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			from, to, err := parsePartitionBounds(tc.expr)
			if tc.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if from != tc.wantFrom {
				t.Errorf("from = %d, want %d", from, tc.wantFrom)
			}
			if to != tc.wantTo {
				t.Errorf("to = %d, want %d", to, tc.wantTo)
			}
		})
	}
}

func TestMonthPartitionName(t *testing.T) {
	tests := []struct {
		name     string
		input    time.Time
		want     string
	}{
		{
			name:     "January 2024",
			input:    time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			want:     "matches_p_2024_01",
		},
		{
			name:     "December 2024",
			input:    time.Date(2024, 12, 1, 0, 0, 0, 0, time.UTC),
			want:     "matches_p_2024_12",
		},
		{
			name:     "March 2025",
			input:    time.Date(2025, 3, 20, 0, 0, 0, 0, time.UTC),
			want:     "matches_p_2025_03",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := MonthPartitionName(tc.input)
			if got != tc.want {
				t.Errorf("MonthPartitionName() = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestMonthBounds(t *testing.T) {
	tests := []struct {
		name  string
		input time.Time
	}{
		{
			name:  "January 2024",
			input: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "February leap year",
			input: time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "December 2024",
			input: time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			from, to := MonthBounds(tc.input)

			// Verify from is start of month
			expectedFrom := time.Date(tc.input.Year(), tc.input.Month(), 1, 0, 0, 0, 0, time.UTC).Unix()
			if from != expectedFrom {
				t.Errorf("from = %d, want %d (start of month)", from, expectedFrom)
			}

			// Verify to is start of next month
			expectedTo := time.Date(tc.input.Year(), tc.input.Month()+1, 1, 0, 0, 0, 0, time.UTC).Unix()
			if to != expectedTo {
				t.Errorf("to = %d, want %d (start of next month)", to, expectedTo)
			}

			// Verify duration is at least 28 days (covers all months)
			if to-from < 28*24*60*60 {
				t.Errorf("month duration too short: %d seconds", to-from)
			}
		})
	}
}

func TestQuarterPartitionName(t *testing.T) {
	tests := []struct {
		name  string
		input time.Time
		want  string
	}{
		{
			name:  "January = Q1",
			input: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			want:  "matches_2024_q1",
		},
		{
			name:  "March = Q1",
			input: time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
			want:  "matches_2024_q1",
		},
		{
			name:  "April = Q2",
			input: time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC),
			want:  "matches_2024_q2",
		},
		{
			name:  "June = Q2",
			input: time.Date(2024, 6, 30, 0, 0, 0, 0, time.UTC),
			want:  "matches_2024_q2",
		},
		{
			name:  "July = Q3",
			input: time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC),
			want:  "matches_2024_q3",
		},
		{
			name:  "September = Q3",
			input: time.Date(2024, 9, 15, 0, 0, 0, 0, time.UTC),
			want:  "matches_2024_q3",
		},
		{
			name:  "October = Q4",
			input: time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC),
			want:  "matches_2024_q4",
		},
		{
			name:  "December = Q4",
			input: time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC),
			want:  "matches_2024_q4",
		},
		{
			name:  "January 2025 edge",
			input: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			want:  "matches_2025_q1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := QuarterPartitionName(tc.input)
			if got != tc.want {
				t.Errorf("QuarterPartitionName() = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestQuarterBounds(t *testing.T) {
	tests := []struct {
		name  string
		input time.Time
	}{
		{
			name:  "Q1 2024",
			input: time.Date(2024, 2, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "Q2 2024",
			input: time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "Q3 2024",
			input: time.Date(2024, 8, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "Q4 2024",
			input: time.Date(2024, 11, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "Year boundary Q4->Q1",
			input: time.Date(2024, 12, 15, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			from, to := QuarterBounds(tc.input)

			// Verify duration is exactly 3 months
			expectedDuration := int64(3 * 30 * 24 * 60 * 60) // ~3 months in seconds
			actualDuration := to - from
			if actualDuration < expectedDuration-86400*5 || actualDuration > expectedDuration+86400*5 {
				// Allow ±5 days variance for month length differences
				t.Errorf("quarter duration = %d seconds, expected ~%d", actualDuration, expectedDuration)
			}

			// Verify from is start of quarter month
			quarter := (int(tc.input.Month()) - 1) / 3
			expectedStartMonth := time.Month(quarter*3 + 1)
			expectedFrom := time.Date(tc.input.Year(), expectedStartMonth, 1, 0, 0, 0, 0, time.UTC).Unix()
			if from != expectedFrom {
				t.Errorf("from = %d, want %d (start of quarter)", from, expectedFrom)
			}
		})
	}
}