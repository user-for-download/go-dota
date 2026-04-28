package postgres

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

type IngestErrorKind string

const (
	IngestErrFKViolation     IngestErrorKind = "fk_violation"
	IngestErrCheckViolation  IngestErrorKind = "check_violation"
	IngestErrNotNull         IngestErrorKind = "not_null_violation"
	IngestErrUniqueViolation IngestErrorKind = "unique_violation"
	IngestErrSerialization   IngestErrorKind = "serialization"
	IngestErrDeadlock        IngestErrorKind = "deadlock"
	IngestErrLocked          IngestErrorKind = "match_locked"
	IngestErrValidation      IngestErrorKind = "validation"
	IngestErrUnmarshal       IngestErrorKind = "unmarshal"
	IngestErrOther           IngestErrorKind = "other"
)

func ClassifyIngestError(err error) IngestErrorKind {
	if err == nil {
		return ""
	}
	if errors.Is(err, ErrMatchLocked) {
		return IngestErrLocked
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23503":
			return IngestErrFKViolation
		case "23514":
			return IngestErrCheckViolation
		case "23502":
			return IngestErrNotNull
		case "23505":
			return IngestErrUniqueViolation
		case "40001":
			return IngestErrSerialization
		case "40P01":
			return IngestErrDeadlock
		}
	}
	return IngestErrOther
}