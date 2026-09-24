package storage

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("call not found")

// Repository - то, что нужно остальному коду от хранилища.
type Repository interface {
	Save(ctx context.Context, event LabeledEvent) error
	GetByID(ctx context.Context, callID string) (CallRecord, error)
	GetStats(ctx context.Context, from, to time.Time) ([]StatsBucket, error)
}

// PostgresRepository - пул соединений к PostgreSQL
type PostgresRepository struct {
	pool *pgxpool.Pool
}

// Конструктор для PostgresRepository
func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

func (r *PostgresRepository) Save(ctx context.Context, event LabeledEvent) error {
	const query = `
		INSERT INTO calls (call_id, caller_inn, label, reason, processed_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (call_id) DO UPDATE SET
			label = EXCLUDED.label,
			reason = EXCLUDED.reason,
			processed_at = EXCLUDED.processed_at
	`
	_, err := r.pool.Exec(ctx, query,
		event.CallID, event.CallerINN, event.Label, event.Reason, event.ProcessedAt,
	)
	return err
}

func (r *PostgresRepository) GetByID(ctx context.Context, callID string) (CallRecord, error) {
	const query = `
		SELECT call_id, caller_inn, label, reason, processed_at, created_at 
		FROM calls 
		WHERE call_id = $1
	`
	var rec CallRecord
	err := r.pool.QueryRow(ctx, query, callID).Scan(
		&rec.CallID, &rec.CallerINN, &rec.Label, &rec.Reason, &rec.ProcessedAt, &rec.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return CallRecord{}, ErrNotFound
	}
	if err != nil {
		return CallRecord{}, err
	}
	return rec, nil
}

func (r *PostgresRepository) GetStats(ctx context.Context, from, to time.Time) ([]StatsBucket, error) {
	const query = `
		SELECT label, COUNT(*) AS count
		FROM calls
		WHERE processed_at >= &1 AND processed_at < &2
		GROUP BY label
		ORDER BY label
	`
	rows, err := r.pool.Query(ctx, query, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var buckets []StatsBucket
	for rows.Next() {
		var bucket StatsBucket
		if err := rows.Scan(&bucket.Label, &bucket.Count); err != nil {
			return nil, err
		}
		buckets = append(buckets, bucket)
	}
	return buckets, nil
}
