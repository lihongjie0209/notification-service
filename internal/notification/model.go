package notification

import "time"

type Template struct {
	ID        string    `db:"id" json:"id"`
	TenantID  string    `db:"tenant_id" json:"tenant_id"`
	Code      string    `db:"code" json:"code"`
	Channel   string    `db:"channel" json:"channel"`
	Locale    string    `db:"locale" json:"locale"`
	Subject   string    `db:"subject" json:"subject"`
	Content   string    `db:"content" json:"content"`
	Status    string    `db:"status" json:"status"`
	Version   int64     `db:"version" json:"version"`
	CreatedAt time.Time `db:"created_at" json:"created_at"`
	UpdatedAt time.Time `db:"updated_at" json:"updated_at"`
	CreatedBy string    `db:"created_by" json:"created_by"`
	UpdatedBy string    `db:"updated_by" json:"updated_by"`
}
type Delivery struct {
	ID                string    `db:"id" json:"id"`
	TenantID          string    `db:"tenant_id" json:"tenant_id"`
	TemplateCode      string    `db:"template_code" json:"template_code"`
	Channel           string    `db:"channel" json:"channel"`
	Locale            string    `db:"locale" json:"locale"`
	Recipient         string    `db:"recipient" json:"recipient"`
	Variables         []byte    `db:"variables" json:"variables"`
	IdempotencyKey    string    `db:"idempotency_key" json:"idempotency_key"`
	Status            string    `db:"status" json:"status"`
	Provider          string    `db:"provider" json:"provider"`
	ProviderMessageID string    `db:"provider_message_id" json:"provider_message_id"`
	FailureReason     string    `db:"failure_reason" json:"failure_reason"`
	Attempts          int32     `db:"attempts" json:"attempts"`
	NextAttemptAt     time.Time `db:"next_attempt_at" json:"next_attempt_at"`
	Version           int64     `db:"version" json:"version"`
	CreatedAt         time.Time `db:"created_at" json:"created_at"`
	UpdatedAt         time.Time `db:"updated_at" json:"updated_at"`
	CreatedBy         string    `db:"created_by" json:"created_by"`
	UpdatedBy         string    `db:"updated_by" json:"updated_by"`
}
type Page struct {
	Deliveries []Delivery `json:"deliveries"`
	Total      int64      `json:"total"`
	Page       int        `json:"page"`
	PageSize   int        `json:"page_size"`
}
type OutboxEvent struct {
	ID, Subject                       string
	Envelope                          []byte
	AvailableAt, CreatedAt, UpdatedAt time.Time
	CreatedBy, UpdatedBy              string
}
