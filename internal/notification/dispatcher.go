package notification

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"text/template"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lihongjie0209/notification-service/internal/config"
	"github.com/lihongjie0209/notification-service/internal/database"
	"github.com/lihongjie0209/notification-service/internal/outbound"
	"go.uber.org/fx"
)

type Message struct{ DeliveryID, Channel, Recipient, Subject, Content string }
type SendResult struct{ Provider, MessageID string }
type Sender interface {
	Send(context.Context, Message) (SendResult, error)
}
type SenderRegistry map[string]Sender

type InAppSender struct{}

func (InAppSender) Send(context.Context, Message) (SendResult, error) {
	return SendResult{Provider: "in_app", MessageID: "stored"}, nil
}

type unavailableSender struct{ channel string }

func (s unavailableSender) Send(context.Context, Message) (SendResult, error) {
	return SendResult{}, fmt.Errorf("%s provider is not configured", s.channel)
}

type ProviderSender struct {
	channel string
	client  *outbound.HTTPClient
	path    string
}

func (s *ProviderSender) Send(ctx context.Context, message Message) (SendResult, error) {
	payload, err := json.Marshal(map[string]string{"delivery_id": message.DeliveryID, "channel": message.Channel, "recipient": message.Recipient, "subject": message.Subject, "content": message.Content})
	if err != nil {
		return SendResult{}, fmt.Errorf("encode provider request: %w", err)
	}
	response, err := s.client.Do(ctx, http.MethodPost, s.path, payload, nil)
	if err != nil {
		return SendResult{}, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return SendResult{}, fmt.Errorf("provider returned status %d", response.StatusCode)
	}
	var result struct {
		MessageID string `json:"message_id"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return SendResult{}, fmt.Errorf("decode provider response: %w", err)
	}
	if result.MessageID == "" {
		return SendResult{}, errors.New("provider response omitted message_id")
	}
	return SendResult{Provider: s.channel, MessageID: result.MessageID}, nil
}

func NewSenderRegistry(registry *outbound.Registry, cfg config.Config) SenderRegistry {
	senders := SenderRegistry{"in_app": InAppSender{}, "email": unavailableSender{"email"}, "sms": unavailableSender{"sms"}, "webhook": unavailableSender{"webhook"}}
	for channel, provider := range cfg.Notification.Providers {
		client, ok := registry.HTTP(provider.Upstream)
		if ok && (channel == "email" || channel == "sms" || channel == "webhook") {
			senders[channel] = &ProviderSender{channel: channel, client: client, path: provider.Path}
		}
	}
	return senders
}

type Dispatcher struct {
	repository Repository
	transactor *database.Transactor
	senders    SenderRegistry
	cfg        config.Notification
	now        func() time.Time
}

func NewDispatcher(repository Repository, transactor *database.Transactor, senders SenderRegistry, cfg config.Config) *Dispatcher {
	workerConfig := cfg.Notification
	if workerConfig.DispatchInterval <= 0 {
		workerConfig.DispatchInterval = time.Second
	}
	if workerConfig.BatchSize <= 0 {
		workerConfig.BatchSize = 100
	}
	if workerConfig.MaxAttempts <= 0 {
		workerConfig.MaxAttempts = 8
	}
	if workerConfig.RetryBase <= 0 {
		workerConfig.RetryBase = 5 * time.Second
	}
	return &Dispatcher{repository: repository, transactor: transactor, senders: senders, cfg: workerConfig, now: time.Now}
}
func (d *Dispatcher) RunOnce(ctx context.Context) (int, error) {
	var due []Delivery
	err := d.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error {
		var claimErr error
		due, claimErr = d.repository.ClaimDue(ctx, tx, d.now(), d.cfg.BatchSize)
		return claimErr
	})
	if err != nil {
		return 0, err
	}
	for _, delivery := range due {
		d.deliver(ctx, delivery)
	}
	return len(due), nil
}
func (d *Dispatcher) deliver(ctx context.Context, delivery Delivery) {
	tpl, err := d.repository.GetTemplate(ctx, delivery.TenantID, delivery.TemplateCode, delivery.Channel, delivery.Locale)
	var result SendResult
	if err == nil {
		var variables map[string]string
		err = json.Unmarshal(delivery.Variables, &variables)
		if err == nil {
			subject, subjectErr := render(tpl.Code+"-subject", tpl.Subject, variables)
			content, contentErr := render(tpl.Code+"-content", tpl.Content, variables)
			err = errors.Join(subjectErr, contentErr)
			if err == nil {
				sender := d.senders[delivery.Channel]
				if sender == nil {
					err = fmt.Errorf("sender for %s is not configured", delivery.Channel)
				} else {
					result, err = sender.Send(ctx, Message{DeliveryID: delivery.ID, Channel: delivery.Channel, Recipient: delivery.Recipient, Subject: subject, Content: content})
				}
			}
		}
	}
	now := d.now()
	delivery.UpdatedAt = now
	delivery.UpdatedBy = "notification-dispatcher"
	if err == nil {
		delivery.Status = "sent"
		delivery.Provider = result.Provider
		delivery.ProviderMessageID = result.MessageID
		delivery.FailureReason = ""
	} else {
		delivery.FailureReason = err.Error()
		if delivery.Attempts >= d.cfg.MaxAttempts {
			delivery.Status = "dead_letter"
		} else {
			delivery.Status = "retrying"
			delivery.NextAttemptAt = now.Add(retryBackoff(d.cfg.RetryBase, delivery.Attempts))
		}
	}
	_ = d.transactor.Within(context.WithoutCancel(ctx), nil, func(tx *sqlx.Tx) error {
		if err := d.repository.Finish(context.WithoutCancel(ctx), tx, delivery, delivery.Version); err != nil {
			return err
		}
		delivery.Version++
		service := &Service{repository: d.repository}
		return service.addDeliveryEvent(context.WithoutCancel(ctx), tx, delivery, "platform.notification.v1.NotificationStatusChanged", "platform.notification.status.changed.v1")
	})
}
func render(name, source string, variables map[string]string) (string, error) {
	parsed, err := template.New(name).Option("missingkey=error").Parse(source)
	if err != nil {
		return "", err
	}
	var output bytes.Buffer
	if err := parsed.Execute(&output, variables); err != nil {
		return "", err
	}
	return output.String(), nil
}
func retryBackoff(base time.Duration, attempt int32) time.Duration {
	shift := max(int32(0), min(attempt-1, 10))
	return base * time.Duration(1<<shift)
}

type Worker struct {
	dispatcher *Dispatcher
	cfg        config.Config
	logger     *slog.Logger
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

func NewWorker(lifecycle fx.Lifecycle, dispatcher *Dispatcher, cfg config.Config, logger *slog.Logger) *Worker {
	if cfg.Notification.DispatchInterval <= 0 {
		cfg.Notification.DispatchInterval = dispatcher.cfg.DispatchInterval
	}
	worker := &Worker{dispatcher: dispatcher, cfg: cfg, logger: logger}
	lifecycle.Append(fx.Hook{OnStart: worker.start, OnStop: worker.stop})
	return worker
}
func (w *Worker) start(context.Context) error {
	if !w.cfg.Database.Enabled {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		ticker := time.NewTicker(w.cfg.Notification.DispatchInterval)
		defer ticker.Stop()
		for {
			if _, err := w.dispatcher.RunOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
				w.logger.ErrorContext(ctx, "dispatch notifications failed", "error", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return nil
}
func (w *Worker) stop(context.Context) error {
	if w.cancel != nil {
		w.cancel()
		w.wg.Wait()
	}
	return nil
}

var WorkerModule = fx.Module("notification-worker", fx.Provide(NewSenderRegistry, NewDispatcher, NewWorker, NewRetentionCleaner), fx.Invoke(func(*Worker, *RetentionCleaner) {}))
