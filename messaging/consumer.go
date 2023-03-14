package messaging

import (
	"context"
	"errors"
	"fmt"
	"github.com/briares/test-containers/config"
	"time"

	"github.com/Azure/go-amqp"
	"go.uber.org/zap"
)

// The initial number of AMQP credits, which specifies how many unacknowledged messages can be in-flight before
// delivery is paused.
const defaultConsumerCredits = 10

type Consumer struct {
	session *Session

	receiverFactory receiverFactory
	semaphore       chan struct{}

	cfg *config.ConsumerConfig

	logger *zap.SugaredLogger
}

// receiverFactory allows replacing the actual AMQP receiver, e.g. for tests.
type receiverFactory func(
	ctx context.Context,
	session amqpSession,
	source string,
	opts *amqp.ReceiverOptions,
) (amqpReceiver, error)

// Handler is implemented by any component that wants to receive AMQP messages.
type Handler interface {
	Handle(message *amqp.Message) error
}

// HandlerFunc is the functional equivalent to Handler, it implements the functional interfaces pattern.
type HandlerFunc func(message *amqp.Message) error

func (h HandlerFunc) Handle(message *amqp.Message) error {
	return h(message)
}

type amqpReceiver interface {
	Receive(ctx context.Context) (*amqp.Message, error)
	AcceptMessage(ctx context.Context, msg *amqp.Message) error
	RejectMessage(ctx context.Context, msg *amqp.Message, e *amqp.Error) error
}

type ConsumerMetrics interface {
	IncMessageBrokerMessagesReceived(broker, queue string)
	IncMessageBrokerMessagesRejected(broker, queue string)
}

func NewConsumer(
	session *Session,
	cfg *config.ConsumerConfig,
	logger *zap.SugaredLogger,
) (*Consumer, error) {
	return &Consumer{
		session:         session,
		receiverFactory: setupReceiver,
		logger:          logger,
		cfg:             cfg,
		semaphore:       make(chan struct{}, cfg.MaxInflightMessages),
	}, nil
}

func setupReceiver(
	ctx context.Context,
	session amqpSession,
	source string,
	options *amqp.ReceiverOptions,
) (amqpReceiver, error) {
	return session.NewReceiver(ctx, source, options)
}

func (c *Consumer) Receive(ctx context.Context, handler Handler) error {
	for {
		c.session.connMutex.Lock()
		receiver, err := c.receiverFactory(ctx, c.session.session, c.cfg.Queue, &amqp.ReceiverOptions{
			Credit: defaultConsumerCredits,
		})
		c.session.connMutex.Unlock()

		if err != nil {
			return err
		}

		//nolint:errcheck // error is being logged, and we are reconnecting anyway
		_ = c.receive(ctx, receiver, c.cfg.Queue, handler)

		if ctx.Err() != nil {
			return nil //nolint:nilerr // intentionally returns nil, because we want to exit when the context is done
		}

		err = c.session.reconnect(ctx)
		if err != nil {
			return fmt.Errorf("reconnect: %w", err)
		}
	}
}

func (c *Consumer) receive(ctx context.Context, receiver amqpReceiver, queue string, handler Handler) error {
	backoff := c.cfg.InitialBackoff

	for {
		message, err := receiver.Receive(ctx)

		if err != nil && errors.Is(ctx.Err(), err) {
			return nil
		}

		if err != nil {
			c.logger.Warnw("receive error", "queue", queue, "backoff", backoff.String(), "error", err)

			time.Sleep(backoff)

			if backoff >= c.cfg.MaxBackoff {
				return err
			}

			backoff *= 2

			continue
		} else {
			backoff = c.cfg.InitialBackoff
		}

		c.semaphore <- struct{}{}

		go c.handle(ctx, receiver, queue, handler, message)
	}
}

func (c *Consumer) handle(
	ctx context.Context,
	receiver amqpReceiver,
	queue string,
	handler Handler,
	message *amqp.Message,
) {
	defer func() {
		<-c.semaphore
	}()

	defer func() {
		if r := recover(); r != nil {
			c.reject(ctx, receiver, queue, message, fmt.Sprintf("%v", r))
		}
	}()

	err := handler.Handle(message)
	if err != nil {
		c.reject(ctx, receiver, queue, message, err.Error())
	} else {
		err = receiver.AcceptMessage(ctx, message)
		if err != nil {
			c.logger.Warnw("accept message error",
				"queue", queue,
				"message-id", message.Properties.MessageID,
				"error", err)
		}
	}
}

func (c *Consumer) reject(
	ctx context.Context,
	receiver amqpReceiver,
	queue string,
	message *amqp.Message,
	description string,
) {
	c.logger.Warnw("rejecting message",
		"queue", queue,
		"message-id", message.Properties.MessageID,
		"description", description)

	err := receiver.RejectMessage(ctx, message, &amqp.Error{
		Condition:   amqp.ErrCondInternalError,
		Description: description,
		Info:        map[string]any{},
	})
	if err != nil {
		c.logger.Warnw("reject message error",
			"queue", queue,
			"message-id", message.Properties.MessageID,
			"error", err)
	}
}
