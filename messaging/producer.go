package messaging

import (
	"context"
	"errors"
	"github.com/briares/test-containers/config"
	"sync"
	"time"

	"github.com/Azure/go-amqp"
	"go.uber.org/zap"
)

type Producer struct {
	connMutex     sync.Mutex
	session       *Session
	senderFactory senderFactory

	cfg *config.ProducerConfig

	logger *zap.SugaredLogger
}

// senderFactory allows replacing the actual AMQP sender, e.g. for tests.
type senderFactory func(
	ctx context.Context,
	session amqpSession,
	target string,
	opts *amqp.SenderOptions,
) (amqpSender, error)

type amqpSender interface {
	Send(ctx context.Context, msg *amqp.Message) error
}

type ProducerMetrics interface {
	IncMessageBrokerMessagesSent(broker, queue string)
}

func NewProducer(
	session *Session,
	cfg *config.ProducerConfig,
	logger *zap.SugaredLogger,
) (*Producer, error) {
	return &Producer{
		session:       session,
		senderFactory: setupSender,
		logger:        logger,
		cfg:           cfg,
	}, nil
}

func setupSender(
	ctx context.Context,
	session amqpSession,
	target string,
	options *amqp.SenderOptions,
) (amqpSender, error) {
	return session.NewSender(ctx, target, options)
}

func (p *Producer) Publish(ctx context.Context, message []byte) error {
	return p.send(ctx, amqp.NewMessage(message))
}

func (p *Producer) send(ctx context.Context, msg *amqp.Message) error {
	p.connMutex.Lock()
	sender, err := p.senderFactory(ctx, p.session.session, p.cfg.Topic, &amqp.SenderOptions{})
	p.connMutex.Unlock()

	if err != nil {
		return err
	}

	if err := p.sendWithBackoff(ctx, sender, p.cfg.Topic, msg); err != nil {
		return err
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	return nil
}

func (p *Producer) sendWithBackoff(ctx context.Context, sender amqpSender, topic string, msg *amqp.Message) error {
	backoff := p.cfg.InitialBackoff

	for {
		err := sender.Send(ctx, msg)

		if err != nil && errors.Is(ctx.Err(), err) {
			return nil
		}

		if err != nil {
			time.Sleep(backoff)

			if backoff >= p.cfg.MaxBackoff {
				return err
			}

			backoff *= 2

			continue
		}

		break
	}

	return nil
}
