package messaging

import (
	"context"
	"github.com/briares/test-containers/config"
	"io"
	"os"
	"sync"
	"time"

	"github.com/Azure/go-amqp"
	"go.uber.org/zap"
)

type Session struct {
	// connMutex protects both conn and session from concurrent access
	connMutex sync.Mutex
	conn      amqpConn
	session   amqpSession

	dialer dialer

	cfg *config.ConnectionConfig

	logger *zap.SugaredLogger
}

// dialer allows replacing the connection and session setup with custom logic, e.g. for tests.
type dialer func(context.Context, *config.ConnectionConfig) (amqpConn, amqpSession, error)

func NewSession(
	ctx context.Context,
	cfg *config.ConnectionConfig,
	logger *zap.SugaredLogger,
) (*Session, error) {
	conn, session, err := connect(ctx, cfg)
	if err != nil {
		return nil, err
	}

	logger.Infow("connected", "url", cfg.AMQPUrl)

	return &Session{
		conn:    conn,
		session: session,
		dialer:  connect,
		logger:  logger,
		cfg:     cfg,
	}, nil
}

type amqpConn interface {
	NewSession(ctx context.Context, opts *amqp.SessionOptions) (*amqp.Session, error)
	io.Closer
}

type amqpSession interface {
	NewReceiver(ctx context.Context, source string, opts *amqp.ReceiverOptions) (*amqp.Receiver, error)
	NewSender(ctx context.Context, target string, opts *amqp.SenderOptions) (*amqp.Sender, error)
	Close(ctx context.Context) error
}

func connect(ctx context.Context, cfg *config.ConnectionConfig) (amqpConn, amqpSession, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, nil, err
	}

	authType := amqp.SASLTypeAnonymous()
	if cfg.Username != "" && cfg.Password != "" {
		authType = amqp.SASLTypePlain(cfg.Username, cfg.Password)
	}

	conn, err := amqp.Dial(cfg.AMQPUrl, &amqp.ConnOptions{
		SASLType:    authType,
		ContainerID: hostname,
		Timeout:     1 * time.Minute,
	})
	if err != nil {
		return nil, nil, err
	}

	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		return nil, nil, err
	}

	return conn, session, nil
}

func (s *Session) Close(ctx context.Context) error {
	s.connMutex.Lock()
	defer s.connMutex.Unlock()

	err := s.session.Close(ctx)
	if err != nil {
		return err
	}

	return s.conn.Close()
}

func (s *Session) reconnect(ctx context.Context) error {
	s.logger.Infow("reconnecting", "url", s.cfg.AMQPUrl)

	conn, session, err := s.dialer(ctx, s.cfg)
	if err != nil {
		return err
	}

	s.connMutex.Lock()
	oldConn := s.conn
	oldSession := s.session

	s.conn = conn
	s.session = session
	s.connMutex.Unlock()

	// Ignore any error closing the session and context, as they might be broken already.
	_ = oldSession.Close(ctx)
	_ = oldConn.Close()

	s.logger.Infow("reconnected", "url", s.cfg.AMQPUrl)

	return nil
}
