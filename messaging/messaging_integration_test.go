//go:build integration

package messaging_test

import (
	"bytes"
	"context"
	"fmt"
	"github.com/briares/test-containers/config"
	"github.com/briares/test-containers/log"
	"github.com/briares/test-containers/messaging"
	"io"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	oneGigabyte         = 1024 * 1024 * 1024
	solaceContainerName = "testcontainers_solace"
)

var (
	// Initialized by TestMain, contains the URL to connect to the Solace testcontainer.
	solaceTestcontainerAMQPUrl string //nolint:gochecknoglobals // Needed for the test setup
	solaceTestcontainerAPIUrl  string //nolint:gochecknoglobals // Needed for the test setup
)

func TestMain(m *testing.M) {
	exitCode := func() int {
		solaceContainer, err := setupSolace(context.Background())
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "failed to set up solace: %v", err)

			return 1
		}

		defer func() {
			if terminateErr := solaceContainer.Terminate(context.Background()); terminateErr != nil {
				_, _ = fmt.Fprintf(os.Stderr, "failed to terminate solace: %v", terminateErr)
				os.Exit(1)
			}
		}()

		solaceTestcontainerAMQPUrl, solaceTestcontainerAPIUrl, err = setupSolacePorts(context.Background(), solaceContainer)
		if err != nil {
			return 1
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestAMQPConnection(t *testing.T) {
	logger, err := log.NewAtLevel("debug")
	require.NoError(t, err)

	solaceSession, err := messaging.NewSession(context.Background(),
		&config.ConnectionConfig{
			AMQPUrl:  solaceTestcontainerAMQPUrl,
			Username: "",
			Password: "",
		}, logger.Sugar())
	require.NoError(t, err)

	_, err = messaging.NewConsumer(solaceSession, &config.ConsumerConfig{Queue: ""}, logger.Sugar())
	require.NoError(t, err)

	err = solaceSession.Close(context.Background())
	require.NoError(t, err)
}

func TestProducer_PublishMessage(t *testing.T) {
	logger, err := log.NewAtLevel("debug")
	require.NoError(t, err)

	createQueue(context.Background(), t, "test-queue-publish-message", "test-topic-publish")
	defer deleteQueue(context.Background(), t, "test-queue-publish-message")

	solaceSession, err := messaging.NewSession(context.Background(),
		&config.ConnectionConfig{
			AMQPUrl: solaceTestcontainerAMQPUrl,
		}, logger.Sugar())
	require.NoError(t, err)

	p, err := messaging.NewProducer(solaceSession, &config.ProducerConfig{
		ConnectionConfig: config.ConnectionConfig{
			AMQPUrl: solaceTestcontainerAMQPUrl,
		},
		Topic: "topic://test-topic-publish",
	}, logger.Sugar())
	require.NoError(t, err)

	defer solaceSession.Close(context.Background())

	messagePayload := []byte("Hello Publisher")
	err = p.Publish(context.Background(), messagePayload)
	require.NoError(t, err)

	message := readMessage(t, "test-queue-publish-message")
	require.Equal(t, string(messagePayload), string(message.GetData()))
}

func publishMessages(t *testing.T, topic string, messages []*amqp.Message) {
	conn, err := amqp.Dial(solaceTestcontainerAMQPUrl, &amqp.ConnOptions{
		SASLType: amqp.SASLTypeAnonymous(),
		Timeout:  1 * time.Minute,
	})
	require.NoError(t, err)

	sess, err := conn.NewSession(context.Background(), nil)
	require.NoError(t, err)

	sender, err := sess.NewSender(context.Background(), "topic://"+topic, nil)
	require.NoError(t, err)

	for _, msg := range messages {
		require.NoError(t, sender.Send(context.Background(), msg))
	}
}

func readMessage(t *testing.T, queue string) *amqp.Message {
	conn, err := amqp.Dial(solaceTestcontainerAMQPUrl, &amqp.ConnOptions{
		SASLType: amqp.SASLTypeAnonymous(),
		Timeout:  1 * time.Minute,
	})
	require.NoError(t, err)

	sess, err := conn.NewSession(context.Background(), nil)
	require.NoError(t, err)

	receiver, err := sess.NewReceiver(context.Background(), queue, nil)
	require.NoError(t, err)

	message, err := receiver.Receive(context.Background())
	require.NoError(t, err)

	err = receiver.AcceptMessage(context.Background(), message)
	require.NoError(t, err)

	return message
}

func createQueue(ctx context.Context, t *testing.T, queue, subscribedTopic string) {
	queueModel := fmt.Sprintf(`{
	"accessType": "non-exclusive",
	"ingressEnabled": true,
	"egressEnabled": true,
	"msgVpnName": "default",
	"permission": "consume",
	"queueName": %q
}`, queue)
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		solaceTestcontainerAPIUrl+"/SEMP/v2/config/msgVpns/default/queues",
		bytes.NewReader([]byte(queueModel)),
	)
	require.NoError(t, err)

	req.SetBasicAuth("admin", "admin")
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)

	_, err = io.Copy(io.Discard, resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	subscriptionModel := fmt.Sprintf(`{
	"msgVpnName": "default",
	"queueName": %q,
	"subscriptionTopic": %q
}`, queue, subscribedTopic)

	req, err = http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		solaceTestcontainerAPIUrl+"/SEMP/v2/config/msgVpns/default/queues/"+queue+"/subscriptions",
		bytes.NewReader([]byte(subscriptionModel)),
	)
	require.NoError(t, err)

	req.SetBasicAuth("admin", "admin")
	req.Header.Set("Content-Type", "application/json")

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)

	_, err = io.Copy(io.Discard, resp.Body)
	require.NoError(t, err)

	require.NoError(t, resp.Body.Close())
}

func deleteQueue(ctx context.Context, t *testing.T, queue string) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodDelete,
		solaceTestcontainerAPIUrl+"/SEMP/v2/config/msgVpns/default/queues/"+url.QueryEscape(queue),
		http.NoBody,
	)
	require.NoError(t, err)

	req.SetBasicAuth("admin", "admin")
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)

	_, err = io.Copy(io.Discard, resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
}

func setupSolace(ctx context.Context) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "solace/solace-pubsub-standard:10.2.1.32",
		ShmSize:      oneGigabyte,
		ExposedPorts: []string{"5672/tcp", "8080/tcp"},
		WaitingFor:   wait.ForListeningPort("5672/tcp"),
		Name:         solaceContainerName,
		Env: map[string]string{
			"USERNAME_ADMIN_GLOBALACCESSLEVEL": "admin",
			"USERNAME_ADMIN_PASSWORD":          "admin",
		},
	}

	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Reuse:            true,
	})
}

func setupSolacePorts(
	ctx context.Context,
	solaceContainer testcontainers.Container,
) (amqpURL, sempURL string, err error) {
	amqpURL, err = solaceContainer.PortEndpoint(ctx, "5672/tcp", "amqp")
	if err != nil {
		return "", "", fmt.Errorf("getting solace amqp endpoint: %w", err)
	}

	sempURL, err = solaceContainer.PortEndpoint(ctx, "8080/tcp", "http")
	if err != nil {
		return "", "", fmt.Errorf("getting solace semp endpoint: %w", err)
	}

	return amqpURL, sempURL, nil
}
