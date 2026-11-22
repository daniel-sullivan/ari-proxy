package messagebus

import (
	"errors"
	"sync"
	"time"

	"github.com/CyCoreSystems/ari/v5/rid"
	"github.com/rabbitmq/amqp091-go"
)

// RmqSubscription handle RabbitMQ subscription
type RmqSubscription struct {
	consumerID string
	channel    *amqp091.Channel
	mu         sync.RWMutex
	done       chan struct{}
	cancelOnce sync.Once

	Topics       []string
	Queue        string
	Exchange     string
	ExchangeKind string
	QueueArgs    amqp091.Table
}

// Unsubscribe remove the subscription
func (rs *RmqSubscription) Unsubscribe() error {
	rs.cancelOnce.Do(func() { close(rs.done) })
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.consumerID == "" {
		return nil
	}
	err := rs.channel.Cancel(rs.consumerID, false)
	closeErr := rs.channel.Close()
	rs.consumerID = ""
	return errors.Join(err, closeErr)
}

// reconnect reconnects the subscription
func (rs *RmqSubscription) reconnect(r *RabbitmqBus) <-chan amqp091.Delivery {
	rs.mu.Lock()

	if rs.channel != nil {
		rs.channel.Close() // nolint: errcheck
	}
	rs.mu.Unlock()
	return rs.retrySubscription(r.done, func() (<-chan amqp091.Delivery, error) {
		return rs.execute(r)
	}, func(err error) {
		r.Log.Error("failed to execute subscription", "error", err)
	})
}

func (rs *RmqSubscription) retrySubscription(busDone <-chan struct{}, attempt func() (<-chan amqp091.Delivery, error), logError func(error)) <-chan amqp091.Delivery {
	for {
		select {
		case <-rs.done:
			return nil
		case <-busDone:
			return nil
		default:
		}
		msgs, err := attempt()
		if err != nil {
			logError(err)
			timer := time.NewTimer(DefaultReconnectionWait)
			select {
			case <-rs.done:
				timer.Stop()
				return nil
			case <-busDone:
				timer.Stop()
				return nil
			case <-timer.C:
			}
			continue
		}
		return msgs
	}
}

// execute declares the subscription on RabbitMQ
func (rs *RmqSubscription) execute(r *RabbitmqBus) (<-chan amqp091.Delivery, error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	select {
	case <-rs.done:
		return nil, errors.New("subscription cancelled")
	default:
	}

	ch, err := r.newChannel()
	if err != nil {
		return nil, err
	}

	rs.channel = ch
	defer func() {
		if err != nil {
			ch.Close() // nolint: errcheck
		}
	}()
	queue, err := ch.QueueDeclare(
		rs.Queue,     // name of queue
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // nowait
		rs.QueueArgs, // arguments
	)
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		rs.Exchange,     // name of exchange
		rs.ExchangeKind, // kind
		true,            // durable
		false,           // delete when unused
		false,           // internal
		false,           // nowait
		nil,             // arguments
	)
	if err != nil {
		return nil, err
	}
	if queue.Name != rs.Queue {
		rs.Queue = queue.Name
	}

	for _, topic := range rs.Topics {
		err = ch.QueueBind(queue.Name, topic, rs.Exchange, false, nil)
		if err != nil {
			return nil, err
		}
	}

	rs.consumerID = rid.New(ridConsumer)
	msgs, err := ch.Consume(rs.Queue, rs.consumerID, false, false, true, true, nil)
	return msgs, err
}
