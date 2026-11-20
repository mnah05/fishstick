package messaging

import "github.com/rabbitmq/amqp091-go"

// DeclareDispatchQueue configures the durable direct exchange and worker queue
// used by both the outbox relay and execution workers.
func DeclareDispatchQueue(ch *amqp091.Channel, exchange, queueName, routingKey string) (amqp091.Queue, error) {
	if err := ch.ExchangeDeclare(exchange, "direct", true, false, false, false, nil); err != nil {
		return amqp091.Queue{}, err
	}
	queue, err := ch.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		return amqp091.Queue{}, err
	}
	if err := ch.QueueBind(queue.Name, routingKey, exchange, false, nil); err != nil {
		return amqp091.Queue{}, err
	}
	return queue, nil
}
