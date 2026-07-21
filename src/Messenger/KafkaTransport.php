<?php

declare(strict_types=1);

namespace Iperson1337\Kafka\Messenger;

use Iperson1337\Kafka\RdKafka\RdKafkaFactory;
use Psr\Log\LoggerInterface;
use RdKafka\Exception;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

class KafkaTransport implements TransportInterface
{
    private KafkaSender $sender;

    private KafkaReceiver $receiver;

    public function __construct(
        private readonly LoggerInterface         $logger,
        private readonly SerializerInterface     $serializer,
        private readonly RdKafkaFactory          $rdKafkaFactory,
        private readonly KafkaSenderProperties   $kafkaSenderProperties,
        private readonly KafkaReceiverProperties $kafkaReceiverProperties
    ) {
    }

    /**
     * @throws Exception
     */
    public function get(): iterable
    {
        return $this->getReceiver()->get();
    }

    /**
     * @throws Exception
     */
    public function ack(Envelope $envelope): void
    {
        $this->getReceiver()->ack($envelope);
    }

    public function reject(Envelope $envelope): void
    {
        $this->getReceiver()->reject($envelope);
    }

    /**
     * @throws Exception
     */
    public function send(Envelope $envelope): Envelope
    {
        return $this->getSender()->send($envelope);
    }

    private function getSender(): KafkaSender
    {
        return $this->sender ?? $this->sender = new KafkaSender(
            $this->logger,
            $this->serializer,
            $this->rdKafkaFactory,
            $this->kafkaSenderProperties
        );
    }

    private function getReceiver(): KafkaReceiver
    {
        return $this->receiver ?? $this->receiver = new KafkaReceiver(
            $this->logger,
            $this->serializer,
            $this->rdKafkaFactory,
            $this->kafkaReceiverProperties
        );
    }
}
