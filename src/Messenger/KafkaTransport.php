<?php

declare(strict_types=1);

namespace Iperson1337\Kafka\Messenger;

use Iperson1337\Kafka\RdKafka\RdKafkaFactory;
use Psr\Log\LoggerInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

class KafkaTransport implements TransportInterface
{

    /** @var KafkaSender */
    private KafkaSender $sender;

    /** @var KafkaReceiver */
    private KafkaReceiver $receiver;

    public function __construct(
        private readonly LoggerInterface         $logger,
        private readonly SerializerInterface     $serializer,
        private readonly RdKafkaFactory          $rdKafkaFactory,
        private readonly KafkaSenderProperties   $kafkaSenderProperties,
        private readonly KafkaReceiverProperties $kafkaReceiverProperties
    ) {
    }

    public function get(): iterable
    {
        return $this->getReceiver()->get();
    }

    public function ack(Envelope $envelope): void
    {
        $this->getReceiver()->ack($envelope);
    }

    public function reject(Envelope $envelope): void
    {
        $this->getReceiver()->reject($envelope);
    }

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
