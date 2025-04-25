<?php

declare(strict_types=1);

namespace Iperson1337\Kafka\Messenger;

use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestFactoryInterface;
use Psr\Http\Message\StreamFactoryInterface;
use Psr\Http\Message\UriInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Receiver\MessageCountAwareInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

class RestProxyTransport implements TransportInterface, MessageCountAwareInterface
{
    private RestProxySender $sender;

    public function __construct(
        private readonly UriInterface            $baseUri,
        private readonly string                  $topicName,
        private readonly SerializerInterface     $serializer,
        private readonly ClientInterface         $client,
        private readonly RequestFactoryInterface $requestFactory,
        private readonly StreamFactoryInterface  $streamFactory,
    ) {
    }

    /**
     * @throws \Exception
     */
    public function get(): iterable
    {
        throw new \Exception('Not implemented!');
    }

    /**
     * @throws \Exception
     */
    public function ack(Envelope $envelope): void
    {
        throw new \Exception('Not implemented!');
    }

    /**
     * @throws \Exception
     */
    public function reject(Envelope $envelope): void
    {
        throw new \Exception('Not implemented!');
    }

    /**
     * {@inheritdoc}
     */
    public function send(Envelope $envelope): Envelope
    {
        return ($this->sender ?? $this->getSender())->send($envelope);
    }

    /**
     * @throws \Exception
     */
    public function getMessageCount(): int
    {
        throw new \Exception('Not implemented!');
    }

    private function getSender(): RestProxySender
    {
        return $this->sender = new RestProxySender(
            $this->baseUri,
            $this->topicName,
            $this->serializer,
            $this->client,
            $this->requestFactory,
            $this->streamFactory
        );
    }
}
