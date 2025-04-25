<?php

declare(strict_types=1);

namespace Iperson1337\Kafka\Messenger;

use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestFactoryInterface;
use Psr\Http\Message\StreamFactoryInterface;
use Psr\Http\Message\UriInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

readonly class RestProxySender implements SenderInterface
{
    public function __construct(
        private UriInterface            $baseUri,
        private string                  $topicName,
        private SerializerInterface     $serializer,
        private ClientInterface         $client,
        private RequestFactoryInterface $requestFactory,
        private StreamFactoryInterface  $streamFactory
    ) {
    }

    /**
     * {@inheritdoc}
     */
    public function send(Envelope $envelope): Envelope
    {
        $encoded = $this->serializer->encode($envelope);

        $request = $this->requestFactory->createRequest('POST', $this->baseUri->withPath('/topics/' . $this->topicName));
        $request = $request->withHeader('Accept', 'application/vnd.kafka.v2+json');
        $request = $request->withHeader('Content-Type', $encoded['headers']['Content-Type']);
        $request = $request->withBody($this->streamFactory->createStream(
            '{ "records": [ { "key": "' . $encoded['key'] . '", "value": "' . $encoded['body'] . '" } ] }'
        ));

        $response = $this->client->sendRequest($request);

        if ($response->getStatusCode() === 204) {
            return $envelope;
        }

        if ($response->getStatusCode() === 200) {
            return $envelope; // $response->getBody(); TODO: do something
        }

        return $envelope;
    }
}
