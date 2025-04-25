<?php

declare(strict_types=1);

namespace Iperson1337\Kafka\Messenger;

use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestFactoryInterface;
use Psr\Http\Message\StreamFactoryInterface;
use Psr\Http\Message\UriFactoryInterface;
use Psr\Log\LoggerInterface;
use function str_starts_with;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

readonly class RestProxyTransportFactory implements TransportFactoryInterface
{
    private const DSN_PROTOCOL_KAFKA_REST = 'kafka+rest';
    private const DSN_PROTOCOL_KAFKA_REST_SSL = 'kafka+rest+ssl';

    public function __construct(
        private ?LoggerInterface $logger,
        private ?ClientInterface          $client,
        private ?RequestFactoryInterface  $requestFactory,
        private ?UriFactoryInterface      $uriFactory,
        private ?StreamFactoryInterface   $streamFactory
    ) {
    }

    public function supports(string $dsn, array $options): bool
    {
        return str_starts_with($dsn, static::DSN_PROTOCOL_KAFKA_REST);
    }

    public function createTransport(string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        $this->checkDependencies();

        $dsn = $this->uriFactory->createUri($dsn);
        $scheme = $dsn->getScheme();

        $dsnOptions = $this->queryStringToOptionsArray($dsn->getQuery());
        $options = array_merge($dsnOptions, $options); // Override DSN options with options array

        $baseUri = $dsn->withQuery('');

        if ($scheme === static::DSN_PROTOCOL_KAFKA_REST) {
            $baseUri = $baseUri->withScheme('http');
        } elseif ($scheme === static::DSN_PROTOCOL_KAFKA_REST_SSL) {
            $baseUri = $baseUri->withScheme('https');
        } else {
            throw new \InvalidArgumentException('The DSN is not formatted as expected.');
        }

        return new RestProxyTransport(
            $baseUri,
            $options['topic'],
            $serializer,
            $this->client,
            $this->requestFactory,
            $this->streamFactory,
        );
    }

    private function queryStringToOptionsArray(string $queryString): array
    {
        $queryParts = explode('&', $queryString) ?? [];

        $dsnOptions = [];
        foreach ($queryParts as $queryPart) {
            list($key, $value) = explode('=', $queryPart);
            $dsnOptions[$key] = urldecode($value);
        }

        return $dsnOptions;
    }

    private function checkDependencies(): void
    {
        if ($this->client === null) {
            throw $this->createMissingServiceException(ClientInterface::class, 'PSR-7 HTTP Client not found.');
        }

        if ($this->requestFactory === null) {
            throw $this->createMissingServiceException(RequestFactoryInterface::class, 'PSR HTTP RequestFactory not found.');
        }

        if ($this->uriFactory === null) {
            throw $this->createMissingServiceException(UriFactoryInterface::class, 'PSR HTTP UriFactory not found.');
        }

        if ($this->streamFactory === null) {
            throw $this->createMissingServiceException(StreamFactoryInterface::class, 'PSR HTTP StreamFactory not found.');
        }
    }

    private function createMissingServiceException(string $className, ?string $message = null): \InvalidArgumentException
    {
        return new \InvalidArgumentException(sprintf(
            '%sPlease install a library that provides "%s" and ensure the service is registered.',
            $message ? $message . ' ' : '',
            $className
        ));
    }
}
