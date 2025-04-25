<?php

declare(strict_types=1);

namespace Iperson1337\Kafka\Messenger;


use AllowDynamicProperties;
use Iperson1337\Kafka\RdKafka\RdKafkaFactory;
use Psr\Log\LoggerInterface;
use RdKafka\Exception;
use RdKafka\KafkaConsumer;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Receiver\ReceiverInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

#[AllowDynamicProperties]
class KafkaReceiver implements ReceiverInterface
{
    private bool $subscribed;
    private ?KafkaConsumer $consumer = null;

    public function __construct(
        private readonly LoggerInterface         $logger,
        private readonly SerializerInterface     $serializer,
        private readonly RdKafkaFactory          $rdKafkaFactory,
        private readonly KafkaReceiverProperties $properties
    ) {
        $this->subscribed = false;
    }

    /**
     * @throws Exception
     */
    public function get(): iterable
    {
        $message = $this->getSubscribedConsumer()->consume($this->properties->getReceiveTimeoutMs());

        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                $this->logger->info(sprintf(
                    'Kafka: Message %s %s %s received ',
                    $message->topic_name,
                    $message->partition,
                    $message->offset
                ));

                $envelope = $this->serializer->decode([
                    'body' => $message->payload,
                    'headers' => $message->headers ?? [],
                    'key' => $message->key ?? null,
                    'offset' => $message->offset,
                    'timestamp' => $message->timestamp ?? null,
                    'topic_name' => $message->topic_name,
                ]);

                return [$envelope->with(new KafkaMessageStamp($message))];
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                $this->logger->info('Kafka: Partition EOF reached. Waiting for next message ...');
                break;
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                $this->logger->debug('Kafka: Consumer timeout.');
                break;
            case RD_KAFKA_RESP_ERR__TRANSPORT:
                $this->logger->debug('Kafka: Broker transport failure.');
                break;
            default:
                throw new TransportException($message->errstr(), $message->err);
        }

        return [];
    }

    /**
     * @throws Exception
     */
    public function ack(Envelope $envelope): void
    {
        $consumer = $this->getConsumer();

        /** @var KafkaMessageStamp|null $transportStamp */
        $transportStamp = $envelope->last(KafkaMessageStamp::class);

        if (null === $transportStamp) {
            $this->logger->warning('No KafkaMessageStamp found on the Envelope.');
            return;
        }

        $message = $transportStamp->getMessage();

        if ($this->properties->isCommitAsync()) {
            $consumer->commitAsync($message);

            $this->logger->info(sprintf(
                'Offset topic=%s partition=%s offset=%s to be committed asynchronously.',
                $message->topic_name,
                $message->partition,
                $message->offset
            ));
        } else {
            $consumer->commit($message);

            $this->logger->info(sprintf(
                'Offset topic=%s partition=%s offset=%s successfully committed.',
                $message->topic_name,
                $message->partition,
                $message->offset
            ));
        }
    }

    public function reject(Envelope $envelope): void
    {
        // Do nothing. auto commit should be set to false!
    }

    /**
     * @throws Exception
     */
    private function getSubscribedConsumer(): KafkaConsumer
    {
        $consumer = $this->getConsumer();

        if (false === $this->subscribed) {
            $this->logger->info('Partition assignment...');
            $consumer->subscribe([$this->properties->getTopicName()]);

            $this->subscribed = true;
        }

        return $consumer;
    }

    private function getConsumer(): KafkaConsumer
    {
        return $this->consumer ?? $this->consumer = $this->rdKafkaFactory->createConsumer($this->properties->getKafkaConf());
    }
}
