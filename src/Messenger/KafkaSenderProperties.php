<?php

declare(strict_types=1);

namespace Iperson1337\Kafka\Messenger;

use RdKafka\Conf as KafkaConf;

final readonly class KafkaSenderProperties
{
    public function __construct(
        private KafkaConf $kafkaConf,
        private string    $topicName,
        private int       $flushTimeoutMs,
        private int       $flushRetries
    ) {
    }

    public function getKafkaConf(): KafkaConf
    {
        return $this->kafkaConf;
    }

    public function getTopicName(): string
    {
        return $this->topicName;
    }

    public function getFlushTimeoutMs(): int
    {
        return $this->flushTimeoutMs;
    }

    public function getFlushRetries(): int
    {
        return $this->flushRetries;
    }
}
