<?php

declare(strict_types=1);

namespace Iperson1337\Kafka\Messenger;

use RdKafka\Conf as KafkaConf;

final readonly class KafkaReceiverProperties
{
    public function __construct(
        private KafkaConf $kafkaConf,
        private string    $topicName,
        private int       $receiveTimeoutMs,
        private bool      $commitAsync
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

    public function getReceiveTimeoutMs(): int
    {
        return $this->receiveTimeoutMs;
    }

    public function isCommitAsync(): bool
    {
        return $this->commitAsync;
    }
}
