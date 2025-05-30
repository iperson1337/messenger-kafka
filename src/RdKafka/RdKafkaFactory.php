<?php

declare(strict_types=1);

namespace Iperson1337\Kafka\RdKafka;

use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\Producer as KafkaProducer;

class RdKafkaFactory
{
    public function createConsumer(Conf $conf): KafkaConsumer
    {
        return new KafkaConsumer($conf);
    }

    public function createProducer(Conf $conf): KafkaProducer
    {
        return new KafkaProducer($conf);
    }
}
