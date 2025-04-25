<?php

declare(strict_types=1);

namespace Iperson1337\Kafka\Messenger;

use RdKafka\Message;
use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

final readonly class KafkaMessageStamp implements NonSendableStampInterface
{

    public function __construct(private Message $message)
    {
    }

    public function getMessage(): Message
    {
        return $this->message;
    }
}
