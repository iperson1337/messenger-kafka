<?php

declare(strict_types=1);

namespace Iperson1337\Kafka\Tests\Functional;

class TestMessage
{
    public $data;

    public function __construct($data)
    {
        $this->data = $data;
    }
}
