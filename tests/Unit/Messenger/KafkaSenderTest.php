<?php

declare(strict_types=1);

namespace Iperson1337\Kafka\Tests\Unit\Messenger;

use Iperson1337\Kafka\Messenger\KafkaSender;
use Iperson1337\Kafka\Messenger\KafkaSenderProperties;
use Iperson1337\Kafka\RdKafka\RdKafkaFactory;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use RdKafka\Conf as KafkaConf;
use RdKafka\Message;
use RdKafka\Producer as KafkaProducer;
use RdKafka\ProducerTopic;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

/**
 * @requires extension rdkafka
 */
class KafkaSenderTest extends TestCase
{
    /** @var MockObject|LoggerInterface */
    private $mockLogger;

    /** @var MockObject|KafkaProducer */
    private $mockProducer;

    private KafkaSender $sender;

    protected function setUp(): void
    {
        $this->mockLogger = $this->createMock(LoggerInterface::class);

        $serializer = $this->createMock(SerializerInterface::class);
        $serializer->method('encode')->willReturn(['body' => '{"data":null}', 'key' => '42', 'headers' => []]);

        $this->mockProducer = $this->createMock(KafkaProducer::class);
        $this->mockProducer->method('newTopic')->willReturn($this->createMock(ProducerTopic::class));

        $factory = $this->createMock(RdKafkaFactory::class);
        $factory->method('createProducer')->willReturn($this->mockProducer);

        $this->sender = new KafkaSender(
            $this->mockLogger,
            $serializer,
            $factory,
            new KafkaSenderProperties(new KafkaConf(), 'test', 10000, 0)
        );
    }

    public function testSendSucceedsWhenBrokerAcknowledgesTheMessage(): void
    {
        $this->flushDeliversReports(RD_KAFKA_RESP_ERR_NO_ERROR);
        $this->mockLogger->expects(self::once())->method('info')->with('Kafka message sent with key 42');

        $envelope = new Envelope(new TestMessage());

        self::assertSame($envelope, $this->sender->send($envelope));
    }

    public function testSendFailsWhenBrokerRejectsTheMessage(): void
    {
        $this->flushDeliversReports(RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED);
        $this->mockLogger->expects(self::never())->method('info');

        $this->expectException(TransportException::class);
        $this->expectExceptionCode(RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED);
        $this->expectExceptionMessage(rd_kafka_err2str(RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED));

        $this->sender->send(new Envelope(new TestMessage()));
    }

    public function testSendFailsWhenFlushSucceedsWithoutDeliveryReport(): void
    {
        $this->mockProducer->method('flush')->willReturn(RD_KAFKA_RESP_ERR_NO_ERROR);
        $this->mockLogger->expects(self::never())->method('info');

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('no delivery report');

        $this->sender->send(new Envelope(new TestMessage()));
    }

    public function testSendFailsWhenFlushTimesOut(): void
    {
        $this->mockProducer->method('flush')->willReturn(RD_KAFKA_RESP_ERR__TIMED_OUT);
        $this->mockLogger->expects(self::never())->method('info');

        $this->expectException(TransportException::class);
        $this->expectExceptionCode(RD_KAFKA_RESP_ERR__TIMED_OUT);

        $this->sender->send(new Envelope(new TestMessage()));
    }

    public function testRejectedMessageDoesNotFailTheNextSend(): void
    {
        $errors = [RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE, RD_KAFKA_RESP_ERR_NO_ERROR];
        $this->mockProducer->method('flush')->willReturnCallback(function () use (&$errors): int {
            $this->report(array_shift($errors));

            return RD_KAFKA_RESP_ERR_NO_ERROR;
        });

        try {
            $this->sender->send(new Envelope(new TestMessage()));
            self::fail('The rejected message must fail.');
        } catch (TransportException $e) {
            self::assertSame(RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE, $e->getCode());
        }

        $envelope = new Envelope(new TestMessage());

        self::assertSame($envelope, $this->sender->send($envelope));
    }

    private function flushDeliversReports(int ...$errors): void
    {
        $this->mockProducer->method('flush')->willReturnCallback(function () use ($errors): int {
            foreach ($errors as $err) {
                $this->report($err);
            }

            return RD_KAFKA_RESP_ERR_NO_ERROR;
        });
    }

    private function report(int $err): void
    {
        $message = new Message();
        $message->err = $err;

        $this->sender->onDeliveryReport($this->mockProducer, $message);
    }
}
