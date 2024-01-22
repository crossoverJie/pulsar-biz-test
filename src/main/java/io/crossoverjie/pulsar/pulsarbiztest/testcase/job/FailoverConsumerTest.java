package io.crossoverjie.pulsar.pulsarbiztest.testcase.job;

import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.StrUtil;
import io.crossoverjie.pulsar.pulsarbiztest.PulsarBizTestApplication;
import io.crossoverjie.pulsar.pulsarbiztest.testcase.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Function:
 *
 * @author crossoverJie
 * Date: 2022/12/13 19:20
 * @since JDK 11
 */
@Slf4j
public class FailoverConsumerTest extends AbstractJobDefine {

    public FailoverConsumerTest(Event event, String jobName, PulsarClient pulsarClient, int timeout,
                                PulsarAdmin admin) {
        super(event, jobName, pulsarClient, timeout, admin);
    }

    @Override
    public void run(PulsarClient pulsarClient,PulsarAdmin admin) throws Exception {
        String topic = genTopic();
        admin.topics().createPartitionedTopic(topic, 4);

        int number = 30;

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionType(SubscriptionType.Failover)
                .batchReceivePolicy(BatchReceivePolicy.builder()
                        .maxNumMessages(number)
                        .build())
                .subscriptionName("my-sub")
                .subscribe();

        Consumer<String> consumer1 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionType(SubscriptionType.Failover)
                .batchReceivePolicy(BatchReceivePolicy.builder()
                        .maxNumMessages(number)
                        .build())
                .subscriptionName("my-sub")
                .subscribe();

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        for (int i = 0; i < number; i++) {
            String msg = "" + i;
            MessageId send = producer.newMessage()
                    .value(msg)
                    .send();
            log.info("Failover send:{}", send.toString());
        }
        producer.close();

        AtomicInteger receiveNumber = new AtomicInteger();
        CompletableFuture<Void> c1 = CompletableFuture.runAsync(() -> {
            for (int i = 0; i < number; i++) {
                if (i == 10) {
                    try {
                        consumer.close();
                    } catch (PulsarClientException e) {
                        throw new RuntimeException(e);
                    }
                    return;
                }
                Message<String> msg;
                try {
                    msg = consumer.receive();
                    log.info("Failover consumer0 message:{}, {}", msg.getValue(), msg.getMessageId().toString());
                    consumer.acknowledge(msg);
                    receiveNumber.incrementAndGet();
                } catch (PulsarClientException e) {
                    throw new RuntimeException(e);
                }
            }

        }, PulsarBizTestApplication.EXECUTOR);
        CompletableFuture<Void> c2 = CompletableFuture.runAsync(() -> {
            for (int i = 0; i < number - 10; i++) {
                Message<String> msg;
                try {
                    msg = consumer1.receive();
                    log.info("Failover consumer1 message:{}, {}", msg.getValue(), msg.getMessageId().toString());
                    consumer1.acknowledge(msg);
                    receiveNumber.incrementAndGet();
                } catch (PulsarClientException e) {
                    throw new RuntimeException(e);
                }
            }
        }, PulsarBizTestApplication.EXECUTOR);

        CompletableFuture.allOf(c1, c2).whenComplete((__, ___) -> {
            if (number != receiveNumber.get()) {
                throw new BizException("消费者获取到的消息与发送消息数量不同");
            }
        }).get();
        consumer1.close();
    }

    private String genTopic() {
        String topic = "persistent://my-tenant/my-ns/my-topic-customer-prt-{}";
        return StrUtil.format(topic, UUID.fastUUID().toString(true));
    }
}
