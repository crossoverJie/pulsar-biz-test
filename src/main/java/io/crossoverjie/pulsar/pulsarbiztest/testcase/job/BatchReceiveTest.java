package io.crossoverjie.pulsar.pulsarbiztest.testcase.job;

import io.crossoverjie.pulsar.pulsarbiztest.testcase.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;

/**
 * Function:
 *
 * @author crossoverJie
 * Date: 2022/12/13 19:20
 * @since JDK 11
 */
@Slf4j
public class BatchReceiveTest extends AbstractJobDefine {
    public BatchReceiveTest(Event event, String jobName,
                            PulsarClient pulsarClient, int timeout) {
        super(event, jobName, pulsarClient, timeout);
    }

    @Override
    public void run(PulsarClient pulsarClient, PulsarAdmin admin) throws Exception {
        String topic = "persistent://my-tenant/my-ns/my-topic-batch";
        int number = 5;

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionType(SubscriptionType.Shared)
                .batchReceivePolicy(BatchReceivePolicy.builder()
                        .maxNumMessages(number)
                        .build())
                .subscriptionName("my-sub")
                .subscribe();

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        for (int i = 0; i < number; i++) {
            String msg = "hello" + i;
            MessageId send = producer.newMessage()
                    .value(msg)
                    .send();
            log.info("batch send:{}", send.toString());
        }
        producer.close();

        int receiveNumber = 0;
        Messages<String> messages = consumer.batchReceive();
        for (Message<String> msg : messages) {
            log.info("batch consumer Message received: " + new String(msg.getData())
                    + " " + msg.getMessageId().toString());
            consumer.acknowledge(msg);
            receiveNumber++;
        }
        if (number != receiveNumber) {
            throw new BizException("消费者获取到的消息与发送消息数量不同");
        }
        consumer.close();
    }
}
