package io.crossoverjie.pulsar.pulsarbiztest.testcase.job;

import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.StrUtil;
import io.crossoverjie.pulsar.pulsarbiztest.testcase.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TopicMetadata;

/**
 * Function:
 *
 * @author crossoverJie
 * Date: 2022/12/13 19:20
 * @since JDK 11
 */
@Slf4j
public class CustomPartitionProducerTest extends AbstractJobDefine {


    public CustomPartitionProducerTest(Event event, String jobName, PulsarClient pulsarClient, int timeout,
                                       PulsarAdmin admin) {
        super(event, jobName, pulsarClient, timeout, admin);
    }

    @Override
    public void run(PulsarClient pulsarClient, PulsarAdmin admin) throws Exception {
        String topic = genTopic();
        admin.topics().createPartitionedTopic(topic, 4);

        int number = 50;

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

                .messageRoutingMode(MessageRoutingMode.CustomPartition)
                .messageRouter(new MessageRouter() {
                    @Override
                    public int choosePartition(Message<?> msg, TopicMetadata metadata) {
                        return 1;
                    }
                })
                .create();

        for (int i = 0; i < number; i++) {
            String msg = "" + i;
            MessageId send = producer.newMessage()
                    .value(msg)
                    .send();
            log.info("CustomPartitionProducer send:{}", send.toString());
        }
        producer.close();

        int receiveNumber = 0;
        for (int i = 0; i < number; i++) {
            Message<String> msg = consumer.receive();
            log.info("CustomPartitionProducer consumer message:{}, {}", msg.getValue(), msg.getMessageId().toString());
            consumer.acknowledge(msg);
            receiveNumber++;
        }
        if (number != receiveNumber) {
            throw new BizException("消费者获取到的消息与发送消息数量不同");
        }
        consumer.close();
    }

    private String genTopic() {
        String topic = "persistent://my-tenant/my-ns/my-topic-customer-prt-{}";
        return StrUtil.format(topic, UUID.fastUUID().toString(true));
    }
}
