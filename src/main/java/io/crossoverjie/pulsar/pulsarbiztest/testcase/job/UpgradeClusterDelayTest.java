package io.crossoverjie.pulsar.pulsarbiztest.testcase.job;

import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.StrUtil;
import io.crossoverjie.pulsar.pulsarbiztest.testcase.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.concurrent.TimeUnit;

/**
 * Function: 集群升级过程中的延迟消息测试
 *
 * @author crossoverJie
 * Date: 2022/12/12 23:05
 * @since JDK 11
 */
@Slf4j
public class UpgradeClusterDelayTest extends AbstractJobDefine {

    public UpgradeClusterDelayTest(Event event, String jobName, PulsarClient pulsarClient, int timeout) {
        super(event, jobName, pulsarClient, timeout);
    }

    @Override
    public void run(PulsarClient pulsarClient, PulsarAdmin admin) throws Exception {
        String topic = genTopic();
        int number = 10;

        Consumer consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("my-sub")
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();

        for (int i = 0; i < number; i++) {
            String msg = "hello" + i;
            MessageId send = producer.newMessage()
                    .deliverAfter(i, TimeUnit.MINUTES)
                    .value(msg.getBytes())
                    .send();
            log.info("delay send:{}",send.toString());
        }
        producer.close();

        int receiveNumber = 0;
        for (int i = 0; i < number; i++) {
            Message msg = consumer.receive();
           log.info("delay consumer Message received: " + new String(msg.getData())
                            + " " + msg.getMessageId().toString());
            consumer.acknowledge(msg);
            receiveNumber ++;
        }
        if (number != receiveNumber){
            throw new BizException("消费者获取到的消息与发送消息数量不同");
        }
        consumer.close();
    }

    private String genTopic(){
        String topic = "persistent://my-tenant/my-ns/my-topic-delay-{}";
        return StrUtil.format(topic, UUID.fastUUID().toString(true));
    }
}
