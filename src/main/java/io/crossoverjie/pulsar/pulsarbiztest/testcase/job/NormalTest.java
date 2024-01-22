package io.crossoverjie.pulsar.pulsarbiztest.testcase.job;

import io.crossoverjie.pulsar.pulsarbiztest.testcase.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * Function:
 *
 * @author crossoverJie
 * Date: 2022/12/12 22:59
 * @since JDK 11
 */
@Slf4j
public class NormalTest extends AbstractJobDefine {


    public NormalTest(Event event, String jobName, PulsarClient pulsarClient, int timeout) {
        super(event, jobName, pulsarClient, timeout);
    }

    @Override
    public void run(PulsarClient pulsarClient, PulsarAdmin admin) throws PulsarClientException {
        String topic = "persistent://my-tenant/my-ns/my-topic";
        int number = 10;
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();

        for (int i = 0; i < number; i++) {
            String msg = "hello" + i;
            MessageId send = producer.newMessage().value(msg.getBytes()).send();
            log.info("normal send {}",send.toString());
        }
        producer.close();

        Consumer consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("my-sub")
                .subscribe();

        int receiveNumber = 0;
        for (int i = 0; i < number; i++) {
            Message msg = consumer.receive();
            log.info(
                    "normal consumer Message received: " + new String(msg.getData())
                            + " " + msg.getMessageId().toString());
            consumer.acknowledge(msg);
            receiveNumber++;
        }
        if (number != receiveNumber){
            throw new BizException("消费者获取到的消息与发送消息数量不同");
        }
        consumer.close();
    }
}
