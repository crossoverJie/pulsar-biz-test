package io.crossoverjie.pulsar.pulsarbiztest.testcase.job;

import cn.hutool.core.date.StopWatch;
import cn.hutool.core.util.StrUtil;
import io.crossoverjie.pulsar.pulsarbiztest.PulsarBizTestApplication;
import io.crossoverjie.pulsar.pulsarbiztest.testcase.Event;
import lombok.Data;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Function:
 *
 * @author crossoverJie
 * Date: 2022/12/12 22:56
 * @since JDK 11
 */

@Data
public abstract class AbstractJobDefine {
    private Event event;
    private String jobName;
    private PulsarClient pulsarClient;

    private int timeout;

    private PulsarAdmin admin;

    public AbstractJobDefine(Event event, String jobName, PulsarClient pulsarClient, int timeout) {
        this.event = event;
        this.jobName = jobName;
        this.pulsarClient = pulsarClient;
        this.timeout = timeout;
    }

    public AbstractJobDefine(Event event, String jobName, PulsarClient pulsarClient, int timeout, PulsarAdmin admin) {
        this.event = event;
        this.jobName = jobName;
        this.pulsarClient = pulsarClient;
        this.timeout = timeout;
        this.admin = admin;
    }

    public void start() {
        event.addJob();
        try {
            CompletableFuture.runAsync(() -> {
                StopWatch watch = new StopWatch();
                try {
                    watch.start(jobName);
                    run(pulsarClient, admin);
                } catch (Exception e) {
                    event.oneException(this, e);
                } finally {
                    watch.stop();
                    event.finishOne(jobName, StrUtil.format("cost: {}s", watch.getTotalTimeSeconds()));
                }
            }, PulsarBizTestApplication.EXECUTOR).get(timeout, TimeUnit.SECONDS);
        } catch (Exception e) {
            event.oneException(this, e);
        }
    }


    /** run busy code
     * @param pulsarClient pulsar client
     * @param admin pulsar admin client
     * @throws Exception e
     */
    public abstract void run(PulsarClient pulsarClient, PulsarAdmin admin) throws Exception;


    @Data
    public static class BizException extends RuntimeException{
        public BizException(String message) {
            super(message);
        }
    }
}
