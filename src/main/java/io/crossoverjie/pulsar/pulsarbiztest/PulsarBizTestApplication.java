package io.crossoverjie.pulsar.pulsarbiztest;

import cn.hutool.core.thread.NamedThreadFactory;
import cn.hutool.core.util.StrUtil;
import io.crossoverjie.pulsar.pulsarbiztest.testcase.Event;
import io.crossoverjie.pulsar.pulsarbiztest.testcase.EventImpl;
import io.crossoverjie.pulsar.pulsarbiztest.testcase.job.AbstractJobDefine;
import io.crossoverjie.pulsar.pulsarbiztest.testcase.job.BatchReceiveTest;
import io.crossoverjie.pulsar.pulsarbiztest.testcase.job.CustomPartitionProducerTest;
import io.crossoverjie.pulsar.pulsarbiztest.testcase.job.DelayTest;
import io.crossoverjie.pulsar.pulsarbiztest.testcase.job.FailoverConsumerTest;
import io.crossoverjie.pulsar.pulsarbiztest.testcase.job.NormalTest;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@SpringBootApplication
@Slf4j
@RestController
public class PulsarBizTestApplication {

    private Event currentEvent;

    public static void main(String[] args) {
        SpringApplication.run(PulsarBizTestApplication.class, args);
    }


    @PostMapping("/testcase/trigger")
    public String trigger(String serviceUrl, String token)
            throws PulsarClientException {
        log.info("trigger serviceUrl={}, token={}", serviceUrl, token);
        PulsarClient pulsarClient = getPulsarClient(serviceUrl, token);

        PulsarAdmin admin = getPulsarAdmin(serviceUrl, token);

        Event event = new EventImpl();
        currentEvent = event;
        CompletableFuture.runAsync(() -> {
            try {
                run(pulsarClient, event, admin);
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        return "ok";
    }

    private static PulsarAdmin getPulsarAdmin(String serviceUrl, String token) throws PulsarClientException {
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(StrUtil.format("http://{}:8080", serviceUrl))
                .authentication(AuthenticationFactory.token(token))
                .build();
        return admin;
    }

    private static PulsarClient getPulsarClient(String serviceUrl, String token) throws PulsarClientException {
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(StrUtil.format("pulsar://{}:6650", serviceUrl))
                .authentication(AuthenticationFactory.token(token))
                .build();
        return pulsarClient;
    }

    public final static Executor EXECUTOR = Executors.newFixedThreadPool(10, new NamedThreadFactory("testcase", false));

    public void run(PulsarClient pulsarClient, Event event, PulsarAdmin admin)
            throws ExecutionException, InterruptedException {
        try {
            admin.tenants().createTenant("my-tenant", TenantInfo.builder()
                    .build());
            admin.namespaces().createNamespace("my-tenant/my-ns");
        } catch (PulsarAdminException e) {
            log.error("create namespace error", e);
        }

        AbstractJobDefine job1 = new NormalTest(event, "通用消息测试", pulsarClient, 30);
        CompletableFuture<Void> c1 = CompletableFuture.runAsync(job1::start, EXECUTOR);

        AbstractJobDefine job2 = new DelayTest(event, "延迟消息测试", pulsarClient, 10);
        CompletableFuture<Void> c2 = CompletableFuture.runAsync(job2::start, EXECUTOR);

        AbstractJobDefine job3 = new BatchReceiveTest(event, "批量消费消息测试", pulsarClient, 10);
        CompletableFuture<Void> c3 = CompletableFuture.runAsync(job3::start, EXECUTOR);

        AbstractJobDefine job4 =
                new CustomPartitionProducerTest(event, "自定义生产者路由消息测试", pulsarClient, 20, admin);
        CompletableFuture<Void> c4 = CompletableFuture.runAsync(job4::start, EXECUTOR);

        AbstractJobDefine job5 =
                new FailoverConsumerTest(event, "故障转移消费测试", pulsarClient, 20, admin);
        CompletableFuture<Void> c5 = CompletableFuture.runAsync(job5::start, EXECUTOR);

        CompletableFuture<Void> all = CompletableFuture.allOf(c1, c2, c3, c4, c5);
        all.whenComplete((___, __) -> {
            event.finishAll();
            pulsarClient.closeAsync();
            admin.close();
        }).get();
    }
}
