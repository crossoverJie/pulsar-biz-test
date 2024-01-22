package io.crossoverjie.pulsar.pulsarbiztest.testcase;

import cn.hutool.core.util.StrUtil;
import io.crossoverjie.pulsar.pulsarbiztest.testcase.job.AbstractJobDefine;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Function:
 *
 * @author crossoverJie
 * Date: 2022/12/12 21:58
 * @since JDK 11
 */
@Slf4j
public class EventImpl implements Event {

    private WeComAlertService weComAlertService;

    public EventImpl(WeComAlertService weComAlertService) {
        this.weComAlertService = weComAlertService;
    }

    private AtomicInteger pendingJobNumber = new AtomicInteger();
    private AtomicInteger totalJobNumber = new AtomicInteger();
    private Map<String, String> exceptionJob = new HashMap<>();
    private Map<String, String> finishJob = new HashMap<>();

    @Override
    public void addJob() {
        pendingJobNumber.incrementAndGet();
        totalJobNumber.incrementAndGet();
    }

    @Override
    public TestCaseRuntimeResponse getRuntime() {
        return TestCaseRuntimeResponse.builder()
                .totalJobNumber(totalJobNumber.get())
                .pendingJobNumber(pendingJobNumber.get())
                .build();
    }

    @Override
    public void finishOne(String jobName, String finishCost) {
        pendingJobNumber.decrementAndGet();
        finishJob.put(jobName, finishCost);
    }

    @Override
    public void oneException(AbstractJobDefine jobDefine, Exception e) {
        log.error("job:{} exception:{}", jobDefine.getJobName(), e);
        if (e instanceof TimeoutException) {
            // 任务执行超时的异常，需要手动减去运行任务数量
            pendingJobNumber.decrementAndGet();
            exceptionJob.put(jobDefine.getJobName(), StrUtil.format("timeoutException:{}s", jobDefine.getTimeout()));
        } else {
            exceptionJob.put(jobDefine.getJobName(), e.toString());
        }
    }

    @Override
    public void finishAll() {
        String title = "Pulsar 新版本测试报告\n";
        StringBuilder sb = new StringBuilder();
        sb.append(title);
        finishJob.forEach((k, v) -> {
            sb.append(StrUtil.format("> 任务名称:{}, {}\n", k, v));
        });

        exceptionJob.forEach((k, v) -> {
            sb.append(StrUtil.format("> <font color=\"warning\">失败任务名称</font>:{}, {}\n", k, v));
        });
        WeComAlertService.WeComAlertResponse response = weComAlertService.alert(WeComAlertService.WeComAlertRequest.builder()
                .msgtype("markdown")
                .markdown(WeComAlertService.WeComAlertRequest.Markdown.builder()
                        .content(sb.toString())
                        .build())
                .build());
        log.info("response={}", response.toString());

    }
}
