package io.crossoverjie.pulsar.pulsarbiztest.testcase;


import io.crossoverjie.pulsar.pulsarbiztest.testcase.job.AbstractJobDefine;
import lombok.Builder;
import lombok.Data;

/**
 * Function:
 *
 * @author crossoverJie
 * Date: 2022/12/12 21:53
 * @since JDK 11
 */
public interface Event {

    /**
     * 新增一个任务
     */
    void addJob();

    /** 获取运行中的任务数量
     * @return 获取运行中的任务数量
     */
    TestCaseRuntimeResponse getRuntime();

    /**
     * 单个任务执行完毕
     *
     * @param jobName    任务名称
     * @param finishCost 任务完成耗时
     */
    void finishOne(String jobName, String finishCost);

    /**单个任务执行异常
     * @param jobDefine 任务
     * @param e 异常
     */
    void oneException(AbstractJobDefine jobDefine, Exception e);

    /**
     * 所有任务执行完毕
     */
    void finishAll();


    @Data
    @Builder
    public static class TestCaseRuntimeResponse {
        private int totalJobNumber;
        private int pendingJobNumber;
    }

}
