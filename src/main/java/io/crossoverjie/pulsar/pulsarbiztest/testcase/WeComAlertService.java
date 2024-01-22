package io.crossoverjie.pulsar.pulsarbiztest.testcase;

import feign.Headers;
import feign.RequestLine;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Function:
 *
 * @author crossoverJie
 * Date: 2022/12/1 16:44
 * @since JDK 11
 */
public interface WeComAlertService {
    @RequestLine("POST ")
    @Headers("Content-Type: application/json")
    WeComAlertResponse alert(WeComAlertRequest request);


    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class WeComAlertRequest {
        private String msgtype;
        private Markdown markdown;

        @Data
        @Builder
        @AllArgsConstructor
        @NoArgsConstructor
        public static class Markdown {
            private String content;
        }
    }

    @NoArgsConstructor
    @Data
    public static class WeComAlertResponse {

        private Integer errcode;
        private String errmsg;
    }

}
