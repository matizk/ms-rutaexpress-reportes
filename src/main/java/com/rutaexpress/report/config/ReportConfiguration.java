package com.rutaexpress.report.config;

import java.time.Clock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

@Configuration
public class ReportConfiguration {
    @Bean
    Clock reportClock() {
        return Clock.systemUTC();
    }

    @Bean
    @Profile("!kafka")
    RestClient shipmentsRestClient(@Value("${report.shipments-base-url}") String shipmentsBaseUrl) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(3_000);
        factory.setReadTimeout(5_000);
        return RestClient.builder().baseUrl(shipmentsBaseUrl).requestFactory(factory).build();
    }
}
