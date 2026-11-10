package com.rutaexpress.report.client;

import com.rutaexpress.report.domain.ShipmentSnapshot;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.server.ResponseStatusException;

@Component
@Profile("!kafka")
public class ShipmentHttpClient implements ShipmentReader {
    private final RestClient restClient;

    public ShipmentHttpClient(RestClient shipmentsRestClient) {
        this.restClient = shipmentsRestClient;
    }

    @Override
    public List<ShipmentSnapshot> findCreatedBetween(LocalDate from, LocalDate to) {
        try {
            ShipmentSnapshot[] response = restClient.get()
                    .uri(uriBuilder -> uriBuilder.path("/api/shipments")
                            .queryParam("from", from).queryParam("to", to).build())
                    .retrieve().body(ShipmentSnapshot[].class);
            return response == null ? List.of() : Arrays.asList(response);
        } catch (RestClientException ex) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "shipment data is temporarily unavailable", ex);
        }
    }
}
