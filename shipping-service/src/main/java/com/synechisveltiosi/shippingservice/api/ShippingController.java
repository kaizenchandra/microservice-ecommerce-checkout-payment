package com.synechisveltiosi.shippingservice.api;

import com.synechisveltiosi.shippingservice.application.ShippingTransactions;
import com.synechisveltiosi.shippingservice.infrastructure.OutboxPublisher;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/shipping")
public class ShippingController {
    private final ShippingTransactions shipping;
    private final OutboxPublisher outbox;

    public ShippingController(ShippingTransactions shipping, OutboxPublisher outbox) {
        this.shipping = shipping;
        this.outbox = outbox;
    }

    @GetMapping("/{orderId}")
    public ShippingTransactions.View get(@PathVariable UUID orderId) {
        return shipping.get(orderId);
    }

    @GetMapping("/{orderId}/outbox")
    public List<OutboxPublisher.Delivery> outbox(@PathVariable UUID orderId) {
        shipping.get(orderId);
        return outbox.deliveries(orderId);
    }

    @GetMapping("/{orderId}/details")
    public ShippingTransactions.View details(@PathVariable UUID orderId, org.springframework.security.core.Authentication auth) {
        var result = shipping.get(orderId);
        boolean admin = auth.getAuthorities().stream().anyMatch(role -> role.getAuthority().equals("ROLE_ADMIN"));
        if (!admin && !result.customerId().toString().equals(auth.getName()))
            throw new com.synechisveltiosi.shippingservice.application.ApiException(org.springframework.http.HttpStatus.NOT_FOUND, "SHIPMENT_NOT_FOUND", "Shipment not found");
        return result;
    }
}
