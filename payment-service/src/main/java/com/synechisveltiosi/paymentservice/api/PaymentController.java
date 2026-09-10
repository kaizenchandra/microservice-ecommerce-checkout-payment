package com.synechisveltiosi.paymentservice.api;

import com.synechisveltiosi.paymentservice.application.ApiException;
import com.synechisveltiosi.paymentservice.application.PaymentTransactions;
import com.synechisveltiosi.paymentservice.application.RefundTransactions;
import com.synechisveltiosi.paymentservice.infrastructure.OutboxPublisher;
import com.synechisveltiosi.paymentservice.infrastructure.ProviderLedger;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/payments")
public class PaymentController {
    private final PaymentTransactions payments;
    private final OutboxPublisher outbox;
    private final ProviderLedger provider;
    private final RefundTransactions refunds;

    public PaymentController(PaymentTransactions payments, OutboxPublisher outbox, ProviderLedger provider, RefundTransactions refunds) {
        this.payments = payments;
        this.outbox = outbox;
        this.provider = provider;
        this.refunds = refunds;
    }

    @GetMapping("/{orderId}")
    public PaymentDtos.View get(@PathVariable UUID orderId, Authentication authentication) {
        boolean admin = authentication.getAuthorities().stream().anyMatch(role -> role.getAuthority().equals("ROLE_ADMIN"));
        return payments.get(orderId, admin ? null : UUID.fromString(authentication.getName()), admin);
    }

    @PostMapping("/{orderId}/retry")
    public PaymentDtos.View retry(@PathVariable UUID orderId) {
        return payments.retry(orderId);
    }

    @GetMapping("/{orderId}/outbox")
    public List<OutboxPublisher.Delivery> deliveries(@PathVariable UUID orderId) {
        payments.get(orderId, null, true);
        return outbox.deliveries(orderId);
    }

    @GetMapping("/{orderId}/refund")
    public RefundTransactions.View refund(@PathVariable UUID orderId) {
        return refunds.get(orderId);
    }

    @GetMapping("/{orderId}/provider")
    public ProviderLedger.View provider(@PathVariable UUID orderId) {
        payments.get(orderId, null, true);
        return provider.view(orderId).orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "PROVIDER_NOT_CALLED", "Provider has no charge record yet"));
    }
}
