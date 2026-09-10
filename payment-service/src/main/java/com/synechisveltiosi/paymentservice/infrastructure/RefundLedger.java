package com.synechisveltiosi.paymentservice.infrastructure;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

@Service
public class RefundLedger {
    private final JdbcTemplate jdbc;

    public RefundLedger(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Attempt attempt(UUID paymentId, String postalCode) {
        if (!"COMPLETED".equals(jdbc.queryForObject("SELECT outcome FROM provider_charge WHERE payment_id = ?", String.class, paymentId)))
            throw new IllegalArgumentException("Refund needs a successful charge");
        jdbc.update("INSERT INTO provider_refund(payment_id) VALUES (?) ON CONFLICT DO NOTHING", paymentId);
        var row = jdbc.queryForMap("SELECT * FROM provider_refund WHERE payment_id = ? FOR UPDATE", paymentId);
        int requests = ((Number) row.get("requests")).intValue() + 1;
        jdbc.update("UPDATE provider_refund SET requests = ? WHERE payment_id = ?", requests, paymentId);
        if (row.get("provider_reference") != null) return new Attempt((UUID) row.get("provider_reference"), false);
        if (postalCode.equals("REFUND-RETRY") && requests <= 2) return new Attempt(null, false);
        UUID reference = UUID.randomUUID();
        jdbc.update("UPDATE provider_refund SET refund_count = 1, provider_reference = ? WHERE payment_id = ?", reference, paymentId);
        return new Attempt(reference, postalCode.equals("REFUND-TIMEOUT"));
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Optional<UUID> lookup(UUID id) {
        return jdbc.query("SELECT provider_reference FROM provider_refund WHERE payment_id = ? AND refund_count = 1", (row, i) -> row.getObject(1, UUID.class), id).stream().findFirst();
    }

    public record Attempt(UUID reference, boolean responseLost) {
    }
}
