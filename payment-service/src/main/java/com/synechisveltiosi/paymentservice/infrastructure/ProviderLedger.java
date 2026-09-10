package com.synechisveltiosi.paymentservice.infrastructure;

import com.synechisveltiosi.paymentservice.application.PaymentProvider;
import com.synechisveltiosi.paymentservice.application.PaymentTransactions;
import com.synechisveltiosi.paymentservice.domain.ChargeResult;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Models the simulator's independent commit boundary. No application payment row is accessed here.
 */
@Service
public class ProviderLedger {
    private final JdbcTemplate jdbc;
    private final JsonMapper json;

    public ProviderLedger(JdbcTemplate jdbc, JsonMapper json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    private static void verify(Map<String, Object> row, String hash) {
        if (!hash.equals(row.get("request_hash")))
            throw new IllegalArgumentException("Provider key reused for different instructions");
    }

    private static ChargeResult result(Map<String, Object> row) {
        return new ChargeResult((String) row.get("outcome"), (UUID) row.get("provider_reference"));
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, timeout = 5)
    public Attempt attempt(PaymentProvider.Request request) {
        String hash = PaymentTransactions.hash(json.writeValueAsString(request));
        jdbc.update("INSERT INTO provider_charge(payment_id, request_hash) VALUES (?, ?) ON CONFLICT DO NOTHING", request.paymentId(), hash);
        var row = jdbc.queryForMap("SELECT * FROM provider_charge WHERE payment_id = ? FOR UPDATE", request.paymentId());
        verify(row, hash);
        int requests = ((Number) row.get("requests")).intValue() + 1;
        jdbc.update("UPDATE provider_charge SET requests = ? WHERE payment_id = ?", requests, request.paymentId());
        if (row.get("outcome") != null) return new Attempt(result(row), false, false);
        if (request.paymentToken().equals("tok_error") && requests <= 2) return new Attempt(null, false, true);
        boolean declined = request.paymentToken().equals("tok_declined");
        var result = new ChargeResult(declined ? "FAILED" : "COMPLETED", declined ? null : UUID.randomUUID());
        jdbc.update("UPDATE provider_charge SET outcome = ?, provider_reference = ?, charge_count = ? WHERE payment_id = ?",
                result.status(), result.providerReference(), declined ? 0 : 1, request.paymentId());
        return new Attempt(result, request.paymentToken().equals("tok_timeout"), false);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Optional<ChargeResult> lookup(PaymentProvider.Request request) {
        var rows = jdbc.queryForList("SELECT * FROM provider_charge WHERE payment_id = ?", request.paymentId());
        if (rows.isEmpty()) return Optional.empty();
        var row = rows.getFirst();
        verify(row, PaymentTransactions.hash(json.writeValueAsString(request)));
        return row.get("outcome") == null ? Optional.empty() : Optional.of(result(row));
    }

    public Optional<View> view(UUID id) {
        return jdbc.query("SELECT outcome, provider_reference, requests, charge_count FROM provider_charge WHERE payment_id = ?",
                        (row, index) -> new View(id, row.getString("outcome"), row.getObject("provider_reference", UUID.class), row.getInt("requests"), row.getInt("charge_count")), id)
                .stream().findFirst();
    }

    public record Attempt(ChargeResult result, boolean responseLost, boolean unavailable) {
    }

    public record View(UUID paymentId, String outcome, UUID providerReference, int requests, int chargeCount) {
    }
}
