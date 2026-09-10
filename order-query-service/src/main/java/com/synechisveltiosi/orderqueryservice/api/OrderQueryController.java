package com.synechisveltiosi.orderqueryservice.api;

import com.synechisveltiosi.orderqueryservice.application.OrderQueries;
import com.synechisveltiosi.orderqueryservice.application.ProjectionTransactions;
import com.synechisveltiosi.orderqueryservice.domain.OrderView;
import com.synechisveltiosi.orderqueryservice.infrastructure.OrderDetailsClient;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/order-views")
public class OrderQueryController {
    private final OrderQueries queries;
    private final ProjectionTransactions projection;
    private final OrderDetailsClient details;

    public OrderQueryController(OrderQueries queries, ProjectionTransactions projection, OrderDetailsClient details) {
        this.queries = queries;
        this.projection = projection;
        this.details = details;
    }

    private boolean admin(Authentication auth) {
        return auth.getAuthorities().stream().anyMatch(role -> role.getAuthority().equals("ROLE_ADMIN"));
    }

    @GetMapping("/{id}")
    public OrderView get(@PathVariable UUID id, Authentication auth) {
        return queries.get(id, admin(auth) ? null : UUID.fromString(auth.getName()), admin(auth));
    }

    @GetMapping("/{id}/details")
    public OrderDetailsClient.Details details(@PathVariable UUID id, Authentication auth, HttpServletRequest request) {
        var view = queries.get(id, admin(auth) ? null : UUID.fromString(auth.getName()), admin(auth));
        return details.get(view, request.getHeader("Authorization"), request.getAttribute("correlationId").toString(), request.getHeader("traceparent"));
    }

    @GetMapping
    public OrderQueries.Page list(@RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "20") int size,
                                  @RequestParam(required = false) String status, Authentication auth) {
        return queries.list(admin(auth) ? null : UUID.fromString(auth.getName()), admin(auth), status, page, size);
    }

    @PostMapping("/admin/rebuild")
    public ResponseEntity<ProjectionTransactions.Rebuild> rebuild() {
        return ResponseEntity.accepted().body(projection.startRebuild());
    }

    @GetMapping("/admin/projection")
    public ProjectionTransactions.Status status() {
        return projection.status();
    }
}
