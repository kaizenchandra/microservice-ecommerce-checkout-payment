package com.synechisveltiosi.orderqueryservice.application;

import com.synechisveltiosi.orderqueryservice.domain.OrderView;
import com.synechisveltiosi.orderqueryservice.infrastructure.ProjectionCodec;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

@Service
@Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
public class OrderQueries {
    private final JdbcTemplate jdbc;
    private final ProjectionCodec codec;

    public OrderQueries(JdbcTemplate jdbc, ProjectionCodec codec) {
        this.jdbc = jdbc;
        this.codec = codec;
    }

    public OrderView get(UUID id, UUID customer, boolean admin) {
        var rows = jdbc.query("""
                SELECT p.view::text FROM order_projection p JOIN projection_control c ON c.active_generation = p.generation
                WHERE p.order_id = ? AND (? OR p.customer_id = ?)
                """, (row, i) -> codec.read(row.getString(1), OrderView.class), id, admin, customer);
        return rows.stream().findFirst().orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "ORDER_VIEW_NOT_FOUND", "Order view not found"));
    }

    public Page list(UUID customer, boolean admin, String status, int page, int size) {
        if (page < 0 || page > 10000 || size < 1 || size > 100 || (status != null && !Set.of("PENDING", "COMPENSATING", "COMPLETED", "CANCELLED").contains(status)))
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_QUERY", "Invalid pagination or order status");
        long generation = jdbc.queryForObject("SELECT active_generation FROM projection_control WHERE singleton", Long.class);
        String where = " WHERE generation = ? AND (? OR customer_id = ?)" + (status == null ? "" : " AND status = ?");
        var args = new ArrayList<Object>(Arrays.asList(generation, admin, customer));
        if (status != null) args.add(status);
        long count = jdbc.queryForObject("SELECT count(*) FROM order_projection" + where, Long.class, args.toArray());
        args.add(size);
        args.add((long) page * size);
        var rows = jdbc.query("SELECT view::text FROM order_projection" + where + " ORDER BY created_at DESC, order_id LIMIT ? OFFSET ?",
                (row, i) -> codec.read(row.getString(1), OrderView.class), args.toArray());
        return new Page(rows, page, size, count, generation);
    }

    public record Page(List<OrderView> items, int page, int size, long totalElements, long generation) {
    }
}
