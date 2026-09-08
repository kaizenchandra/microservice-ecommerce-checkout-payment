package com.synechisveltiosi.cartservice.infrastructure;

import com.synechisveltiosi.cartservice.domain.Cart;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.Optional;
import java.util.UUID;

public interface CartRepository extends JpaRepository<Cart, UUID> {
    Optional<Cart> findByIdAndCustomerId(UUID id, UUID customerId);
}
