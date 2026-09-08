package com.synechisveltiosi.cartservice.api;

import com.synechisveltiosi.cartservice.application.CartService;
import com.synechisveltiosi.cartservice.application.CartTransactions;
import jakarta.validation.Valid;
import jakarta.validation.constraints.PositiveOrZero;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.security.Principal;
import java.util.UUID;

@RestController
@RequestMapping("/api/carts")
public class CartController {
    private final CartTransactions transactions;
    private final CartService service;
    public CartController(CartTransactions transactions, CartService service) {
        this.transactions = transactions;
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<CartDtos.View> create(Principal principal) {
        var cart = transactions.create(UUID.fromString(principal.getName()));
        return ResponseEntity.created(URI.create("/api/carts/" + cart.id())).body(cart);
    }

    @GetMapping("/{id}")
    public CartDtos.View get(@PathVariable UUID id, Principal principal) {
        return transactions.get(id, UUID.fromString(principal.getName()));
    }

    @PostMapping("/{id}/items")
    public CartDtos.View add(@PathVariable UUID id, @Valid @RequestBody CartDtos.Add command,
                             Principal principal, @RequestHeader("Authorization") String authorization) {
        return service.change(id, UUID.fromString(principal.getName()), command.productId(), command.quantity(),
                command.expectedVersion(), CartTransactions.Mutation.ADD, authorization);
    }

    @PutMapping("/{id}/items/{productId}")
    public CartDtos.View set(@PathVariable UUID id, @PathVariable UUID productId,
                             @Valid @RequestBody CartDtos.SetQuantity command, Principal principal,
                             @RequestHeader("Authorization") String authorization) {
        return service.change(id, UUID.fromString(principal.getName()), productId, command.quantity(),
                command.expectedVersion(), CartTransactions.Mutation.SET, authorization);
    }

    @DeleteMapping("/{id}/items/{productId}")
    public CartDtos.View remove(@PathVariable UUID id, @PathVariable UUID productId,
                                @RequestParam @PositiveOrZero long expectedVersion, Principal principal,
                                @RequestHeader("Authorization") String authorization) {
        return service.change(id, UUID.fromString(principal.getName()), productId, 0,
                expectedVersion, CartTransactions.Mutation.REMOVE, authorization);
    }
}
