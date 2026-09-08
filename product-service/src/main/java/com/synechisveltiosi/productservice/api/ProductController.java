package com.synechisveltiosi.productservice.api;

import com.synechisveltiosi.productservice.application.ProductService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.UUID;

@RestController
@RequestMapping("/api/products")
public class ProductController {
    private final ProductService products;
    public ProductController(ProductService products) { this.products = products; }

    @GetMapping
    public ProductDtos.PageView list(@RequestParam(defaultValue = "0") @Min(0) @Max(10000) int page,
                                    @RequestParam(defaultValue = "20") @Min(1) @Max(100) int size) {
        return products.list(page, size);
    }

    @GetMapping("/{id}")
    public ProductDtos.View get(@PathVariable UUID id) { return products.get(id); }

    @PostMapping
    public ResponseEntity<ProductDtos.View> create(@Valid @RequestBody ProductDtos.Create command) {
        var product = products.create(command);
        return ResponseEntity.created(URI.create("/api/products/" + product.id())).body(product);
    }

    @PutMapping("/{id}")
    public ProductDtos.View update(@PathVariable UUID id, @Valid @RequestBody ProductDtos.Update command) {
        return products.update(id, command);
    }
}
