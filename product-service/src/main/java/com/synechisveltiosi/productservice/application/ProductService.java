package com.synechisveltiosi.productservice.application;

import com.synechisveltiosi.productservice.api.ProductDtos;
import com.synechisveltiosi.productservice.domain.Product;
import com.synechisveltiosi.productservice.infrastructure.ProductRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class ProductService {
    private final ProductRepository products;

    public ProductService(ProductRepository products) { this.products = products; }

    @Transactional(readOnly = true)
    public ProductDtos.PageView list(int page, int size) {
        var result = products.findByActiveTrue(PageRequest.of(page, size, Sort.by("name").and(Sort.by("id"))));
        return new ProductDtos.PageView(result.map(ProductDtos.View::from).getContent(), page, size,
                result.getTotalElements(), result.getTotalPages());
    }

    @Transactional(readOnly = true)
    public ProductDtos.View get(UUID id) { return ProductDtos.View.from(find(id)); }

    @Transactional
    public ProductDtos.View create(ProductDtos.Create command) {
        Product product = new Product(command.sku(), command.name(), command.description(), command.price().toMoney());
        return ProductDtos.View.from(products.saveAndFlush(product));
    }

    @Transactional
    public ProductDtos.View update(UUID id, ProductDtos.Update command) {
        Product product = find(id);
        if (product.version() != command.expectedVersion()) {
            throw new ApiException(HttpStatus.CONFLICT, "STALE_VERSION", "Product changed; reload before updating");
        }
        product.revise(command.name(), command.description(), command.price().toMoney(), command.active());
        products.flush();
        return ProductDtos.View.from(product);
    }

    private Product find(UUID id) {
        return products.findById(id).orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "PRODUCT_NOT_FOUND", "Product not found"));
    }
}
