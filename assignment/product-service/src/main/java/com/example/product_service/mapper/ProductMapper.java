package com.example.product_service.mapper;

import com.example.product_service.dto.ProductRequest;
import com.example.product_service.dto.ProductResponse;
import com.example.product_service.entity.Product;
import org.springframework.stereotype.Component;

@Component
public class ProductMapper {
    public Product toEntity(ProductRequest r) {
        return new Product().setDescription(r.description()).setPrice(r.price());
    }

    public ProductResponse toDto(Product p) {
        return new ProductResponse(p.getId(),p.getDescription(),p.getPrice());
    }
}
