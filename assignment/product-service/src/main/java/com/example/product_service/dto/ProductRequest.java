package com.example.product_service.dto;

public record ProductRequest(
        String description,
        Integer price
) {
}
