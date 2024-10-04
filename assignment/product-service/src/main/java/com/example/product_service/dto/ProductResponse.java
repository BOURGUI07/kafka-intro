package com.example.product_service.dto;

public record ProductResponse(
        Integer id,
        String description,
        Integer price
) {
}
