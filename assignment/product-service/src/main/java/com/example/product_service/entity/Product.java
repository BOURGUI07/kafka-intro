package com.example.product_service.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.springframework.data.annotation.Id;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class Product {

    @Id
    private Integer id;
    private String description;
    private Integer price;

}
