package com.example.analytics_service.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class ProductViewCount implements Persistable<Integer> {
    @Id
    private Integer id;
    private Long count;

    @Transient
    private boolean isNew;


    @Override
    public boolean isNew() {
        return isNew || id == null;
    }
}
