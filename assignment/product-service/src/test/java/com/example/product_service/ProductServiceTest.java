package com.example.product_service;

import com.example.product_service.dto.ProductResponse;
import com.example.product_service.event.ProductViewEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.test.StepVerifier;

@AutoConfigureWebTestClient
@SpringBootTest
public class ProductServiceTest extends AbstractIntegrationTest{

    @Autowired
    private WebTestClient client;


    @Test
    public void test1(){
        //view products
        //check if events are emitted
        viewProductSucess(1);
        viewProductSucess(1);
        viewProductError(1000);
        viewProductSucess(5);
        var flux = this.<ProductViewEvent>createReceiver(PRODUCT_VIEW_EVENTS)
                .receive()
                .take(3);

        flux.as(StepVerifier::create)
                .consumeNextWith(record -> Assertions.assertEquals(1,record.value().getProductId()))
                .consumeNextWith(record -> Assertions.assertEquals(1,record.value().getProductId()))
                .consumeNextWith(record -> Assertions.assertEquals(5,record.value().getProductId()))
                .verifyComplete();

    }

    private void viewProductSucess(Integer id){
        client.get().uri("/products/"+id)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.id").isEqualTo(id)
                .jsonPath("$.description").isEqualTo("product-"+id);
    }

    private void viewProductError(Integer id){
        client.get().uri("/products/"+id)
                .exchange()
                .expectStatus().is4xxClientError();

    }


}
