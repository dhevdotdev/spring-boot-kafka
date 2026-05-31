package com.example.orderservice.repository;

import java.util.UUID;

import com.example.orderservice.domain.OrderEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<OrderEntity, UUID> {
}
