package com.example.inventoryservice.repository;

import java.util.UUID;

import com.example.inventoryservice.domain.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, UUID> {
}
