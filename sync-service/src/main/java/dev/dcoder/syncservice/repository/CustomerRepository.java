package dev.dcoder.syncservice.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface CustomerRepository extends JpaRepository<dev.dcoder.syncservice.model.Customer, Long> {
}

