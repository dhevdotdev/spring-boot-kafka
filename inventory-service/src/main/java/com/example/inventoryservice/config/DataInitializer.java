package com.example.inventoryservice.config;

import java.util.List;

import com.example.inventoryservice.domain.Product;
import com.example.inventoryservice.repository.ProductRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/**
 * Seeds a few products on startup so the demo has stock to reserve. Idempotent:
 * only inserts a product if it isn't already there, so restarts don't reset
 * quantities you've drawn down.
 *
 * <ul>
 *   <li>WIDGET — plenty of stock (orders get CONFIRMED)</li>
 *   <li>GADGET — limited stock (order enough and you'll see REJECTED)</li>
 *   <li>GIZMO  — out of stock (always REJECTED)</li>
 * </ul>
 * Ordering the unseeded code {@code BOOM} triggers the retry/DLT path instead.
 */
@Component
public class DataInitializer implements CommandLineRunner {

	private static final Logger log = LoggerFactory.getLogger(DataInitializer.class);

	private final ProductRepository products;

	public DataInitializer(ProductRepository products) {
		this.products = products;
	}

	@Override
	public void run(String... args) {
		List<Product> seed = List.of(
				new Product("WIDGET", 100),
				new Product("GADGET", 5),
				new Product("GIZMO", 0));
		for (Product product : seed) {
			if (!products.existsById(product.getCode())) {
				products.save(product);
				log.info("Seeded product {} with {} units", product.getCode(), product.getAvailableQuantity());
			}
		}
	}
}
