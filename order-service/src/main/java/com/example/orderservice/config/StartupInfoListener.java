package com.example.orderservice.config;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/** Prints a small banner with the URLs the service is reachable on once it's ready. */
@Component
public class StartupInfoListener implements ApplicationListener<ApplicationReadyEvent> {

	private static final Logger log = LoggerFactory.getLogger(StartupInfoListener.class);

	@Override
	public void onApplicationEvent(ApplicationReadyEvent event) {
		Environment env = event.getApplicationContext().getEnvironment();
		String name = env.getProperty("spring.application.name", "application");
		String port = env.getProperty("server.port", "8080");

		String host = "localhost";
		try {
			host = InetAddress.getLocalHost().getHostAddress();
		}
		catch (UnknownHostException ignored) {
			// fall back to localhost
		}

		log.info("""

				----------------------------------------------------------
				  {} is ready:
				  Local:     http://localhost:{}/
				  External:  http://{}:{}/
				  API:       http://localhost:{}/api/orders
				  Actuator:  http://localhost:{}/actuator
				----------------------------------------------------------""",
				name, port, host, port, port, port);
	}
}
