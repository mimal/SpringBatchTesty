package chapter1.batch.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource("persistence-generic-entity.properties")
public class H2JpaConfig {
}
