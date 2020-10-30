package example.gcp;

import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

//@Configuration
public class InitLoad {
    @Bean
    CommandLineRunner init() {
        return args -> {
            LoadAvroFromGCS.runLoadAvroFromGCS();
            LoadAvroFromGCS.runLoadAvroFromGCSNonOptionalFields();
        };
    }
}
