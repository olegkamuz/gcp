package example.gcp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class LoadApp {

    private static Log LOGGER = LogFactory.getLog(LoadController.class);
    public static void main(String[] args) {
        SpringApplication.run(LoadApp.class, args);
    }

}