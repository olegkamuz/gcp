package example.gcp.controller;

import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import example.gcp.service.utils.LoadAvroFromGCSImpl;
import example.gcp.Body;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RestController
@Slf4j
public class LoadController {
    @Autowired
    private LoadAvroFromGCSImpl loadAvroFromGCSImpl;

    @PostMapping(value = "/load", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> load(@RequestBody Body body) {

        log.warn("Start load method!");

        Optional<Body.Message> optMessage = Optional.ofNullable(body.getMessage());
        if (!optMessage.isPresent()) {
            String msg = "Bad Request: invalid Pub/Sub message format";
            log.error(msg);
            return new ResponseEntity<>(msg, HttpStatus.BAD_REQUEST);
        }
        Body.Message message = optMessage.get();

        String pubSubMessage = message.getData();
        JsonObject data;
        try {
            String decodedMessage = new String(Base64.getDecoder().decode(pubSubMessage));
            data = JsonParser.parseString(decodedMessage).getAsJsonObject();
        } catch (Exception e) {
            String msg = "Error: Invalid Pub/Sub message: data property is not valid base64 encoded JSON";
            log.error(msg);
            return new ResponseEntity<>(msg, HttpStatus.BAD_REQUEST);
        }

        if (data.get("name") == JsonNull.INSTANCE ||
            data.get("bucket") == JsonNull.INSTANCE ||
            data.get("name") == null || data.get("bucket") == null) {
            String msg = "Error: Invalid Cloud Storage notification: expected name and bucket properties";
            log.error(msg);
            return new ResponseEntity<>(msg, HttpStatus.BAD_REQUEST);
        }

        String name = data.get("name").getAsString();
        Long generation = data.get("generation").getAsLong();

        log.warn(String.format(
                "name & generation: %s %s",
                name,
                generation
        ));

        if(!loadAvroFromGCSImpl.load(name,generation)) {
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return new ResponseEntity<>(HttpStatus.OK);
    }
    @GetMapping("/")
    @ResponseBody
    public String index() {
        return "index";
    }

}