package example.gcp;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Base64;

@RestController
public class LoadController {
    private Log LOGGER = LogFactory.getLog(LoadController.class);

    @PostMapping(value = "/load", consumes = MediaType.APPLICATION_JSON_VALUE)

    public ResponseEntity load(@RequestBody Body body) {
        LOGGER.warn("Start load method!");

        (body.getMessage()).getData();
        Body.Message message = body.getMessage();
        if (message == null) {
            String msg = "Bad Request: invalid Pub/Sub message format";
            LOGGER.error(msg);
            return new ResponseEntity(msg, HttpStatus.BAD_REQUEST);
        }

        String pubSubMessage = message.getData();
        JsonObject data;
        try {
            String decodedMessage = new String(Base64.getDecoder().decode(pubSubMessage));
            data = JsonParser.parseString(decodedMessage).getAsJsonObject();
        } catch (Exception e) {
            String msg = "Error: Invalid Pub/Sub message: data property is not valid base64 encoded JSON";
            LOGGER.error(msg);
            return new ResponseEntity(msg, HttpStatus.BAD_REQUEST);
        }

        if (data.get("name") == null || data.get("bucket") == null) {
            String msg = "Error: Invalid Cloud Storage notification: expected name and bucket properties";
            LOGGER.error(msg);
            return new ResponseEntity(msg, HttpStatus.BAD_REQUEST);
        }

        LOGGER.warn("name & generation: " + data.get("name").getAsString() + " " + data.get("generation").getAsLong());
        if(LoadAvroFromGCS.load(data.get("name").getAsString(),data.get("generation").getAsLong())) {
            return new ResponseEntity(HttpStatus.OK);
        }

        return new ResponseEntity(HttpStatus.OK);
    }
    @GetMapping("/")
    @ResponseBody
    public String index() {
        return "index";
    }

}