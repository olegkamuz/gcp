package example.gcp;

import org.springframework.web.bind.annotation.*;
import example.gcp.LoadAvroFromGCS;
import org.springframework.web.bind.annotation.ResponseBody;

@RestController
public class LoadController {
    @GetMapping("/")
    @ResponseBody
    public String index() {
        return "index";
    }
    @PostMapping("/load_avro_all")
    public String loadAll() {
        LoadAvroFromGCS.runLoadAvroFromGCS();
        System.out.println("avro all done!");
        return "avro all done!";
    }
    @PostMapping("/load_avro_non_optional")
    public String loadNonOptional() {
        LoadAvroFromGCS.runLoadAvroFromGCSNonOptionalFields();
        System.out.println("avro non-optional done!");
        return "avro non-optional done!";
    }
}