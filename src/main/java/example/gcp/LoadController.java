package example.gcp;

import org.springframework.web.bind.annotation.*;

@RestController
public class LoadController {
    @PostMapping("/load")
    public String load() {
        LoadAvroFromGCS.load();
        return "done";
    }
    @GetMapping("/")
    @ResponseBody
    public String index() {
        return "index";
    }

}