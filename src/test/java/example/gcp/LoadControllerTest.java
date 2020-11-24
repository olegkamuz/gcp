package example.gcp;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import example.gcp.service.utils.LoadAvroFromGCSImpl;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.io.File;
import java.nio.file.Files;
import java.util.Base64;

@SpringBootTest
@AutoConfigureMockMvc
public class LoadControllerTest {

    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private ObjectMapper objectMapper;
    @MockBean
    private LoadAvroFromGCSImpl loadAvroFromGCSImpl;

    @Test
    public void shouldReturnBadRequestStatusMessageNull() throws Exception {
        Body body = new Body();

        this.mockMvc.perform(post("/load")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(body)))
                .andExpect(status().isBadRequest());
    }

    @Test
    public void shouldReturnBadRequestStatusNotValidBase64() throws Exception {
        File resource = new ClassPathResource("obj.json").getFile();
        String jsonNotEncoded = new String(Files.readAllBytes(resource.toPath()));

        Body body = new Body();
        body.setMassage("abc", "abc", jsonNotEncoded);

        this.mockMvc.perform(post("/load")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(body)))
                .andExpect(status().isBadRequest());
    }

    @Test
    public void shouldReturnBadRequestStatusNotInvalidCloudStorageNotificationNoNameNoBucket() throws Exception {
        File resource = new ClassPathResource("objNoNameNoBucket.json").getFile();
        byte[] byteArr = Files.readAllBytes(resource.toPath());
        String jsonEncoded = new String(Base64.getEncoder().encode(byteArr));

        Body body = new Body();
        body.setMassage("abw", "abw", jsonEncoded);

        this.mockMvc.perform(post("/load")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(body)))
                .andExpect(status().isBadRequest());
    }

    @Test
    public void shouldReturnBadRequestStatusNotInvalidCloudStorageNotificationNullNameNullBucket() throws Exception {
        File resource = new ClassPathResource("objNullNameNullBucket.json").getFile();
        byte[] byteArr = Files.readAllBytes(resource.toPath());
        String jsonEncoded = new String(Base64.getEncoder().encode(byteArr));

        Body body = new Body();
        body.setMassage("abw", "abw", jsonEncoded);

        this.mockMvc.perform(post("/load")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(body)))
                .andExpect(status().isBadRequest());

    }

    @Test
    public void shouldReturnInternalServerErrorMockServiceFalse() throws Exception {
        File resource = new ClassPathResource("obj.json").getFile();
        byte[] byteArr = Files.readAllBytes(resource.toPath());
        String jsonNotEncoded = new String(byteArr);
        String jsonEncoded = new String(Base64.getEncoder().encode(byteArr));

        Body body = new Body();
        body.setMassage("abw", "abw", jsonEncoded);

        JsonObject data = JsonParser.parseString(jsonNotEncoded).getAsJsonObject();

        when(loadAvroFromGCSImpl.load(data.get("name").getAsString(), data.get("generation").getAsLong())).thenReturn(false);
        this.mockMvc.perform(post("/load")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(body)))
                .andExpect(status().isInternalServerError());

    }

    @Test
    public void shouldReturnOkRequestMockServiceTrue() throws Exception {
        File resource = new ClassPathResource("obj.json").getFile();
        byte[] byteArr = Files.readAllBytes(resource.toPath());
        String jsonNotEncoded = new String(byteArr);
        String jsonEncoded = new String(Base64.getEncoder().encode(byteArr));

        Body body = new Body();
        body.setMassage("abw", "abw", jsonEncoded);

        JsonObject data = JsonParser.parseString(jsonNotEncoded).getAsJsonObject();

        when(loadAvroFromGCSImpl.load(data.get("name").getAsString(), data.get("generation").getAsLong())).thenReturn(true);
        this.mockMvc.perform(post("/load")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(body)))
                .andExpect(status().isOk());

    }
}