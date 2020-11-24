package example.gcp.service;

public interface LoadAvroFromGCS {
    boolean load(String name, Long generation);
}
