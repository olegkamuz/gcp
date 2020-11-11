package example.gcp;


import lombok.Data;

@Data
public class Body {

    private Message message;

    public Body() {}

    @Data
    public class Message {

        private String messageId;
        private String publishTime;
        private String data;

        public Message() {}

        public Message(String messageId, String publishTime, String data) {
            this.messageId = messageId;
            this.publishTime = publishTime;
            this.data = data;
        }

    }
}