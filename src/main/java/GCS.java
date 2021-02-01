import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class GCS {
    public static void main(String[] args) throws IOException {
//        String PROJECT_ID = "incorta-dev-qa";
//        String PATH_TO_JSON_KEY = "/home/omar/Downloads/incorta-dev-qa-7d591e4ef472.json";
//        String BUCKET_NAME = "sys-1474-gcstesting";
//        String OBJECT_NAME = "testGCS/test";
        FileReader reader=new FileReader("config.properties");

        Properties p=new Properties();
        p.load(reader);
        StorageOptions options = StorageOptions.newBuilder()
                .setProjectId((String) p.get("PROJECT_ID"))
                .setCredentials(GoogleCredentials.fromStream(
                        new FileInputStream((String) p.get("PATH_TO_JSON_KEY")))).build();

        Storage storage = options.getService();
        Blob blob = storage.get((String) p.get("BUCKET_NAME"), (String) p.get("OBJECT_NAME"));

//        reader(blob);
        writer(blob);
    }
    public static void writer(Blob blob) throws IOException {
        // [START writer]
        long startTime = System.currentTimeMillis();
        byte[] content = "Hello, from  terminal!".getBytes(StandardCharsets.UTF_8);
        try (WriteChannel writer = blob.writer()) {
            try {
                writer.write(ByteBuffer.wrap(content, 0, content.length));
            } catch (Exception ex) {
                // handle exception
            }
        }
        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println(estimatedTime + " Write time in ms");
    }
    public static void reader(Blob blob) {
        long startTime = System.currentTimeMillis();
        String s = new String(blob.getContent(), StandardCharsets.UTF_8);
        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println(estimatedTime + " Read time in ms");
        System.out.println(s);
    }
}
