import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/*
ASYNC_THROUGH
[Experimental] Write the file asynchronously to the under fs.
CACHE_THROUGH
Write the file synchronously to the under fs, and also try to write to the highest tier in a worker's com.Alluxio storage.
MUST_CACHE
Write the file, guaranteeing the data is written to com.Alluxio storage or failing the operation.
NONE
Do not store the data in com.Alluxio or Under Storage.
THROUGH
Write the file synchronously to the under fs, skipping com.Alluxio storage.
 */
public class Alluxio {
    public static void main(String[] args) throws IOException, AlluxioException {
//        System.out.println("isdnf");
        AlluxioURI path = new AlluxioURI("alluxio://192.168.1.7:19998/a33");
        readFile(path);
//        writeFile(path, WritePType.CACHE_THROUGH);
    }
    public static void readFile(AlluxioURI path) throws IOException, AlluxioException {
        FileSystem fs = FileSystem.Factory.get();
        FileInStream in = fs.openFile(path);
        long startTime = System.currentTimeMillis();
        System.out.println(Arrays.toString(in.readAllBytes()));
        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println(estimatedTime + " Read time in ms");
//        System.out.println(s);
        in.close();
    }
    public static void writeFile(AlluxioURI path, WritePType writePType) throws IOException, AlluxioException {
        long startTime = System.currentTimeMillis();
        FileSystem fs = FileSystem.Factory.get();
        //creating file there directly (like cp command to alluxio or UFS)
        FileOutStream out = fs.createFile(path, CreateFilePOptions.newBuilder().setWriteType(writePType).build()); //set the write type in the parameters.
        String s = "asidfn"; //content of the file
//        byte[] a = FileUtils.readFileToByteArray(file); //file type is java io file
        byte[] a = s.getBytes(StandardCharsets.UTF_8); //file type is java io file
        out.write(a);
        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println(estimatedTime + " Write time in ms");
        out.close();

    }
}


/* TODOS:
1.repair read function(readAllBytes())
2.push to github and clone from the VM
3.run from VM
 */
