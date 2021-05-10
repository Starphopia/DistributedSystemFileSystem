import java.io.*;
import java.sql.Timestamp;

public class ErrorLogger {
    private PrintWriter writer;
    public ErrorLogger(String path) {
        try {
            writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void logError(String msg) {
        writer.println(new Timestamp(System.currentTimeMillis()) + " " + msg);
        writer.flush();
        System.out.println(msg);
    }
}