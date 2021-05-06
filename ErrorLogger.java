import java.io.*;
import java.sql.Timestamp;

public class ErrorLogger {
    private BufferedWriter writer;
    public ErrorLogger(String path) {
        try {
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void logError(String msg) {
        try {
            writer.write(new Timestamp(System.currentTimeMillis()) + " " + msg);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}