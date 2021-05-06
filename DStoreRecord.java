import java.io.*;
import java.net.Socket;

public class DStoreRecord {
    BufferedReader reader;
    PrintWriter writer;

    public DStoreRecord(Socket socket) throws IOException {
        reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
    }

    public BufferedReader getReader() { return reader; }

    public PrintWriter getWriter() {
        return writer;
    }
}