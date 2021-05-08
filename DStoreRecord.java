import java.io.*;
import java.net.Socket;

public class DStoreRecord {
    private BufferedReader reader;
    private PrintWriter writer;
    private Socket socket;

    public DStoreRecord(Socket socket) throws IOException {
        reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
        this.socket = socket;
    }

    public BufferedReader getReader() { return reader; }

    public PrintWriter getWriter() { return writer; }

    public Socket getSocket() { return socket; }
}