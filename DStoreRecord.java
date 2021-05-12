import java.io.*;
import java.net.Socket;

public class DStoreRecord {
    private PrintWriter writer;
    private Socket socket;

    public DStoreRecord(Socket socket) {
        try {
            writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);
            this.socket = socket;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public PrintWriter getWriter() { return writer; }

    public Socket getSocket() { return socket; }
}