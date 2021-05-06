import java.io.*;
import java.net.Socket;

public class Helper {
    public static PrintWriter makeWriter(Socket socket) throws IOException {
        return new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
    }

    public static BufferedReader makeReader(Socket socket) throws IOException {
        return new BufferedReader(new InputStreamReader(socket.getInputStream()));
    }
}
