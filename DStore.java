import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

public class DStore {
    private Socket port, cport;
    private double timeout;
    private String fileFolder;

    public static void main(String[] args) {
        try {
            new DStore(Integer.parseInt(args[0]), Integer.parseInt(args[1]), 
                       Double.parseDouble(args[2]), args[3]);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param port          Port to listen to 
     * @param cport         Port to talk to the controller
     * @param timeout       (ms)
     * @param fileFolder    Directory where the data will be stored locally
     */
    public DStore(int port, int cport, double timeout, String fileFolder) {
        this.timeout = timeout;
        this.fileFolder = fileFolder;

        try (
            ServerSocket listenSocket = new ServerSocket(port);
            Socket controllerSocket = new Socket(InetAddress.getLocalHost(), port)
        ) {
            
        } catch (IOException e) {
            e.getStackTrace();
        }
    }

    
}
