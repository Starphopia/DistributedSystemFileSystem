import java.io.*;
import java.net.*;

public class DStore {
    private Integer port, cport;
    private int timeout;
    private String fileFolder;

    public static void main(String[] args) {
        try {
            new DStore(Integer.parseInt(args[0]), Integer.parseInt(args[1]), 
                       Integer.parseInt(args[2]), args[3]);
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
    public DStore(int port, int cport, int timeout, String fileFolder) {
        this.timeout = timeout;
        this.fileFolder = fileFolder;
        this.port = port;
        this.cport = cport;

        try (
            ServerSocket listenSocket = new ServerSocket(port);
            Socket conSocket = new Socket(InetAddress.getLocalHost(), cport);
        ) {
            // Attempts to join the system.
            joinSystem(conSocket);
            listen(listenSocket, conSocket);

        } catch (IOException e) {
            e.getStackTrace();
        }
    }

    private void listen(ServerSocket listenSocket, Socket conSocket) {
        System.out.println("listensocket " + listenSocket.isClosed());
        System.out.println("conSocket " + conSocket.isClosed());
        while (true) {
            System.out.println("listensocket " + listenSocket.isClosed());
            System.out.println("conSocket " + conSocket.isClosed());
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Continuously sends request to join a system.
     * @param conSocket     socket that will be used to connect to the server.
     * @return The socket to the system used to successfully joined.
     */
    private void joinSystem(Socket conSocket) {
        System.out.println("JOining system");
        try {
            PrintWriter toControl = new PrintWriter(new OutputStreamWriter(conSocket.getOutputStream()));
            BufferedReader in = new BufferedReader(new InputStreamReader(conSocket.getInputStream()));
            toControl.println("JOIN " + port);
            toControl.flush();
            // Waits for acknowledgment
//            conSocket.setSoTimeout(timeout);
            String line;
            while ((line = in.readLine()) == null || !line.equals(StatusMessages.JOINED_SUCCESS));
            if (line.equals(StatusMessages.JOINED_SUCCESS)) {
                System.out.println("Joined successful");
            }
            System.out.println(line);

        } catch (SocketTimeoutException e) {
            System.out.println("Resending join request");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }




    
}
