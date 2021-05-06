import java.io.*;
import java.net.*;

public class DStore {
    private Integer port, cport;
    private int timeout;
    private String fileFolder;
    private BufferedReader fromControl;
    private Socket conSocket;
    private PrintWriter toControl;

    private ErrorLogger errorLogger = new ErrorLogger("dstoreError.log");

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
        new File(fileFolder).mkdirs();
        this.port = port;
        this.cport = cport;

        try {
            DstoreLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL, port);
            ServerSocket listenSocket = new ServerSocket(port);
            conSocket = new Socket(InetAddress.getLocalHost(), cport);

            // Attempts to join the system.
            joinSystem(conSocket);
            listen(listenSocket, conSocket);

            listenSocket.close();
            conSocket.close();
        } catch (IOException e) {
            e.getStackTrace();
        }
    }

    private void listen(ServerSocket listenSocket, Socket conSocket) {
        for (;;) {
            try {
                Socket client = listenSocket.accept();
                BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream()));

                String message = reader.readLine();
                String[] words = message.split(" ");
                DstoreLogger.getInstance().messageReceived(client, message);
                switch (words[0]) {
                    case Protocol.STORE_TOKEN:
                        store(words[1], Integer.parseInt(words[2]), client);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Continuously sends request to join a system. Creates the buffered reader and print writer as well.
     * @param conSocket     socket that will be used to connect to the server.
     * @return The socket to the system used to successfully joined.
     */
    private void joinSystem(Socket conSocket) {
        try {
            toControl = Helper.makeWriter(conSocket);
            fromControl = Helper.makeReader(conSocket);
            toControl.println(Protocol.JOIN_TOKEN + " " + port);
            toControl.flush();

            // Waits for acknowledgment
//            conSocket.setSoTimeout(timeout);
            String line;
            while ((line = fromControl.readLine()) == null || !line.equals(Protocol.JOINED_COMPLETE_TOKEN));
            if (line.equals(Protocol.JOINED_COMPLETE_TOKEN)) {
                DstoreLogger.getInstance().messageReceived(conSocket, line);
            }
        } catch (SocketTimeoutException e) {
            System.out.println("Resending join request");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Stores a file given by the client.
     * @param filename      name of the file to be stored
     * @param fileSize      size of the file to be stored
     * @param client        socket used to communicate with the client.
     */
    private void store(String filename, int fileSize, Socket client) throws IOException {
        FileOutputStream fileStream = null;
        try {
            File newFile = new File(fileFolder + File.separator + filename);
            newFile.createNewFile();
            fileStream = new FileOutputStream(newFile, false);
            // Sends back acknowledgment.
            PrintWriter toClient = Helper.makeWriter(client);
            toClient.println(Protocol.ACK_TOKEN);
            toClient.flush();
            DstoreLogger.getInstance().messageSent(client, Protocol.ACK_TOKEN);

            // Receivesdata from the client.
            InputStream inputStream = client.getInputStream();
            client.setSoTimeout(timeout);
            byte[] data = inputStream.readNBytes(fileSize);

            // Writes received data to the user.
            fileStream.write(data);

            // Send back the acknowledgment.
            toControl.println(Protocol.STORE_ACK_TOKEN);
            toControl.flush();
            DstoreLogger.getInstance().messageSent(conSocket, Protocol.STORE_ACK_TOKEN);
        } catch(SocketTimeoutException e) {
            errorLogger.logError("Timeout!");
        } catch (SocketException e) {
            errorLogger.logError(e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
            errorLogger.logError(e.getMessage());
        } finally {
            if (fileStream != null) {
                fileStream.close();
            }
        }
    }
}
