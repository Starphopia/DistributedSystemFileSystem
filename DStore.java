import java.io.*;
import java.net.*;
import java.util.Arrays;
import java.util.stream.Collectors;

public class DStore {
    private final Integer port;
    private final int timeout;
    private final String fileFolder;
    private BufferedReader fromControl;
    private Socket conSocket;
    private PrintWriter toControl;

    private final ErrorLogger errorLogger = new ErrorLogger("dstoreError.log");

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

        try {
            DstoreLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL, port);
            ServerSocket listenSocket = new ServerSocket(port);
            conSocket = new Socket(InetAddress.getLocalHost(), cport);

            // Attempts to join the system.
            joinSystem(conSocket);
            // Starts the thread that listens to the controller.
            new Thread(new DStoreListenToControlThread()).start();
            // Starts listening to the client.
            listen(listenSocket);

            listenSocket.close();
            conSocket.close();
        } catch (IOException e) {
            e.getStackTrace();
        }
    }

    private void listen(ServerSocket listenSocket) {
        for (;;) {
            try {
                Socket client = listenSocket.accept();
                BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream()));

                String message = reader.readLine();
                String[] words = message.split(" ");
                DstoreLogger.getInstance().messageReceived(client, message);
                switch (words[0]) {
                    case Protocol.STORE_TOKEN -> store(words[1], Integer.parseInt(words[2]), client);
                    case Protocol.LOAD_DATA_TOKEN -> load(words[1], client);
                    default -> errorLogger.logError(Protocol.MALFORMED_ERROR);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Continuously sends request to join a system. Creates the buffered reader and print writer as well.
     * @param conSocket     socket that will be used to connect to the server.
     */
    private void joinSystem(Socket conSocket) {
        boolean hasJoined = false;
        while (!hasJoined) {
            try {
                toControl = Helper.makeWriter(conSocket);
                fromControl = Helper.makeReader(conSocket);
                toControl.println(Protocol.JOIN_TOKEN + " " + port);
                toControl.flush();

                // Waits for acknowledgment
                conSocket.setSoTimeout(timeout);
                String line;
                while ((line = fromControl.readLine()) == null || !line.equals(Protocol.JOINED_COMPLETE_TOKEN)) ;
                DstoreLogger.getInstance().messageReceived(conSocket, line);
                conSocket.setSoTimeout(0);
                hasJoined = true;
            } catch (SocketTimeoutException e) {
                errorLogger.logError("Resending join request");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
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

            // Receives data from the client.
            InputStream inputStream = client.getInputStream();
            client.setSoTimeout(timeout);
            byte[] data = inputStream.readNBytes(fileSize);

            // Writes received data to the user.
            fileStream.write(data);
            fileStream.flush();
            fileStream.close();

            // Send back the acknowledgment.
            String msg = Protocol.STORE_ACK_TOKEN + " " + filename;
            toControl.println(msg);
            toControl.flush();
            DstoreLogger.getInstance().messageSent(conSocket, msg);
        } catch(SocketTimeoutException e) {
            errorLogger.logError("Timeout!");
        } catch(SocketException e) {
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


    /**
     * Returns the data requested by the client.
     * @param filename          name of the file to be retrieved
     * @param client            the socket used to communciate with teh client
     * @throws IOException if a problem occurs.
     */
    private void load(String filename, Socket client) throws IOException {
        File file = new File(fileFolder + File.separator + filename);

        // Serves up file if it exists else closes the connection.
        if (!file.exists()) {
            client.close();
        } else {
            InputStream fileStream = new FileInputStream(file);
            byte[] data = fileStream.readAllBytes();
            fileStream.close();

            OutputStream outputStream = client.getOutputStream();
            outputStream.write(data);
            outputStream.flush();
        }
    }

    private class DStoreListenToControlThread implements Runnable {

        @Override
        public void run() {
            String line;
            for (;;) {
                try {
                    line = fromControl.readLine();
                    DstoreLogger.getInstance().messageReceived(conSocket, line);

                    String[] words = line.split(" ");
                    switch (words[0].toUpperCase()) {
                        case Protocol.REMOVE_TOKEN -> remove(words[1]);
                        case Protocol.LIST_TOKEN -> list();
                        default -> errorLogger.logError("Malfunctioned message: " + line);
                    }
                } catch (IOException e) {
                    errorLogger.logError(e.getMessage());
                }
            }
        }

        /**
         * Removes a file from the index and returns the acknowledgement.
         * @param filename          name of the file to be removed.
         */
        private void remove(String filename) {
            File file = new File(fileFolder + File.separator + filename);

            // Serves up file if it exists else closes the connection.
            if (!file.exists()) {
                toControl.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                DstoreLogger.getInstance().messageSent(conSocket, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            } else {
                String msg = Protocol.REMOVE_ACK_TOKEN + " " + filename;
                file.delete();
                toControl.println(msg);
                DstoreLogger.getInstance().messageSent(conSocket, msg);
            }
            toControl.flush();
        }

        /**
         * Sends a list of files stored in the directory to the controller.
         */
        private void list() {
            File directory = new File(fileFolder);
            String msg = Arrays.stream(directory.listFiles())
                               .filter(File::isFile)
                               .map(File::getName)
                               .collect(Collectors.joining(" "));
            msg = Protocol.LIST_TOKEN + " " + msg;
            toControl.println(msg);
            toControl.flush();
            DstoreLogger.getInstance().log(msg);
        }
    }
}
