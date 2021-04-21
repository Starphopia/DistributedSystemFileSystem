import java.net.*;
import java.util.HashMap;
import java.io.*;

class Controller {
    private int R;
    private double timeout, rebalancePeriod;
    private HashMap<Socket, Integer> dStores = new HashMap<Socket, Integer>();
    private HashMap<String, Double> fileSizes = new HashMap<>();
    private HashMap<String, DStore> fileDStores = new HashMap<>();

    /**
     * Status of the index.
     */
    public enum Status {
        STORE_IN_PROGRESS, STORE_COMPLETED, REMOVE_IN_PROGRESS, REMOVE_COMPLETED
    }

    public static void main(String[] args) {
        try {
            new Controller(Integer.parseInt(args[0]), Integer.parseInt(args[1]),
                           Double.parseDouble(args[2]), Double.parseDouble(args[3]));
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
    }

    public Controller(int cport, int R, double timeout, double rebalancePeriod) {
        this.R = R;
        this.timeout = timeout;
        this.rebalancePeriod = rebalancePeriod;    
        
        // Creates a new socket for listening to.
        try (ServerSocket ss = new ServerSocket(cport)) {
            for (;;) {
                try (Socket client = ss.accept()) {
                    acceptConnection(ss, client);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Proccesses messages of one client.
     * @param ss        server socket used to listen on.
     * @param client    client's socket.
     */
    private  void acceptConnection(ServerSocket ss, Socket client) {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                System.out.println(line + "received");
                String[] words = line.split(" ");

                switch (words[0].toUpperCase()) {
                    case "JOIN":
                        join(Integer.parseInt(words[1]), client);
                    case "STORE":
                        store(words[1], client);
                }
            }
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    /**
     * Adds a dstore to the possible dstores.
     * @param port
     * @param dStoreSocket
     */
    private void join(Integer port, Socket dStoreSocket) {
        // If dStore has not already joined add.
        if (!dStores.containsKey(dStoreSocket)) {
            dStores.put(dStoreSocket, port);
        }

        PrintWriter out = NEW
        try (
            PrintWriter out = new PrintWriter(
                new OutputStreamWriter(dStoreSocket.getOutputStream()))
        ) {
            System.out.println("DStore joined");
            out.println(StatusMessages.JOINED_SUCCESS);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * Returns the ports that the files can be stored on to the client.
     * @param fileName
     * @param client
     */
    private void store(String fileName, Socket client) {

    }

    public void getDStoreList() {
        
    }
}