import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

class ConnectionThread implements Runnable {
    private final ServerSocket ss;
    private final Socket client;
    private final Controller con;
    private int port;

    private LoadRequest loadRequest = new LoadRequest();

    public ConnectionThread(ServerSocket ss, Socket client, Controller control) {
        this.ss = ss;
        this.client = client;
        this.con = control;
    }

    /**
     * Processes messages of one client.
     */
    public void run() {
        String line = "";

        try {
            BufferedReader in = Helper.makeReader(client);
            while ((line = in.readLine()) != null) {
                ControllerLogger.getInstance().messageReceived(client, line);

                String[] words = line.split(" ");
                if (words[0].equalsIgnoreCase(Protocol.JOIN_TOKEN)) {
                    join(Integer.parseInt(words[1]), client);
                } else if (con.getDStores().size() >= con.getR()) {
                    // Else client's request is served.
                    switch (words[0].toUpperCase()) {
                        case Protocol.LIST_TOKEN -> list(client);
                        case Protocol.STORE_TOKEN -> store(words[1], Integer.parseInt(words[2]), client);
                        case Protocol.STORE_ACK_TOKEN -> con.recordStoreAck(words[1], port);
                        case Protocol.LOAD_TOKEN -> load(words[1], client);
                        case Protocol.RELOAD_TOKEN -> reload(words[1], client);
                        default -> con.getErrorLogger().logError(Protocol.MALFORMED_ERROR + " " + line);
                    }
                } else {
                    // Not enough D stores so controller block requests.
                    PrintWriter writer = Helper.makeWriter(client);
                    writer.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    writer.flush();
                    ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                }
            }
        } catch (IOException e) {
            con.getErrorLogger().logError(e.getMessage());
        } catch (IndexOutOfBoundsException e) {
            con.getErrorLogger().logError(Protocol.MALFORMED_ERROR + " " + line);
        } catch (DStoreAlreadyExistsException e) {
            con.getErrorLogger().logError("DStore " + port + " already exists");
        }
    }


    /**
     * Adds a dstore to the possible D stores.
     * @param port              of the dstore
     * @param dStoreSocket      used to communicate with the dstore
     */
    private void join(Integer port, Socket dStoreSocket) throws DStoreAlreadyExistsException {
        HashMap<Integer, Socket> dStores = con.getDStores();
        boolean isNew = !dStores.containsKey(port) || !dStores.get(port).equals(dStoreSocket);
        this.port = port;
        // If dStore has not already joined add and logs it.
        try {
            if (isNew) {
                ControllerLogger.getInstance().dstoreJoined(dStoreSocket, port);
                dStores.put(port, dStoreSocket);
            }

            PrintWriter out = Helper.makeWriter(dStores.get(port));
            out.println(Protocol.JOINED_COMPLETE_TOKEN);
            ControllerLogger.getInstance().messageSent(dStoreSocket, Protocol.JOINED_COMPLETE_TOKEN);
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (!isNew) {
            throw new DStoreAlreadyExistsException("DStore already exists");
        }
    }

    /**
     * Returns the ports that the files can be stored on to the client.
     *
     * @param filename name of the file to be stored.
     * @param size     how big the file will be.
     */
    private void store(String filename, int size, Socket client) throws IOException {
        PrintWriter toClient = Helper.makeWriter(client);

        if (con.fileExists(filename)) {
            toClient.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
            toClient.flush();
            ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
        } else {
            synchronized (this) {
                while (!con.isReady());
                con.setIndexStatus(Controller.Status.STORE_IN_PROGRESS);

                // Gets R random DStores.
                List<Integer> selectedStores = new ArrayList<>(con.getDStores().keySet());
                Collections.shuffle(selectedStores);
                selectedStores = selectedStores.subList(0, con.getR());
                con.addStoreAckExpectation(filename);

                String selectedString = Protocol.STORE_TO_TOKEN + " " + selectedStores.stream()
                        .map(Object::toString)
                        .collect(Collectors.joining(" "));
                toClient.println(selectedString);
                toClient.flush();
                ControllerLogger.getInstance().messageSent(client, selectedString);

                // Waits for all acknowledgements from the dStores, else it times out.
                try {
                    // Waits for all of the acknowledgments to be received.
                    long conTimeout = (long)con.getTimeout();
                    ArrayList<Integer> finalSelectedStores = new ArrayList<>(selectedStores);
                    CompletableFuture.supplyAsync(() -> {
                        while (!con.getAckReceived(filename).equals(finalSelectedStores)) ;
                        return true;
                    }).get(conTimeout, TimeUnit.MILLISECONDS);

                    // Store and set store complete.
                    con.newFile(filename, size, selectedStores);
                    toClient.println(Protocol.STORE_COMPLETE_TOKEN);
                    con.setIndexStatus(Controller.Status.STORE_COMPLETED);
                    con.removeStoreAckExpectation(filename);
                    toClient.flush();
                    ControllerLogger.getInstance().messageSent(client, Protocol.STORE_COMPLETE_TOKEN);
                } catch (TimeoutException e) {
                    System.out.println("Time out has occurred");
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                con.setIndexStatus(Controller.Status.READY);
            }
        }
    }

    /**
     * Sends the port where the client can obtain a file.
     * @param filename      name of the file requested.
     * @param client        used to communicate with the client.
     */
    public void load(String filename, Socket client) {
        try {
            PrintWriter writer = Helper.makeWriter(client);
            if (!con.fileExists(filename)) {
                writer.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            } else if (!loadRequest.isNewFile(filename)){
                List<Integer> availableStores = con.getDStoresWithFile(filename);
                // Randomises order in which client will try out D stores.
                Collections.shuffle(availableStores);
                loadRequest.newRequest(filename, new PriorityQueue<>(availableStores));
                writer.println(Protocol.LOAD_FROM_TOKEN + " "
                               + loadRequest.nextDStore() + " "
                               + con.getFileSize(filename));
            }
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void reload(String filename, Socket client) {
        try {
            PrintWriter toClient = Helper.makeWriter(client);
            if (loadRequest.isEmptyDStores()) {
                toClient.println(Protocol.ERROR_LOAD_TOKEN);
            } else {
                toClient.println(Protocol.LOAD_FROM_TOKEN + " " + con.getFileSize(filename));
            }
            toClient.flush();
        } catch (IOException e) {
            con.getErrorLogger().logError(e.getMessage());
        }
    }

    /**
     * Sends a list of files to the client.
     * @param client        requesting the list of files.
     */
    public void list(Socket client) {
        while (!con.isReady());
        PrintWriter writer;
        try {
            writer = Helper.makeWriter(client);
            String files = String.join(" ", con.getFiles());
            writer.println(Protocol.LIST_TOKEN + " " + files);
            writer.flush();
            ControllerLogger.getInstance().messageSent(client, files);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static class DStoreAlreadyExistsException extends Exception {
        public DStoreAlreadyExistsException(String errorMessage) {
            super(errorMessage);
        }
    }

    /**
     * Keeps track of the D stores left to try.
     */
    private class LoadRequest {
        private String filename = "";
        private PriorityQueue<Integer> dStoresLeft = new PriorityQueue<>();

        /**
         * @param filename      that the client is currently requesting.
         * @param dStores       d stores left to try.
         */
        public void newRequest(String filename, PriorityQueue<Integer> dStores) {
            this.filename = filename;
            this.dStoresLeft = dStores;
        }

        /**
         * @return next D store left to try
         */
        public int nextDStore() {
            return dStoresLeft.remove();
        }

        /**
         * @param filename
         * @return true if the filename is the same as the current one being requested.
         */
        public boolean isNewFile(String filename) {
            return !filename.equals(this.filename);
        }

        /**
         * @return true if no more D stores left.
         */
        public boolean isEmptyDStores() {
            return dStoresLeft.isEmpty();
        }
    }
}

