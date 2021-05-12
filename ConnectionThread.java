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
    private final Socket client;
    private final Controller con;
    private int port;

    private LoadRequest loadRequest = new LoadRequest();

    public ConnectionThread(Socket client, Controller control) {
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
                        case Protocol.LIST_TOKEN -> {
                            if (words.length == 1) {
                                list(client);
                            } else {
                                parseDList(Arrays.copyOfRange(words, 1, words.length));
                            }
                        }
                        case Protocol.STORE_TOKEN -> store(words[1], Integer.parseInt(words[2]), client);
                        case Protocol.STORE_ACK_TOKEN -> con.recordStoreAck(words[1], port);
                        case Protocol.REMOVE_ACK_TOKEN -> con.recordRemoveAck(words[1], port);
                        case Protocol.LOAD_TOKEN -> load(words[1], client);
                        case Protocol.RELOAD_TOKEN -> reload(words[1], client);
                        case Protocol.REMOVE_TOKEN -> remove(words[1], client);
                        case Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN -> con.getErrorLogger().logError("File already exists");
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
        }
    }

    /**
     * Records the file list of a particular dStore.
     * @param filenames
     */
    private void parseDList(String[] filenames) {
        con.addDStoreFileList(port, filenames);
    }


    /**
     * Adds a dstore to the possible D stores.
     * @param port              of the dstore
     * @param dStoreSocket      used to communicate with the dstore
     */
    private void join(Integer port, Socket dStoreSocket) {
        this.port = port;
        // If dStore has not already joined add and logs it.
        try {
            if (!con.hasJoined(port, dStoreSocket)) {
                ControllerLogger.getInstance().dstoreJoined(dStoreSocket, port);
                con.addDStore(port, dStoreSocket);
            } else {
                con.getErrorLogger().logError("DStore " + port + " already exists");
            }

            PrintWriter out = con.getDStoreRecord(port).getWriter();
            out.println(Protocol.JOINED_COMPLETE_TOKEN);
            ControllerLogger.getInstance().messageSent(dStoreSocket, Protocol.JOINED_COMPLETE_TOKEN);
            out.flush();
        } catch (Exception e) {
            con.getErrorLogger().logError(e.getMessage());
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
        synchronized (con.getModifyIndexLock()) {
            while(!con.isReady());
            if (con.fileExists(filename)) {
                toClient.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                toClient.flush();
                ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
            } else {
                con.setIndexStatus(Controller.Status.STORE_IN_PROGRESS);

                // Gets R random DStores.
                List<Integer> selectedStores = new ArrayList<>(con.getDStores().keySet());
                Collections.shuffle(selectedStores);
                selectedStores = selectedStores.subList(0, con.getR());
                con.addStoreAckExpectation(filename, selectedStores);

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
                    CompletableFuture.supplyAsync(() -> {
                        while (!con.hasAllStoreAcksReceived(filename)) ;
                        return true;
                    }).get(conTimeout, TimeUnit.MILLISECONDS);

                    // Store and set store complete.
                    con.newFile(filename, size, selectedStores);
                    toClient.println(Protocol.STORE_COMPLETE_TOKEN);
                    con.setIndexStatus(Controller.Status.STORE_COMPLETED);
                    toClient.flush();
                    ControllerLogger.getInstance().messageSent(client, Protocol.STORE_COMPLETE_TOKEN);
                } catch (TimeoutException | InterruptedException | ExecutionException e) {
                    con.getErrorLogger().logError(e.getMessage());
                } finally {
                    con.deleteStoreAckExpectation(filename);
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
    private void load(String filename, Socket client) {
        try {
            synchronized(con.getModifyIndexLock()) {
                PrintWriter writer = Helper.makeWriter(client);
                if (!con.fileExists(filename)) {
                    writer.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    writer.flush();
                } else if (loadRequest.isNewFile(filename)) {
                    List<Integer> availableStores = con.getDStoresWithFile(filename);
                    // Randomises order in which client will try out D stores.
                    Collections.shuffle(availableStores);
                    loadRequest.newRequest(filename, new PriorityQueue<>(availableStores));
                    String msg = Protocol.LOAD_FROM_TOKEN + " " + loadRequest.nextDStore() + " "
                            + con.getFileSize(filename);
                    writer.println(msg);
                    ControllerLogger.getInstance().messageSent(client, msg);
                    writer.flush();
                }
            }
        } catch (IOException e) {
            con.getErrorLogger().logError(e.getMessage());
        }
    }

    private void reload(String filename, Socket client) {
        try {
            synchronized(con.getModifyIndexLock()) {
                PrintWriter toClient = Helper.makeWriter(client);
                String msg = loadRequest.isEmptyDStores() ? Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN
                        : Protocol.LOAD_FROM_TOKEN + " " + loadRequest.nextDStore() + " " + con.getFileSize(filename);
                toClient.println(msg);
                ControllerLogger.getInstance().messageSent(client, msg);
                toClient.flush();
            }
        } catch (IOException e) {
            con.getErrorLogger().logError(e.getMessage());
        }
    }

    /**
     * Removes a selected file from the D store.
     * @param filename      to be removed
     * @param client        used to communicate with the client whom the removal is for.
     */
    private void remove(String filename, Socket client) {
        try {
            PrintWriter toClient = Helper.makeWriter(client);

            synchronized(con.getModifyIndexLock()) {
                while(!con.isReady());
                // If the file does not exist in the index notify the user, else proceed.
                if (!con.fileExists(filename)) {
                    toClient.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                } else {
                    con.setIndexStatus(Controller.Status.REMOVE_IN_PROGRESS);

                    long conTimeout = (long)con.getTimeout();
                    List<Integer> storesToUpdate = con.getDStoresWithFile(filename);

                    // To keep records of acknowledgments.
                    con.addRemoveAckExpectation(filename, storesToUpdate);
                    String msg = Protocol.REMOVE_TOKEN + " " + filename;
                    // For filename
                    for (DStoreRecord dStore : con.getDStoreRecord(storesToUpdate)) {
                        dStore.getWriter().println(msg);
                        dStore.getWriter().flush();
                        ControllerLogger.getInstance().messageSent(dStore.getSocket(), msg);
                    }

                    CompletableFuture.supplyAsync(() -> {
                        while (con.hasAllRemoveAcksReceived(filename));
                        return true;
                    }).get(conTimeout, TimeUnit.MILLISECONDS);

                    con.removeFile(filename);
                    con.deleteRemoveAckExpectation(filename);
                    toClient.println(Protocol.REMOVE_COMPLETE_TOKEN);
                    toClient.flush();
                    ControllerLogger.getInstance().messageSent(client, Protocol.REMOVE_COMPLETE_TOKEN);
                    con.setIndexStatus(Controller.Status.REMOVE_COMPLETED);
                    con.setIndexStatus(Controller.Status.READY);
                }

            }

        } catch (IOException | ExecutionException | InterruptedException | TimeoutException e) {
            e.printStackTrace();
            con.getErrorLogger().logError(e.getMessage());
            con.setIndexStatus(Controller.Status.READY);
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
            con.getErrorLogger().logError(e.getMessage());
        }
    }

    /**
     * Keeps track of the D stores left to try.
     */
    private static class LoadRequest {
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
         * @param filename      the name of the file to be checked against the current file.
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

