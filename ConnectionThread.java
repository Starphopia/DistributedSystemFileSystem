import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

class ConnectionThread implements Runnable {
    private ServerSocket ss;
    private Socket client;
    private Controller con;

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
                if (words[0].toUpperCase().equals(Protocol.JOIN_TOKEN)) {
                    join(Integer.parseInt(words[1]), client);
                } else if (con.getDStores().size() >= con.getR()) {
                    // Else client's request is served.
                    switch (words[0].toUpperCase()) {
                        case Protocol.LIST_TOKEN:
                            list(client);
                            break;

                        case Protocol.STORE_TOKEN:
                            store(words[1], Integer.parseInt(words[2]), client);
                            break;

                        case Protocol.STORE_ACK_TOKEN:
                            con.recordStoreAck(words[1], client.getPort());
                            break;

                        default:
                            con.getErrorLogger().logError(Protocol.MALFORMED_ERROR + " " + line);
                    }
                } else {
                    // Not enough dstores so controller block requests.
                    PrintWriter writer = Helper.makeWriter(client);
                    writer.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    writer.flush();
                    ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (IndexOutOfBoundsException e) {
            con.getErrorLogger().logError(Protocol.MALFORMED_ERROR + " " + line);
        }
    }


    /**
     * Adds a dstore to the possible dstores.
     *
     * @param port
     * @param dStoreSocket
     */
    private void join(Integer port, Socket dStoreSocket) {
        HashMap<Integer, DStoreRecord> dStores = con.getDStores();
        // If dStore has not already joined add and logs it.
        try {
            if (!dStores.containsKey(port) || !dStores.get(port).equals(dStoreSocket)) {
                ControllerLogger.getInstance().dstoreJoined(dStoreSocket, port);
                dStores.put(port, new DStoreRecord(dStoreSocket));
            }

            PrintWriter out = dStores.get(port).getWriter();
            out.println(Protocol.JOINED_COMPLETE_TOKEN);
            ControllerLogger.getInstance().messageSent(dStoreSocket, Protocol.JOINED_COMPLETE_TOKEN);
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
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
            con.setIndexStatus(Controller.Status.STORE_IN_PROGRESS);

            // Gets R random DStores.
            List<Integer> selectedStores = new ArrayList<>(con.getDStores().keySet());
            Collections.shuffle(selectedStores);
            selectedStores = selectedStores.subList(0, con.getR());

            String selectedString = Protocol.STORE_TO_TOKEN + " " + selectedStores.stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(" "));
            toClient.println(selectedString);
            toClient.flush();
            ControllerLogger.getInstance().messageSent(client, selectedString);

            // Waits for all acknowledgements from the dStores, else it times out.
            try {
                // Waits for all of the acknowledgments to be received.
                List<Integer> finalSelectedStores = selectedStores;
                CompletableFuture.supplyAsync(() -> {
                    while(!con.getAckReceieved(filename).equals(finalSelectedStores));
                    return null;
                }).get((long)con.getTimeout(), TimeUnit.MILLISECONDS);

                // Store and set store complete.
                con.newFile(filename, size, selectedStores);
                con.setIndexStatus(Controller.Status.STORE_COMPLETED);
                toClient.println(Protocol.STORE_COMPLETE_TOKEN);
                toClient.flush();
                ControllerLogger.getInstance().messageSent(client, Protocol.STORE_COMPLETE_TOKEN);
            } catch (TimeoutException e) {
                System.out.println("Time out has occurred");
            } catch (InterruptedException | ExecutionException e) {
                con.getErrorLogger().logError(e.getMessage());
            }
        }
    }

    public void list(Socket client) throws IOException {
        PrintWriter writer = Helper.makeWriter(client);
        String files = con.getFiles().stream().collect(Collectors.joining(" "));
        writer.println(files);
        writer.flush();
        ControllerLogger.getInstance().messageSent(client, files);
    }


}

