import java.net.*;
import java.util.*;
import java.io.*;
import java.util.concurrent.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;


class Controller {
    private final int R;
    private final double timeout;
    private final double rebalancePeriod;
    private ConcurrentHashMap<Integer, DStoreRecord> dStores = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Integer> fileSizes = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<Integer>> fileLocations = new ConcurrentHashMap<>();

    private volatile Status indexStatus = Status.READY;
    private ConcurrentHashMap<String, AckRequest> storeAckMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, AckRequest> removeAckMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Integer, String[]> dStoreFilesMap = new ConcurrentHashMap<>();
    private final ErrorLogger errorLogger;

    private final Object modifyIndexLock = new Object();
    private HashSet<Integer> rebalanceAcks = new HashSet<>();

    /**
     * Status of the index.
     */
    public enum Status {
        READY, STORE_IN_PROGRESS, STORE_COMPLETED, REMOVE_IN_PROGRESS, REMOVE_COMPLETED
    }

    public static void main(String[] args) {
        try {
            ControllerLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL);
            new Controller(Integer.parseInt(args[0]), Integer.parseInt(args[1]),
                    Double.parseDouble(args[2]), Double.parseDouble(args[3]));
        } catch (NumberFormatException | IOException e) {
            e.printStackTrace();
        }
    }

    public Controller(int cport, int R, double timeout, double rebalancePeriod) {
        this.R = R;
        this.timeout = timeout;
        this.rebalancePeriod = rebalancePeriod;
        errorLogger = new ErrorLogger("controllerError.log");
        new Timer().scheduleAtFixedRate(new RebalanceTask(), 0, (long)rebalancePeriod);

        // Creates a new socket for listening to.
        try (ServerSocket ss = new ServerSocket(cport)) {
            for (;;) {
                Socket client = ss.accept();
                new Thread(new ConnectionThread(client, this)).start();
            }
        } catch (IOException e) {
            errorLogger.logError(e.getMessage());
        }
    }

    public int getR() {
        return R;
    }

    public double getTimeout() {
        return timeout;
    }

    public boolean hasJoined(int port, Socket dStoreSocket) {
        return dStores.containsKey(port) && dStores.get(port).getSocket().equals(dStoreSocket);
    }

    public void addDStore(int port, Socket dStoreSocket) {
        dStores.put(port, new DStoreRecord(dStoreSocket));
    }

    /**
     * Checks whether a file exists within the file system.
     * @param filename      name of the file queried.
     * @return true if it exists
     */
    public boolean fileExists(String filename) {
        return fileSizes.containsKey(filename);
    }

    public int getFileSize(String filename) { return fileSizes.get(filename); }

    public ConcurrentHashMap<Integer, DStoreRecord> getDStores() { return dStores; }

    public List<DStoreRecord> getDStoreRecord(List<Integer> ports) {
        return ports.stream()
                    .map(dStores::get)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
    }

    public DStoreRecord getDStoreRecord(Integer port) {
        return dStores.get(port);
    }


    /**
     * @param filename      name of file to be found in the system.
     * @return the ports of D stores that stores this file.
     */
    public List<Integer> getDStoresWithFile(String filename) { return fileLocations.get(filename); }

    public ErrorLogger getErrorLogger() {
        return errorLogger;
    }

    public synchronized void setIndexStatus(Status status) {
        indexStatus = status;
    }

    public synchronized boolean isReady() { return indexStatus.equals(Status.READY);}

    public void addStoreAckExpectation(String fileName, List<Integer> portsToReply) {
        storeAckMap.put(fileName, new AckRequest(portsToReply));
    }

    public void addRemoveAckExpectation(String fileName, List<Integer> portsToReply) {
        removeAckMap.put(fileName, new AckRequest(portsToReply));
    }

    public void recordStoreAck(String fileName, Integer port) {
        if (storeAckMap.containsKey(fileName)) {
            storeAckMap.get(fileName).recordAck(port);
        }
    }

    public void recordRemoveAck(String fileName, Integer port) {
        if (removeAckMap.containsKey(fileName)) {
            removeAckMap.get(fileName).recordAck(port);
        }
    }

    public boolean hasAllStoreAcksReceived(String fileName) {
        return storeAckMap.get(fileName).isFulfilled();
    }

    public boolean hasAllRemoveAcksReceived(String filename) {
        return removeAckMap.get(filename).isFulfilled();
    }

    public void deleteStoreAckExpectation(String filename) {
        storeAckMap.remove(filename);
    }

    public void deleteRemoveAckExpectation(String filename) {
        removeAckMap.remove(filename);
    }

    public synchronized Object getModifyIndexLock() {
        return modifyIndexLock;
    }

    /**
     * Records that a rebalance complete has been received from the port and notifies the rebalance method if
     * all REBALANCE_COMPLETE has been received.
     * @param port      of the D store that has sent the REBALANCE_COMPLETE message.
     */
    public void recordRebalanceAck(int port) {
        rebalanceAcks.add(port);
        if (rebalanceAcks.size() == dStores.size()) {
            synchronized (rebalanceAcks) {
                rebalanceAcks.notify();
            }
        }
    }

    public void newFile(String filename, int size, List<Integer> ports) {
        fileSizes.put(filename, size);
        fileLocations.put(filename, ports);
    }

    public void removeFile(String filename) {
        fileSizes.remove(filename);
        fileLocations.remove(filename);
    }

    /**
     * Returns a list of the files stored in the system.
     * @return the files.
     */
    public Set<String> getFiles() {
        return fileSizes.keySet();
    }

    public void addDStoreFileList(int port, String[] files) {
        dStoreFilesMap.put(port, files);
    }

    public void addRebalanceAck(int port) {
        rebalanceAcks.add(port);
    }

    /**
     * Port of the D store connection to be removed.
     */
    private void removeFailedDStores() {
        // Terminates all connections with failed DStores
        HashSet<Integer> failedDStores = new HashSet<>(dStores.keySet());
        failedDStores.removeAll(dStoreFilesMap.keySet());

        for (Integer port : failedDStores) {
            dStores.get(port).getWriter().close();
            dStores.remove(port);
        }
    }

    /**
     * Rebalances the D stores.
     */
    public synchronized void rebalance() {
        if (dStores.size() >= R) {
            synchronized (modifyIndexLock) {
                if (indexStatus.equals(Status.READY)) {
                    // Preps the dStore file map and file locations map.
                    dStoreFilesMap.clear();
                    fileLocations.clear();
                    rebalanceAcks.clear();

                    // Sends list to each of the D stores.
                    for (DStoreRecord record : dStores.values()) {
                        record.getWriter().println(Protocol.LIST_TOKEN);
                        record.getWriter().flush();
                        ControllerLogger.getInstance().messageSent(record.getSocket(), Protocol.LIST_TOKEN);
                    }

                    // Waits for file lists to be received from all D stores.
                    try {
                        CompletableFuture.supplyAsync(() -> {
                            while (dStoreFilesMap.size() != dStores.size()) ;
                            return true;
                        }).get((long)timeout, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                        e.printStackTrace();
                    }

                    removeFailedDStores();
                    updateFileLocations();
//
//                    if (dStores.size() >= R) {
//                        redistributeFiles();
//                    }
                }
            }
        }
    }


    /**
     * Updates information on where each file is stored.
     */
    private void updateFileLocations() {
        dStoreFilesMap.entrySet().forEach(entry -> addFileLocationForOneDStore(entry.getKey(), entry.getValue()));
    }


    /**
     * Adds a file location of one dstore to the file locations.
     * @param dStorePort        that stores this file.
     * @param files             to be stored.
     */
    private void addFileLocationForOneDStore(Integer dStorePort, String[] files) {
        Arrays.stream(files)
              .forEach(file -> {
                      if (fileLocations.containsKey(file)) {
                          fileLocations.get(file).add(dStorePort);
                      } else {
                          fileLocations.put(file, new ArrayList<>(dStorePort));
                      }
              });
    }

    /**
     * Redistributes the file over dStores.
     */
    private void redistributeFiles() {
        // To be modified with new assignments.
        HashMap<Integer, ArrayList<SendRequest>> pendingSends = new HashMap<>();
        HashMap<Integer, ArrayList<String>> pendingRemoves = new HashMap<>();


        // Next get the D stores that are overloaded and D stores with too few files stored.
        int N = dStores.size();
        int F = fileSizes.size();

        int minFiles = Math.floorDiv(R * F, N);
        int maxFiles = (R * F + (N - 1)) / N;
        Map<Integer, List<String>> overflowStores = filterByNumFiles(dStoreFilesMap, n -> n > maxFiles);
        Map<Integer, List<String>> underflowStores = filterByNumFiles(dStoreFilesMap, n -> n < minFiles);
        Map<Integer, List<String>> okFreeStores = filterByNumFiles(dStoreFilesMap, n -> n >= minFiles && n < maxFiles);
        Map<Integer, List<String>> okMaxStores = filterByNumFiles(dStoreFilesMap, n -> n == maxFiles);


        try {
            if (F > 0) {
                rebalanceLostFiles(pendingSends, underflowStores, okFreeStores, okMaxStores, minFiles, maxFiles);
                rebalanceOverFlowStores(pendingSends, pendingRemoves, underflowStores, overflowStores, okFreeStores,
                        okMaxStores, maxFiles, minFiles);
                rebalanceUnderFlowStores(pendingSends, pendingRemoves, underflowStores, okMaxStores, maxFiles, minFiles);
            }
            // A rebalance request is sent to each D store.
            for (Map.Entry<Integer, DStoreRecord> store : dStores.entrySet()) {
                String msg = createRebalanceMessage(store.getKey(), pendingSends, pendingRemoves);
                DStoreRecord record = store.getValue();
                record.getWriter().println(msg);
                record.getWriter().flush();
                ControllerLogger.getInstance().messageSent(record.getSocket(), msg);
            }

            synchronized (rebalanceAcks) {
                rebalanceAcks.wait((long) timeout);
            }
        } catch (NoPossibleReceiverException | NoPossibleDonatorException | InterruptedException e) {
            errorLogger.logError(e.getMessage());
            errorLogger.logError(e.getStackTrace().toString());
        }
    }

    /**
     * Generates the rebalance message that will be sent to a D store
     * @param dStorePort            where the message will be sent to
     * @param pendingSends          sends that need to be performed.
     * @param pendingRemoves        removes that need to be performed.
     * @return the rebalance message string that will be sent to the D store.
     */
    private String createRebalanceMessage(int dStorePort, HashMap<Integer, ArrayList<SendRequest>> pendingSends,
                                          HashMap<Integer, ArrayList<String>> pendingRemoves) {
        // Maps file names to ports that they need to be sent to.
        HashMap<String, HashSet<Integer>> fileToPortsMap = new HashMap<>();
        if (pendingSends.size() > 0) {
            pendingSends.get(dStorePort).stream()
                    .map(request -> request.fileToSend)
                    .distinct()
                    .forEach(fileToSend -> fileToPortsMap.put(fileToSend, new HashSet<>()));
            pendingSends.get(dStorePort).stream()
                    .forEach(request -> fileToPortsMap.get(request.fileToSend).add(request.recipient));
        }

        // Constructs the message.
        StringBuilder msg = new StringBuilder(Protocol.REBALANCE_TOKEN);
        msg.append(" ");
        msg.append(fileToPortsMap.size());
        msg.append(" ");
        for (Map.Entry<String, HashSet<Integer>> fileToPorts : fileToPortsMap.entrySet()) {
            msg.append(fileToPorts.getKey());
            msg.append(" ");
            msg.append(fileToPorts.getValue().stream()
                    .map(String::valueOf)
                    .collect(Collectors.joining(" ")));
            msg.append(" ");
        }
        msg.append(pendingRemoves.size() > 0 ? pendingRemoves.get(dStorePort).size() : 0);
        msg.append(" ");
        if (pendingRemoves.size() > 0) {
            msg.append(String.join(" ", pendingRemoves.get(dStorePort)));
        }
        return msg.toString();
    }

    /**
     * @param pendingSends          requests for D stores to send.
     * @param underFlowStores       stores that have less files than the minimum requirement.
     * @param okFreeStores          stores that have an acceptable number of files that aren't at max capacity.
     * @param minFiles              the minimum number of files that a D store must contain.
     * @throws NoPossibleReceiverException  if there are no possible stores that can store this.
     */
    private void rebalanceLostFiles(HashMap<Integer, ArrayList<SendRequest>> pendingSends,
                                    Map<Integer, List<String>> underFlowStores, Map<Integer, List<String>> okFreeStores,
                                    Map<Integer, List<String>> okMaxStores, int minFiles, int maxFiles)
                                    throws NoPossibleReceiverException {
        // Assign files that doesn't have enough replicas to D stores with free space.
        for (Map.Entry<String, Integer> fileInfo : getNotEnoughReplicaFiles().entrySet()) {
            ListIterator<Integer> senders = fileLocations.get(fileInfo.getKey()).listIterator();

            // Assigns file to receiver D store.
            for (int i = 0; i < fileInfo.getValue(); i++) {
                // Finds a sender.
                if (!senders.hasNext()) {
                    senders = fileLocations.get(fileInfo.getKey()).listIterator();
                }
                int sender = senders.next();
                int receiver = getReceiver(minFiles, maxFiles, underFlowStores, okFreeStores, okMaxStores,
                                           fileInfo.getKey());

                fileLocations.get(fileInfo.getKey()).add(receiver);

                // Then adds a send request.
                if (!pendingSends.containsKey(sender)) {
                    pendingSends.put(sender, new ArrayList<>());
                }
                pendingSends.get(sender).add(new SendRequest(fileInfo.getKey(), receiver));
            }
        }
    }


    /**
     * Assigns overflowing D store files to the rest.
     * @param pendingSends          send requests.
     * @param pendingRemoves        remove requests.
     * @param underFlowStores       stores that holds less than the minimum number of files.
     * @param overFlowStores        stores that holds more than the maximum number of files.
     * @param okFreeStores          stores that hold an acceptable number of files that aren't at max capacity.
     * @param maxFiles              that a store can hold.
     * @param minFiles              that a store can hold.
     */
    private void rebalanceOverFlowStores(HashMap<Integer, ArrayList<SendRequest>> pendingSends,
                                         HashMap<Integer, ArrayList<String>> pendingRemoves,
                                         Map<Integer, List<String>> underFlowStores,
                                         Map<Integer, List<String>> overFlowStores,
                                         Map<Integer, List<String>> okFreeStores,
                                         Map<Integer, List<String>> okMaxStores,
                                         int maxFiles, int minFiles) {
        // For each over flow D store, move as many files as possible.
        for (Map.Entry<Integer, List<String>> overflowedStore : overFlowStores.entrySet()) {
            int dStoreSize = overflowedStore.getValue().size();
            ListIterator<String> files = overflowedStore.getValue().listIterator();
            String file;
            // While the D store is still overflowing.
            while (dStoreSize > maxFiles) {
                file = files.next();
                int receiver = 0;
                try {
                    receiver = getReceiver(minFiles, maxFiles, underFlowStores, okFreeStores, okMaxStores, file);
                    fileLocations.get(file).add(receiver);
                    fileLocations.get(file).remove(overflowedStore);
                    // Adds send request
                    if (!pendingSends.containsKey(overflowedStore.getKey())) {
                        pendingSends.put(overflowedStore.getKey(), new ArrayList<>());
                    }
                    pendingSends.get(overflowedStore.getKey()).add(new SendRequest(file, receiver));
                    // Adds remove request
                    if (!pendingRemoves.containsKey(overflowedStore.getKey())) {
                        pendingRemoves.put(overflowedStore.getKey(), new ArrayList<>());
                    }
                    pendingRemoves.get(overflowedStore.getKey()).add(file);

                    dStoreSize--;
                } catch (NoPossibleReceiverException e) {
                    errorLogger.logError("No receivers available to store " + file);
                }
            }
        }
    }


    /**
     * Assigns files to underflowing D store files from the others.
     * @param pendingSends          send requests.
     * @param underFlowStores       stores that have less than the minimum required number of files.
     * @param okMaxStores           can donate one file to the underflow store.
     * @param maxFiles              that a store can have.
     * @param minFiles              that a store can have
     */
    private void rebalanceUnderFlowStores(HashMap<Integer, ArrayList<SendRequest>> pendingSends,
                                          HashMap<Integer, ArrayList<String>> pendingRemoves,
                                          Map<Integer, List<String>> underFlowStores,
                                          Map<Integer, List<String>> okMaxStores, int maxFiles, int minFiles)
                                          throws NoPossibleDonatorException {
        for (Map.Entry<Integer, List<String>> underFlowStore : underFlowStores.entrySet()) {
            int dStoreSize = underFlowStore.getValue().size();
            ArrayList currentFiles = new ArrayList(underFlowStore.getValue());

            while (dStoreSize < minFiles) {
                DonatorRecord donator = getDonator(minFiles, maxFiles, okMaxStores, currentFiles);
                fileLocations.get(donator.file).remove(donator.donator);
                fileLocations.get(donator.file).add(underFlowStore.getKey());

                if (!pendingSends.containsKey(donator.donator)) {
                    pendingSends.put(donator.donator, new ArrayList<>());
                }
                pendingSends.get(donator).add(new SendRequest(donator.file, underFlowStore.getKey()));
                // Adds remove request
                if (!pendingRemoves.containsKey(donator.donator)) {
                    pendingRemoves.put(donator.donator, new ArrayList<>());
                }
                pendingRemoves.get(donator.donator).add(donator.file);

                dStoreSize++;
            }
        }

    }



    /**
     * Filters a D store by the given predicate.
     * @param stores       to be filtered
     * @param filter       predicate on the number of files for a particular d store
     * @return newly filtered d store map.
     */
    private Map<Integer, List<String>> filterByNumFiles(Map<Integer, String[]> stores, Predicate<Integer> filter) {
        Map<Integer, List<String>> filteredStores = stores.entrySet().stream()
                .filter(e -> filter.test(e.getValue().length))
                .collect(Collectors.toMap(Map.Entry::getKey, e -> Arrays.asList(e.getValue())));

        return new HashMap<>(filteredStores);
    }



    /**
     * Get the ports of D stores storing this file.
     * @param stores        stores to be filtered.
     * @param filename      what d store should contain.
     * @return ports of d stores storing #filename.
     */
    private ListIterator<Integer> getPortsWithoutFile(Map<Integer, List<String>> stores, String filename) {
        return stores.entrySet().stream()
                .filter(e -> !e.getValue().contains(filename))
                .map(e -> e.getKey())
                .collect(Collectors.toList()).listIterator();
    }

    /**
     * @return files that haven't been replicated enough and the number of times they need replicating.
     */
    private Map<String, Integer> getNotEnoughReplicaFiles() {
        return new HashMap(fileLocations.entrySet().stream()
                .filter(e -> e.getValue().size() < R)
                .collect(Collectors.toMap(Map.Entry::getKey, e -> R - e.getValue().size())));
    }


    /**
     * Returns the port of a store that can store a given file.
     * @param minFiles              that a store can hold
     * @param maxFiles              that a store can hold
     * @param underFlowStores       holds less than the minimum number of files.
     * @param okFreeStores          holds an acceptable number of files but can accept more.
     * @param okMaxStores           at maximum capacity.
     * @param filename              name of the file to be stored.
     * @return if a dstore can be found that can do the following.
     * @throws NoPossibleReceiverException  if no dstores available that can store the file.
     */
    private int getReceiver(int minFiles, int maxFiles, Map<Integer, List<String>> underFlowStores,
                            Map<Integer, List<String>> okFreeStores, Map<Integer, List<String>> okMaxStores,
                            String filename) throws NoPossibleReceiverException{
        ListIterator<Integer> underFlowReceivers = getPortsWithoutFile(underFlowStores, filename);
        ListIterator<Integer> okFreeReceivers = getPortsWithoutFile(okFreeStores, filename);

        int receiver = 0;
        // Checks if there are underflow D stores left to store the file.
        if (underFlowReceivers.hasNext()) {
            receiver = underFlowReceivers.next();
            int newSize = underFlowStores.get(receiver).size() + 1;
            // Checks whether d stores are no longer underflow.
            if (newSize < minFiles) {
                underFlowStores.get(receiver).add(filename);
            } else {
                okFreeStores.put(receiver, underFlowStores.get(receiver));
                underFlowStores.remove(receiver);
            }
        } else if (okFreeReceivers.hasNext()){
            receiver = okFreeReceivers.next();
            int newSize = okFreeStores.get(receiver).size() + 1;
            // Checks whether d store is at maximum capacity.
            if (newSize == maxFiles) {
                okMaxStores.put(receiver, okFreeStores.put(receiver, okFreeStores.get(receiver)));
                okFreeStores.remove(receiver);
            }
        } else {
            throw new NoPossibleReceiverException("No possible receiver exception for " + filename);
        }

        return receiver;
    }


    /**
     * Chooses a D store to donate a file to the underflow D store.
     * @param minFiles          minimum number of files that should be stored.
     * @param maxFiles          maximum number of files that should be stored.
     * @param okMaxStores       D stores that are at maximum capacity and can donate one file.
     * @param existingFiles     files that the underflow D store already has.
     * @return
     */
    private DonatorRecord getDonator(int minFiles, int maxFiles, Map<Integer, List<String>> okMaxStores,
                          List<String> existingFiles) throws NoPossibleDonatorException {
        Integer donator = null;
        Iterator<Map.Entry<Integer, List<String>>> okMaxStoresIterator = okMaxStores.entrySet().iterator();
        // Loops through all possible donators.
        while (okMaxStoresIterator.hasNext()) {
            Map.Entry<Integer, List<String>> maxStore = okMaxStoresIterator.next();
            try {
                // Finds the first file that doesn't class with the underflow dStores other files.
                String newFile = maxStore.getValue().stream()
                                                    .dropWhile(file -> existingFiles.contains(file))
                                                    .findFirst()
                                                    .get();
                okMaxStores.remove(donator);
                return new DonatorRecord(donator, newFile);
            } catch (NullPointerException e) {
                errorLogger.logError(maxStore.getKey() + " store can't donate a file");
            }
        }
        throw new NoPossibleDonatorException("No possible donator can be found");
    }


    /**
     * Scheduled task that will periodically rebalance.
     */
    private class RebalanceTask extends TimerTask {

        /**
         * The action to be performed by this timer task.
         */
        @Override
        public void run() {
            rebalance();
        }
    }

    private class SendRequest {
        String fileToSend;
        int recipient;

        private SendRequest(String file, int dport) {
            fileToSend = file;
            recipient = dport;
        }
    }

    private class DonatorRecord {
        int donator;
        String file;

        public DonatorRecord(int donator, String file) {
            this.donator = donator;
            this.file = file;
        }
    }

    private class NoPossibleReceiverException extends Exception {
        public NoPossibleReceiverException(String msg) {
            super(msg);
        }
    }

    private class NoPossibleDonatorException extends Exception {
        public NoPossibleDonatorException(String msg) {super(msg);}
    }

    private class ConnectionThread implements Runnable {
        private final Socket client;
        private final Controller con;
        private int port;
        private boolean isDStore = false;

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
                                if (!isDStore) {
                                    list(client);
                                } else {
                                    parseDList(Arrays.copyOfRange(words, 1, words.length));
                                }
                            }
                            case Protocol.STORE_TOKEN -> store(words[1], Integer.parseInt(words[2]), client);
                            case Protocol.STORE_ACK_TOKEN -> con.recordStoreAck(words[1], port);
                            case Protocol.REMOVE_ACK_TOKEN -> con.recordRemoveAck(words[1], port);
                            case Protocol.REBALANCE_COMPLETE_TOKEN -> con.recordRebalanceAck(port);
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
            isDStore = true;
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
                out.flush();
                ControllerLogger.getInstance().messageSent(dStoreSocket, Protocol.JOINED_COMPLETE_TOKEN);

//            con.rebalance();
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
            } catch (NoSuchElementException e) {
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


    /**
     * Represents an ack request to be fulfilled.
     */
    public class AckRequest {
        private Set<Integer> receivedAcks;
        private final Set<Integer> expectedPorts;
        private volatile boolean isFulfilled;
        private final Object lock = new Object();

        public AckRequest(List<Integer> expectedPorts) {
            this.expectedPorts = new HashSet<>(expectedPorts);
            this.receivedAcks = new HashSet<>();
        }


        public void recordAck(Integer port) {
            synchronized(lock) {
                if (expectedPorts.contains(port)) {
                    receivedAcks.add(port);
                }
                isFulfilled = receivedAcks.equals(expectedPorts);
            }
        }

        public boolean isFulfilled() {
            return isFulfilled;
        }
    }

}

