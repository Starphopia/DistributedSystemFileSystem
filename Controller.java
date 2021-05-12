import java.net.*;
import java.util.*;
import java.io.*;
import java.util.concurrent.*;
import java.util.function.IntBinaryOperator;
import java.util.function.IntUnaryOperator;
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
    private final Object dStoreListWait = new Object();

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

        // If all the dstores answered.
        if (dStoreFilesMap.size() == dStores.size()) {
            dStoreListWait.notify();
        }
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
    private synchronized void rebalance() {
        if (dStores.size() >= R) {
            synchronized (modifyIndexLock) {
                if (indexStatus.equals(Status.READY)) {
                    // Preps the dStore file map and file locations map.
                    dStoreFilesMap.clear();
                    fileLocations.clear();

                    // Sends list to each of the D stores.
                    for (DStoreRecord record : dStores.values()) {
                        record.getWriter().println(Protocol.LIST_TOKEN);
                        record.getWriter().flush();
                        ControllerLogger.getInstance().messageSent(record.getSocket(), Protocol.LIST_TOKEN);
                    }

                    // Waits for file lists to be received from all D stores.
                    try {
                        CompletableFuture.supplyAsync(() -> {
                            try {
                                dStoreListWait.wait();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            return true;
                        }).get((long) timeout, TimeUnit.MILLISECONDS);
                    } catch (TimeoutException | ExecutionException | InterruptedException ignored) {}

                    removeFailedDStores();
                    updateFileLocations();

                    if (dStores.size() >= R) {
                        redistributeFiles();
                    }
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

    private void addFileLocationForOneDStore(Integer dStorePort, String[] files) {
        Arrays.stream(files)
              .forEach(file -> fileLocations.get(files).add(dStorePort));
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
        int maxFiles = (R * F + (N-1)) / N;
        Map<Integer, List<String>> overflowStores = filterByNumFiles(dStoreFilesMap, n -> n > maxFiles);
        Map<Integer, List<String>> underflowStores = filterByNumFiles(dStoreFilesMap, n -> n < minFiles);
        Map<Integer, List<String>> okFreeStores = filterByNumFiles(dStoreFilesMap, n -> n >= minFiles && n < maxFiles);

        try {
            rebalanceLostFiles(pendingSends, underflowStores, okFreeStores, minFiles, maxFiles);
            rebalanceOverFlowStores(pendingSends, pendingRemoves, underflowStores, overflowStores, okFreeStores,
                                    maxFiles, minFiles);
        } catch (NoPossibleReceiverException e) {
            errorLogger.logError("No possible receiver exception has occured! ");
            errorLogger.logError(e.getStackTrace().toString());
        }
    }

    /**
     * @param pendingSends          requests for D stores to send.
     * @param underFlowStores       stores that have less files than the minimum requirement.
     * @param okFreeStores          stores that have an acceptable number of files that aren't at max capacity.
     * @param minFiles              the minimum number of files that a D store must contain.
     * @throws NoPossibleReceiverException  if there are no possible stores that can store this.
     */
    private void rebalanceLostFiles(HashMap<Integer, ArrayList<SendRequest>> pendingSends,
                                    Map<Integer, List<String>> underFlowStores,
                                    Map<Integer, List<String>> okFreeStores, int minFiles, int maxFiles)
                                    throws NoPossibleReceiverException{
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
                int receiver = getReceiver(minFiles, maxFiles, underFlowStores, okFreeStores, fileInfo.getKey());

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
                                         int maxFiles, int minFiles) {
        // For each over flow D store, move as many files as possible.
        for (Map.Entry<Integer, List<String>> store : overFlowStores.entrySet()) {
            int dStoreSize = store.getValue().size();
            ListIterator<String> files = store.getValue().listIterator();
            String file;
            while (dStoreSize > maxFiles) {
                file = files.next();
                int receiver = 0;
                try {
                    receiver = getReceiver(minFiles, maxFiles, underFlowStores, okFreeStores, file);
                    fileLocations.get(file).add(receiver);

                    // Then adds the send and remove requests.
                    if (!pendingSends.containsKey(store.getKey())) {
                        pendingSends.put(store.getKey(), new ArrayList<>());
                    }
                    pendingSends.get(store.getKey()).add(new SendRequest(file, receiver));

                    if (!pendingRemoves.containsKey(store.getKey())) {
                        pendingRemoves.put(store.getKey(), new ArrayList<>());
                    }
                    pendingRemoves.get(store.getKey()).add(file);

                    dStoreSize--;
                } catch (NoPossibleReceiverException e) {
                    errorLogger.logError("No receivers available to store " + file);
                    errorLogger.logError(e.getStackTrace().toString());
                }
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
     * @param filename              name of the file to be stored.
     * @return if a dstore can be found that can do the following.
     * @throws NoPossibleReceiverException  if no dstores available that can store the file.
     */
    private int getReceiver(int minFiles, int maxFiles, Map<Integer, List<String>> underFlowStores,
                            Map<Integer, List<String>> okFreeStores, String filename) throws NoPossibleReceiverException{
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
                okFreeStores.remove(receiver);
            }
        } else {
            throw new NoPossibleReceiverException("No possible receiver exception for " + filename);
        }

        return receiver;
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

    private class NoPossibleReceiverException extends Exception {
        public NoPossibleReceiverException(String msg) {
            super(msg);
        }
    }
}

