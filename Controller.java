import java.net.*;
import java.util.*;
import java.io.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;


class Controller {
    private final int R;
    private final double timeout;
    private final double rebalancePeriod;
    private ConcurrentHashMap<Integer, Socket> dStores = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Integer> fileSizes = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<Integer>> fileLocations = new ConcurrentHashMap<>();

    private volatile Status indexStatus = Status.READY;
    private ConcurrentHashMap<String, AckRequest> storeAckMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, AckRequest> removeAckMap = new ConcurrentHashMap<>();
    private final ErrorLogger errorLogger;

    private final Object modifyIndexLock = new Object();

    /**
     * Status of the index.
     */
    public enum Status {
        READY, STORE_IN_PROGRESS, STORE_COMPLETED, REMOVE_IN_PROGRESS, REMOVE_COMPLETED;
    }

    public static void main(String[] args) {
        try {
            ControllerLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL);
            new Controller(Integer.parseInt(args[0]), Integer.parseInt(args[1]),
                    Double.parseDouble(args[2]), Double.parseDouble(args[3]));
        } catch (NumberFormatException | IOException e) {
            e.getMessage();
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
                new Thread(new ConnectionThread(ss, client, this)).start();
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

    public ConcurrentHashMap<Integer, Socket> getDStores() {
        return dStores;
    }

    public boolean hasJoined(int port, Socket dStoreSocket) {
        return dStores.containsKey(port) && dStores.get(port).equals(dStoreSocket);
    }

    public void addDStore(int port, Socket dStoreSocket) {
        dStores.put(port, dStoreSocket);
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

    /**
     * @param filename      name of file to be found in the system.
     * @return the ports of D stores that stores this file.
     */
    public List<Integer> getDStoresWithFile(String filename) { return fileLocations.get(filename); }

    public List<Socket> getDStoreSockets(List<Integer> ports) {
        return ports.stream()
                    .map(dStores::get)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
    }

    public ErrorLogger getErrorLogger() {
        return errorLogger;
    }


    public synchronized void setIndexStatus(Status status) {
        indexStatus = status;
    }

    public synchronized boolean isReady() { return indexStatus.equals(Status.READY);};

    public boolean storeAckExists(String fileName) {
        return storeAckMap.contains(fileName);
    }

    public boolean removeAckExists(String fileName) {
        return removeAckMap.contains(fileName);
    }

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



}

