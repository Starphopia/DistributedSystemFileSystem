import java.net.*;
import java.util.*;
import java.io.*;


class Controller {
    private final int R;
    private final double timeout;
    private final double rebalancePeriod;
    private volatile HashMap<Integer, Socket> dStores = new HashMap<>();
    private volatile HashMap<String, Integer> fileSizes = new HashMap<>();
    private volatile HashMap<String, List<Integer>> fileLocations = new HashMap<>();

    private volatile Status indexStatus = Status.READY;
    private HashMap<String, List<Integer>> storeAckList = new HashMap<>();
    private final Object ackListLock = new Object();
    private final ErrorLogger errorLogger;

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

    public HashMap<Integer, Socket> getDStores() {
        return dStores;
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

    public ErrorLogger getErrorLogger() {
        return errorLogger;
    }


    public synchronized void setIndexStatus(Status status) {
        indexStatus = status;
    }

    public synchronized boolean isReady() { return indexStatus.equals(Status.READY);};

    public void addStoreAckExpectation(String fileName) {
        synchronized(ackListLock) {
            storeAckList.put(fileName, new ArrayList<>());
        }
    }

    public void recordStoreAck(String fileName, Integer port) {
        synchronized(ackListLock) {
            storeAckList.get(fileName).add(port);
        }
    }

    public List<Integer> getAckReceived(String fileName) {
        synchronized(ackListLock) {
            return storeAckList.get(fileName);
        }
    }

    public void removeStoreAckExpectation(String fileName) {
        synchronized (ackListLock) {
            storeAckList.remove(fileName);
        }
    }

    public void newFile(String filename, int size, List<Integer> ports) {
        fileSizes.put(filename, size);
        fileLocations.put(filename, ports);
    }


    /**
     * Returns a list of the files stored in the system.
     * @return the files.
     */
    public Set<String> getFiles() {
        return fileSizes.keySet();
    }
}

