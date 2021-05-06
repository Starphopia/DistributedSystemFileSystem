import java.net.*;
import java.util.*;
import java.io.*;


class Controller {
    private int R;
    private double timeout, rebalancePeriod;
    private HashMap<Integer, DStoreRecord> dStores = new HashMap<>();
    private HashMap<String, Integer> fileSizes = new HashMap<>();
    private HashMap<String, List<Integer>> fileLocations = new HashMap<>();

    private volatile Status indexStatus = Status.READY;
    private final String joinedMsg = "Joined success";
    private HashMap<String, List<Integer>> storeAckList = new HashMap<String, List<Integer>>();

    private ControllerLogger logger;
    private ErrorLogger errorLogger;

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
        } catch (NumberFormatException e) {
            e.printStackTrace();
        } catch (IOException e) {
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

    public void printDStores() {
        System.out.println("The Dstores store files: ");
        dStores.entrySet().stream().forEach(System.out::println);
        System.out.println("The file sizes are: ");
        fileSizes.entrySet().stream().forEach(System.out::println);
    }

    public int getR() {
        return R;
    }

    public double getTimeout() {
        return timeout;
    }

    public HashMap<Integer, DStoreRecord> getDStores() {
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

    public ErrorLogger getErrorLogger() {
        return errorLogger;
    }


    public void setIndexStatus(Status status) {
        indexStatus = status;
    }

    public void addStoreAckExpectation(String fileName) {
        storeAckList.put(fileName, new ArrayList<>());
    }

    public void recordStoreAck(String fileName, Integer port) {
        storeAckList.get(fileName).add(port);
    }

    public List<Integer> getAckReceieved(String fileName) { return storeAckList.get(fileName); }

    public void removeStoreAckExpectation(String fileName) {
        storeAckList.remove(fileName);
    }

    public void newFile(String filename, int size, List<Integer> ports) {
        fileSizes.put(filename, size);
        fileLocations.put(filename, ports);
    }

    /**
     * Returns a list of the files stored in the system.
     * @return
     */
    public Set<String> getFiles() {
        return fileSizes.keySet();
    }
}

