import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

/**
 * For testing purposes.
 */
public class DummyClient {
    private ErrorLogger logger;
    private int cport;
    private double timeout;

    public static void main(String[] args) {
        new DummyClient(Integer.parseInt(args[0]), Double.parseDouble(args[1]));
    }

    public DummyClient(int cport, double timeout) {
        Random random = new Random();
//        logger = new ErrorLogger("Client" + random.nextDouble() + ".log");
        this.cport = cport;
        this.timeout = timeout;

        try {
            testStoreFunctionality("test1.txt");
        } catch(Exception e) {
            logger.logError(e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Stores a file.
     * @param path      path to the file to be stored.
     */
    private void testStoreFunctionality(String path) {
        File file = new File(path);
        Integer[] dStores = null;

        while (dStores == null) {
            try (
                Socket conSocket = new Socket(InetAddress.getLocalHost(), cport);
                PrintWriter writer = new PrintWriter(new OutputStreamWriter(conSocket.getOutputStream()));
                BufferedReader reader = new BufferedReader(new InputStreamReader(conSocket.getInputStream()));

            ) {
                writer.println(Protocol.STORE_TOKEN + " " + file.getName() + " " + file.length());
                writer.flush();
                conSocket.setSoTimeout((int)timeout);
                String[] line = null;
                while (line == null || !(line)[0].equals(Protocol.STORE_TO_TOKEN)) {
                     line = reader.readLine().split(" ");
                }

                dStores = Arrays.stream(Arrays.copyOfRange(line, 1, line.length))
                                .map(Integer::valueOf)
                                .toArray(Integer[]::new);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (SocketTimeoutException e) {
                System.out.println("Timeout!");
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ArrayIndexOutOfBoundsException e) {
                e.printStackTrace();
            }
        }
    }
}
