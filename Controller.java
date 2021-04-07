import java.net.*;
import java.io.*;

class Controller {
    private ServerSocket cport;
    private int R;
    private double timeout;
    private double rebalancePeriod;        

    public static void main(String[] args) {
        try {
            new Controller(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Double.parseDouble(args[2]),
                           Double.parseDouble(args[3]));
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param cport                 port to listen on
     * @param R                     number of times that the file will be replicated
     * @param timeout               
     * @param rebalancePeriod       how long to wait to start the next rebalance 
     *                              operation (ms)
     */
    public Controller(int cport, int R, double timeout, double rebalancePeriod) {
        this.R = R;
        this.timeout = timeout;
        this.rebalancePeriod = rebalancePeriod;

        // Creates a new socket for listening to.
        try (ServerSocket ss = new ServerSocket(cport)) {
            for (;;) {
                try (
                    Socket client = ss.accept()
                    BufferedReader in = new BufferedReader(new InputStream(client.getInputStream()));
                ) {

                }
            }
        } catch (IOException e) {
            System.out.println("error " + e);
        }
    }    


}