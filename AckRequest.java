import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
