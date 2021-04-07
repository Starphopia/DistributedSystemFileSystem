public class Client {
   private int cport;
   private double timeout;
   
    public static void main(String[] args) {
        try {
            new Client(Integer.parseInt(args[0]), Double.parseDouble(args[1]));
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
    }

   public Client(int cport, double timeout) {
        
   }
}
