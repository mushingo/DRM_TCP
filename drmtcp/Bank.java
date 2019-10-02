package drmtcp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/* The Bank is used to check if the financial credentials are valid or invalid
**
** The Bank takes two command line arguments. The first is the port the 
** server is to listen to for incoming connections. The second is the nameServer
** port which is used to register the Bank's ip/port/hostname details.
*/
public class Bank {
  //Exit Status codes
  private static final int BAD_ARGS = 1;
  private static final int LISTEN_FAILURE = 2;
  private static final int ACCEPT_FAILURE = 3;
  private static final int REGISTRATION_FAILURE = 4;
  
  //Instance variables
  private ServerSocket serverSocket = null; //listening socket
  private Socket connSocket = null; //connection socket
  private ServerMap servers = null; //map of servers 
  
  private String message; //message read from remote client process
  private BufferedReader in; //stream in from remote client process
  private PrintWriter out; //stream out to remote client process
  
  
  /* Creates a new Bank Object using the command line arguments.
  **
  ** @param args The arguments supplied on the command line. This should be a 
  ** a port for the for the server to listen for connections and the NameServer
  ** port.
  */
  public static void main (String[] args) {
    new Bank(args);
  }
  
  /* Creates a new Bank Object which registers its details with the nameServer
  ** and then starts listening for new connections and then replies to incoming
  ** messages. 
  **
  ** @param args Command line arguments supplied to constructor and should be a
  ** port for the Bank to listen to and the NameServer port.
  */
  public Bank(String[] args) {
    int bankPort;
    int nameServerPort;
    
    if (args.length != 2) {
      exit(BAD_ARGS);  
    }
    
    if ((bankPort = check_valid_port(args[0])) < 0) {
      exit(LISTEN_FAILURE);  
    }
    
    if ((nameServerPort = check_valid_port(args[1])) < 0) {
      exit(BAD_ARGS);  
    }
    
    try {
      servers = new ServerMap(nameServerPort);
      servers.register(ComsFormat.bank_hostname, bankPort, ComsFormat.bank_ip);
    } catch (ServerConnectException | RegistrationException e){
      exit(REGISTRATION_FAILURE);
    }
    servers.close_nameserver();
    
    listen(bankPort);
    System.err.print("Bank waiting for incoming connections\n");
    
	//Accepts a connection, processes messages until none left, then accepts
	//a new connection and repeats.
    while (true) {
      accept();
      while (true) {
        try {
          read_message();
        } catch (IOException e) {
          close_connection();
          break;
        }
        process_message(); 
      }
    }
  }
  
  /* Listen for new connections on the Bank's port or exit if this fail.
  **
  ** @port the port to listen on
  */
  private void listen(int port) {
    try {
          serverSocket = new ServerSocket(port);
      } catch (IOException e) {
          exit(LISTEN_FAILURE);
      }
  }
  
  /* Create a new connection and create and store input and output streams used
  ** to communicate with remote process.
  */  
  private void accept() {
    try {
      connSocket = serverSocket.accept();
      System.out.println("New connection accepted by Bank");
    } catch (IOException e) {
      exit(ACCEPT_FAILURE);
    }
    
    try {
      out = new PrintWriter(connSocket.getOutputStream(), true);
      in = new BufferedReader(
          new InputStreamReader(connSocket.getInputStream()));
    } catch (IOException e) {
      System.err.println("Socket Error");
      return;
    }
  }  
  
  /* Read a message (a single line) from the connected remote processes and 
  ** temporarily store the message within the Bank instance.
  */   
  private void read_message () throws IOException {
    String line = in.readLine();
    System.out.println("Message from client: " + line);
    message = line;
  }  

  /* Attempt to close the connection established between another remote process 
  ** and the Bank and any file streams opened for communication.  
  ** If the closing fails, the failure is ignored.
  */    
  private void close_connection() {
    try {
      if (connSocket != null) {
        connSocket.close();
      }
      if (in != null) {
        in.close();
      }
      if (out != null) {
        out.close();
      }
    } catch (IOException e) {
      System.err.print("Error closing network stream. Ignoring error and "
          + "proceeding to wait for next connection\n");
    }
  }

  /* Process message from remote client process. If the message is a valid 
  ** financial credential check the bank checks to see if the credentials are 
  ** valid. If they are it replies "1" and prints out "OK" if they are not the 
  ** reply is "0" and "NOT OK" is printed out. If the message is not a valid 
  ** financial credential check the bank does not reply.
  **/
  private void process_message () {
    String[] messageParts = message.split(" ");
    String reply;
    long itemId;
    
    if (messageParts.length != 3) {
      return;
    }
    
    try {
      itemId = Long.parseLong(messageParts[0]);
    } catch (NumberFormatException e) {
      return;
    }
	
    System.out.println(itemId);
	
    if ((itemId % 2) == 0){
      reply = "1";
      System.out.println("OK");
    } else {
      reply = "0";
      System.out.println("NOT OK");
    }
    
    out.println(reply);
  }
  
  /* Checks that the supplied port is a number within the valid port range 
  ** > 0 < 65535 and if so returns an int representing that port. 
  ** Otherwise returns -1.
  **
  ** @param porArg the argument to check and convert
  ** @return The port if it's within the valid range, -1 otherwise
  */     
  private int check_valid_port(String portArg) { 
    try {
      Integer.parseInt(portArg);
    } catch (NumberFormatException e) {
      return -1;    
    }
    int port = Integer.parseInt(portArg);
    
    if (port < 1 || port > 65535) {
      return -1;
    }
	
    return port;
  }
  
  /* Exit the Bank server with the appropriate error message and status.
  **
  ** @param status the exit status to exit with
  */
  private void exit(int status) {
    switch (status) {
      case BAD_ARGS: 
        System.err.print("Invalid command line arguments for Bank\n");
        System.exit(BAD_ARGS);
      case REGISTRATION_FAILURE: 
        System.err.print("Registration with NameServer failed\n");
        System.exit(REGISTRATION_FAILURE);
      case LISTEN_FAILURE:
        System.err.print("Bank unable to listen on given port\n");
      case ACCEPT_FAILURE:
        System.err.print("Encountered IOEXCEPTION after blocking on "
            + "accept, this may indicate that there are no longer "
            + "any file descriptors left to use\n");
        System.exit(ACCEPT_FAILURE);
    }
  }
}
