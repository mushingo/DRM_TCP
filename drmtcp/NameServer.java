package drmtcp;

import java.net.*;
import java.io.*;
import java.util.*;

/* The NameServer is used by other processes to register and lookup server 
** IP Addresses and Ports given a unique host name.
**
** The NameSever takes one command line argument which is the port the server is
** to listen to for incoming connections.
*/
public class NameServer {
  //Exit Status codes
  private static final int BAD_ARGS = 1;
  private static final int LISTEN_FAILURE = 2;
  private static final int ACCEPT_FAILURE = 3;
    
  private static final String REGISTRATION_SUCCESS = "REGISTRATION_SUCCESS";  
  private static final String REGISTRATION_KEYWORD = "REG";
  private static final String LOOKUP_KEYWORD = "LOOKUP";
  
  //Number of valid parts in an IPV4 address
  private static final int IPV4_ADDRESS_PARTS = 4;
  
  //Instance Variables
  private int port; //listening port
  private HashMap<String, DnsEntry> dnsMap; //Map of hostnames to DNSEntries
  private String message; //Message received from remote process
  
  private ServerSocket serverSocket = null; //listening socket
  private Socket connSocket = null; //connect socket to remote process
  private BufferedReader in; //Stream in from remote process
  private PrintWriter out; //Stream out to remote process
  
  /* Creates a new NameServer Object using the command line arguments.
  **
  ** @param args The arguments supplied on the command line. This should be a 
  ** single valid port for the name server to listen on.
  */
  public static void main (String[] args)  {
    new NameServer(args);
  }
  
  /* Creates a new NameServer Object which then starts listening to and 
  ** processing incoming connections and messages.
  **
  ** @param args Command line arguments supplied to constructor and should be a
  ** single valid port
  */
  public NameServer(String[] args) {
    if (args.length != 1) {
      exit_server(BAD_ARGS);  
    }
    
    if ((this.port = check_valid_port(args[0])) < 0) {
      exit_server(BAD_ARGS);  
    }
    
    dnsMap = new HashMap<String, DnsEntry>();
	
	listen();
	System.err.print("Name Server waiting for incoming connections ...\n");
	
	while (true) {
      accept();
      try {
        read_message();
      } catch (IOException e) {
        System.err.print("Message Read Failure. Ignoring\n");
        close_connection();
        continue;
      }
      process_message();
      close_connection();
    }
  }
  
  /* Listen for new connections on the NameServer's port or exit if this fail.
  */
  private void listen() {
    try {
      serverSocket = new ServerSocket(port);
    } catch (IOException e) {
      exit_server(LISTEN_FAILURE);
    } 
  }
  
  /* Create a new connection and create and store input and output streams used
  ** to communicate with remote process.
  */
  private void accept() {
    try {
      connSocket = serverSocket.accept();
      System.out.println("New connection accepted by NameServer.");
    } catch (IOException e) {
      exit_server(ACCEPT_FAILURE);
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
  ** temporarily store the message within the NameServer instance.
  */ 
  private void read_message () throws IOException {
    String line = in.readLine();
    System.out.println("Message from client: " + line);
    message = line;
  }
  
  /* Attempt to close the connection established between another remote process 
  ** and the NameServer and any file streams opened for communication.  
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
  
  /* Process the message received from remote process. The message is checked to
  ** see if it is a Registration or a Lookup query. If it is neither the message
  ** is ignored. A registration query causes the information supplied to be 
  ** registered, a lookup query looks up the information and then replies to 
  ** the remote processes with the appropriate information.
  */
  private void process_message () {
    String[] messageParts = message.split(" ");
      
    if (messageParts[0].equals(REGISTRATION_KEYWORD) 
        && messageParts.length == 4) {
      register(messageParts);      
    } else if (messageParts[0].equals(LOOKUP_KEYWORD) 
        && messageParts.length == 2) {
      String reply = lookup(messageParts);
      lookup_reply(reply);
    } else {
      return;
    }
  }
  
  /* Store the given registration details for a remote process so that they can 
  ** be available to retrieve when required. Basic checks of the IP and Port are
  ** conducted to check that they are valid. If the registration is successful
  ** a message is sent to the registering processes informing it of its success.
  **
  ** @param registrationDetails A string containing the registration details of
  ** a remote processes (hostname, port, IP)
  */
  private void register(String[] registrationDetails) {
    int port;
    String hostname;
    String ipAddress;
    
    if ((port = check_valid_port(registrationDetails[2])) < 0) {
      return;
    }
    if (!check_valid_ip(registrationDetails[3]) 
        || !check_valid_hostname(registrationDetails[1])) {
      return;
    }
    hostname = registrationDetails[1];
    ipAddress = registrationDetails[3];
    
    DnsEntry dnsEntry = new DnsEntry(hostname, ipAddress, port);
    
    dnsMap.put(dnsEntry.get_hostName(), dnsEntry);
    System.out.println(REGISTRATION_SUCCESS);
    
    out.println(REGISTRATION_SUCCESS);
  }
  
  /* A very basic check of a supplied hostname to check that it's a non-null,
  ** non empty string.
  **
  ** @param hostname A string representing a remote process's hostname.
  */ 
  private boolean check_valid_hostname(String hostname) {
    if (hostname.isEmpty() || hostname == null) {
      return false;
    }
    return true;
  }
    
  /* A basic check to make sure the supplied string is formatted a valid ipv4 
  ** dotted decimal address (or is localhost).
  **
  ** @param ipAddress An IP address to check
  ** @return true if the IP is valid, false if invalid
  */  
  private boolean check_valid_ip(String ipAddress) {
    String[] ipParts;
    
    if (ipAddress.equalsIgnoreCase("localhost")) {
      return true;
    }
    
    if (ipAddress.isEmpty() || ipAddress == null) {
      return false;
    }
    
    ipParts = ipAddress.split("\\.", 5); 
    
    if (ipParts.length != IPV4_ADDRESS_PARTS ) {
      return false;
    }
    
    for (int i = 0; i < IPV4_ADDRESS_PARTS; i++) {
      try {
        Integer.parseInt(ipParts[i]);
      } catch (NumberFormatException e) {
        return false;    
      }
      
      int decimal = Integer.parseInt(ipParts[i]);
      
      if (decimal < 0 || decimal > 255) {
        return false;
      }
    }
	
    return true;
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
  
  /* Looks up the supplied hostname for a remote process, retrieves the IP 
  ** address and port and formats message into appropriate response format.
  **
  ** @param messageParts The lookup message from the host divided into words
  ** @return A formatted lookup reply string.
  */
  private String lookup(String[] messageParts) {
    String message;
    DnsEntry dnsEntry = dnsMap.get(messageParts[1]);
    
    if (dnsEntry == null) {
      message = "Error: Process has not registered with the Name Server";
    } else {
      message = dnsEntry.get_ipaddress() + " " + dnsEntry.get_port();
    }
    
    return message;
  }
  
  /* Sends a lookup reply message to a remote process if possible. Otherwise 
  ** prints an error and returns.
  **
  ** @param reply the message to reply with
  */
  private void lookup_reply(String reply) {
    PrintWriter out;
    
    try {
      out = new PrintWriter(connSocket.getOutputStream(), true);
    } catch (IOException e) {
      System.err.println("Socket Error");
      return;
    }
    
    out.println(reply);
    out.close();
  }
  
  /* Exit the name server with the appropriate error message and status.
  **
  ** @param status the exit status to exit with
  */
  private void exit_server(int status) {
    switch (status) {
      case BAD_ARGS: 
        System.err.print("Invalid command line arguments for "
            + "NameServer\n");
        System.exit(BAD_ARGS);
      case LISTEN_FAILURE:
        System.err.print("Cannot listen on given port number " + port 
            + "\n");
        System.exit(LISTEN_FAILURE);
      case ACCEPT_FAILURE:
        System.err.print("Encountered IOEXCEPTION after blocking on "
            + "accept, this may indicate that there are no longer "
            + "any file descriptors left to use\n");
        System.exit(ACCEPT_FAILURE);
    }
  }
}

/* A class used to store the hostname, ipAddres and port of a remote process 
** that has registered with the nameserver.
*/
class DnsEntry {
  private String hostName;
  private String ipAddress;
  private int port;
  
  /* Creates a new DNS entry storing the hostname, ipAddress and port of a 
  ** remote process
  **
  ** @param hostname the process's hostname
  ** @param ipAddreess the process's IP address
  ** @param port the process's port
  */
  public DnsEntry(String hostname, String ipAdddress, int port) {
    this.hostName = hostname;
    this.ipAddress = ipAdddress;
    this.port = port;
  }
  
  /* Returns the IP address of the remote process.
  ** 
  ** @return the IP address of the remote process.
  */
  public String get_ipaddress () {
    return this.ipAddress;
  }
  
  /* Returns the port of the remote process.
  ** 
  ** @return the port of the remote process.
  */  
  public int get_port () {
    return this.port;
  }
  
  /* Returns the host name of the remote process.
  ** 
  ** @return the host name of the remote process.
  */  
  public String get_hostName () {
    return this.hostName;
  }
    
}
