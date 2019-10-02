package drmtcp;

import java.net.*;
import java.io.*;
import java.util.*;
import java.nio.file.Paths;
import java.nio.file.Files;

/* The Store performs intermediation for client processes so they can request 
** lists of items held by the store and facilitate the buying of those items
** by the clients by checking financial info  with the Bank server and 
** retrieving content from the Content Server.
**
** 
** The Store Server takes three command line arguments. The first is the 
** port the server is to listen to for incoming connections. The third is the 
** nameServer port which is used to register the Store Server's 
** ip/port/hostname details. The second is the path to a file containing details
** about the store stock.
** 
*/
public class Store {
  //Exit Status codes
  private static final int BAD_ARGS = 1;
  private static final int REGISTRATION_FAILURE = 2;  
  private static final int LISTEN_FAILURE = 3;
  private static final int ACCEPT_FAILURE = 4;
  private static final int LOOKUP_FAILURE  = 5;  
  private static final int NAMESERVER_CONNECT_FAIL  = 6;

  //Instance variables  
  private ServerMap servers = null; //map of servers 
  private ServerSocket serverSocket = null; //listening socket
  private Socket connSocket = null; //connection socket 
  
  private String message; //message read from remote client process
  private BufferedReader in; //stream in from remote client process
  private PrintWriter out; //stream out to remote client process
  
  private Stock stock; //The stock content info read from file
  
  /* Creates a new Store Object using the command line arguments.
  **
  ** @param args The arguments supplied on the command line. This should be a 
  ** a port for the for the server to listen for connections, the stock file 
  ** path and the NameServer port.
  */    
  public static void main (String[] args) {
    new Store(args);
  }
  
  /* Creates a new Store Object which registers its details with the nameServer
  ** and loads up and stores the stock details from the supplied file. The
  ** server then looks up the Bank and Content server details and connects to
  ** these servers then starts listening for new connections from clients and 
  ** connects and replies to incoming messages. 
  **
  ** @param args The arguments supplied on the command line. This should be a 
  ** a port for the for the server to listen for connections, the stock file 
  ** path and the NameServer port.
  */  
  public Store(String[] args) {
    int stockPort;
    int nameServerPort;
    
    if (args.length != 3) {
      exit(BAD_ARGS);  
    }
    
    if ((stockPort = check_valid_port(args[0])) < 0) {
      exit(BAD_ARGS);  
    }
    
    if ((nameServerPort = check_valid_port(args[2])) < 0) {
      exit(BAD_ARGS);  
    }
    
    String currentDir = new File("").getAbsolutePath();
    String path = currentDir + ComsFormat.fileSep + args[1];  
    
    try {
      stock = new Stock(Files.readAllLines(Paths.get(path))); 
      
    } catch (IOException e) {
      System.out.println("Could not find \""  + args[1] 
          + "\" in directory: " + currentDir);
      exit(BAD_ARGS);
    }
    
    try {
      servers = new ServerMap(nameServerPort);
      servers.register(ComsFormat.store_hostname, stockPort
          , ComsFormat.store_ip);
    } catch (ServerConnectException | RegistrationException e){
      exit(REGISTRATION_FAILURE);
    }
    
    try {
      servers.add_server(ComsFormat.content_hostname);
      servers.add_server(ComsFormat.bank_hostname);
    } catch (LookupException e){
      System.err.print(e.getMessage() + ComsFormat.separator 
          + "has not registered\n");
      exit(LOOKUP_FAILURE);
    } catch (ServerConnectException | NameServerContactException e){
      System.err.print("Could not contact NameServer\n");
      exit(NAMESERVER_CONNECT_FAIL);
    } 
    servers.close_nameserver();
    
    listen(stockPort);
    System.err.print("Store waiting for incoming connections\n");
	
	//Accepts a connection, processes messages until none left, then accepts
	//a new connection and repeats.  	
        while (true) {
          accept();
          try {
            read_message();
          } catch (IOException e) {
            close_connection();
            continue;
          }
          process_message();
          close_connection();
      }
  
  }
  
  /* Listen for new connections on the Store's port or exit if this fails.
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
      // At this point, we have a connection
      System.out.println("Connection accepted by Store");
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
  ** temporarily store the message within the Store instance.
  */  
  private void read_message() throws IOException {
    String line = in.readLine();
    System.out.println("Message from client: " + line);
    message = line;
  }
  
  /* Attempt to close the connection established between another remote process 
  ** and the Store and any file streams opened for communication.  
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
  
  /* If the message is a valid list request the Store replies with the list, if 
  ** it is a valid buy request the store processes the buy request and sends the
  ** result, otherwise the message is ignored.
  */
  private void process_message() {
    if (message.equals(ComsFormat.listRequest)){
      send_list();
    } 
      
    String[] messageParts = message.split(ComsFormat.separator);
      
    if (messageParts.length == 3 
      && (messageParts[0].equals(ComsFormat.buyRequest))) {       
      process_buy_request(messageParts);
    }

    out.close();
    return;
  }
  
  /* Send a formatted Stock list to the client processes connected to store.
  */
  private void send_list() {
    out.println(ComsFormat.listStart);
    out.print(stock.toString());
    out.println(ComsFormat.listEnd);
  }
  
  /* Extract item requested to buy and check it's valid. Check financial info
  ** with Bank Server, attempt to retrieve content from Content Server. If any 
  ** of the checks or attempts fail reply to client processes with a transaction 
  ** fail message. Otherwise if everything succeeds send the content to client.
  **
  ** @param messageParts the buy message received broken into an array of words
  */
  private void process_buy_request(String[] messageParts) {
    long creditCard = 0;
    long itemId = 0;
    float itemPrice = 0;
    String bankMsg = "";
    String bankReply = "";
    String contentMsg = "";
    String content = "";
      
    try {
      creditCard = Long.parseLong(messageParts[1]);
      itemId = Long.parseLong(messageParts[2]);
      itemPrice = stock.get_price(itemId);
    } catch (NumberFormatException e) {
      transaction_fail(itemId);
      return;
    } 
    if (itemPrice < 0) {
      transaction_fail(itemId);
      return;
    }
        
    bankMsg = itemId + ComsFormat.separator + itemPrice + ComsFormat.separator 
	    + creditCard;
      
    servers.get_server(ComsFormat.bank_hostname).get_print_writer()
	    .println(bankMsg);
    try {
      System.out.println(bankMsg);
      bankReply = servers.get_server(ComsFormat.bank_hostname)
          .get_buffered_reader().readLine();
      System.out.println(bankReply);
    } catch (IOException e) {
        transaction_fail(itemId);
    }
      
    contentMsg = ComsFormat.request_content + ComsFormat.separator + itemId;
      
    if (bankReply.equals(ComsFormat.purchase_success)) {
      content = get_content(contentMsg);
      System.out.println(content);
      if (content.equals("")) {
        transaction_fail(itemId);
        return;
      }
      out.println(content);
    } else if (bankReply.equals(ComsFormat.purchase_fail)) {
      transaction_fail(itemId);
    }
  }  
  
  /* Attempt to retrieve content from Content server for a given item. If the 
  ** attempt fails return an empty string as the content.
  **
  ** @param message the message used to request content item
  ** @return the content, blank if attempt to retive fails
  */
  private String get_content(String message) {
    String content = "";
    servers.get_server(ComsFormat.content_hostname)
	    .get_print_writer().println(message);
    
    try {
      content = servers.get_server(ComsFormat.content_hostname)
          .get_buffered_reader().readLine();
    } catch (IOException e) {
      return content;
    }
      return content;
  }
  
  /* Send a message indicating that the item buy request has failed. 
  **
  ** @param itemId the ID of the item that the buy attempted failed on
  */  
  private void transaction_fail(long itemId) {
    out.println(itemId + ComsFormat.separator + ComsFormat.transaction_fail);
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
  
  /* Exit the Store server with the appropriate error message and status.
  **
  ** @param status the exit status to exit with
  */    
  private void exit(int status) {
    switch (status) {
      case BAD_ARGS: 
        System.err.print("Invalid command line arguments for Store\n");
        System.exit(BAD_ARGS);
      case REGISTRATION_FAILURE: 
        System.err.print("Registration with NameServer failed\n");
        System.exit(REGISTRATION_FAILURE);
      case LOOKUP_FAILURE:
        System.exit(LOOKUP_FAILURE);
      case LISTEN_FAILURE:
        System.err.print("Store unable to listen on given port\n");
        System.exit(LISTEN_FAILURE);
      case ACCEPT_FAILURE:
        System.err.print("Encountered IOEXCEPTION after blocking on "
            + "accept, this may indicate that there are no longer "
            + "any file descriptors left to use\n");
        System.exit(ACCEPT_FAILURE);
    }
  }
}

/* A class used to store the stock information read from the stock file.
*/
class Stock {
  //A map of the itemId to the content. Tree map is used to preserve ordering
  private TreeMap<Long, Float> stockMap;
  
  public Stock(List<String> stock) {
    stockMap = new TreeMap<Long, Float>();
    for (int i = 0; i < stock.size(); i++) {
      String stockParts[] = stock.get(i).split(ComsFormat.separator);
      long itemId = Long.parseLong(stockParts[0]);
      float itemPrice = Float.parseFloat(stockParts[1]);
      stockMap.put(itemId, itemPrice);
    }
  }
  
  /* Return the price of the given stock item.
  **
  ** @param itemId the ID of the stock item to look up
  ** @return the price of the given stock item
  */ 
  public float get_price (long itemId) {
    float noItem = -1;
    return stockMap.getOrDefault(itemId, noItem);
  }
  
  /* Return the string representation of the entire stock.
  **
  ** @return the string representation of the entire stock.
  */  
  public String toString () {
    String string = "";
    for (Long itemId: stockMap.keySet()) {
      string = string + itemId + " " + stockMap.get(itemId) 
	      + ComsFormat.newline;
    }
    return string;
  }
}

/* A class used to connect to the NameServer and retrieve and store information
** about other remote servers.
*/
class ServerMap {
  private HashMap<String, Server> servers; //A mapping of hostnames to Servers
  private Server nameServer; // The NameServer Server
  
  private int nameServerPort; //Name Server port
  
  /* Create a new map of servers and open connection/store information about the 
  ** nameServer. The map associates host name with a Server object. The server
  ** object represents a connected server.
  **
  ** @param nameServerPort the port of the Name Server
  */
  public ServerMap(int nameServerPort) throws ServerConnectException{
    servers = new HashMap<String, Server>();
    this.nameServerPort = nameServerPort;
    nameServer = new Server(ComsFormat.nameserver_hostname
        , ComsFormat.nameserver_ip, nameServerPort);
  }
  
  /* Register server details with the name Server.
  **
  ** @param host the hostname of the service to register
  ** @param port the port of the service to register
  ** @param the ip address of the service to register
  */
  public void register(String host, int port, String ip) 
      throws RegistrationException{
    String reply;
    
    try {
      reply = talk_to_nameserver(ComsFormat.registration 
        + ComsFormat.separator + host + ComsFormat.separator + port 
        + ComsFormat.separator + ip);
    } catch (NameServerContactException e) {
      throw new RegistrationException();
    }
      
    if (!reply.equals(ComsFormat.regSucesss)) {
      throw new RegistrationException();
    }
  }
  
  /* Connect to a new server and add it's details to the map of servers.
  ** 
  ** @param host the hostname of the server
  */
  public void add_server(String host) throws LookupException
      , ServerConnectException, NameServerContactException{
      
    String ip = lookup_ip(host);
    int port = lookup_port(host);
    
    Server server = new Server(host, ip, port);
    
    servers.put(host, server);
  }
  
  /* Close the connection and any streams established to the name server if 
  ** possible. Ignore if any problems are encountered.
  **/  
  public void close_nameserver() {
    try {
      if (nameServer.get_socket() != null) {
        nameServer.get_socket().close();
      }
      if (nameServer.get_buffered_reader() != null) {
        nameServer.get_buffered_reader().close();
      }
      if (nameServer.get_print_writer() != null) {
        nameServer.get_print_writer().close();
      }
    } catch (IOException e) {
      System.err.print("Error closing Name Server connection. Ignoring "
          + "error.\n");
    }
  }
  
  
  /* Send a lookup request to the name server to get the port and ip of given
  ** host.
  **
  ** @param host the hostname to look up
  ** @return an array of strings containing the port and ip of host
  */
  private String[] lookup(String host) throws LookupException
      , NameServerContactException {
    String reply;
    
    try { 
      nameServer = new Server(ComsFormat.nameserver_hostname 
          , ComsFormat.nameserver_ip,nameServerPort);
    } catch (ServerConnectException e) {
      throw new NameServerContactException();
    }
    
    reply = talk_to_nameserver(ComsFormat.lookup + ComsFormat.separator + host);
      
    if (reply.equals(ComsFormat.lookupError)) {
      throw new LookupException(host);
    }
    return reply.split(ComsFormat.separator);
  }
  
  /* Returns the IP address of the given host after looking it up with the name
  ** Server.
  **
  ** @param host the hostname to lookup
  ** @return the IP address as a string
  */  
  private String lookup_ip (String host) throws LookupException
      , NameServerContactException{
    return lookup(host)[0];
  }
  
  /* Returns the port of the given host after looking it up with the name
  ** Server.
  **
  ** @param host the hostname to lookup
  ** @return the port of the given host
  */    
  private int lookup_port (String host) throws LookupException
      , NameServerContactException{
    return Integer.parseInt(lookup(host)[1]);
  }
  
  /* Send a message to the nameserver and get the reply.
  **
  ** @param message the message to send to the Name Server
  ** @return the message reply recived from the Name Server
  */
  private String talk_to_nameserver (String message) 
      throws NameServerContactException {
    String reply = "";
	
    try {
      nameServer.get_print_writer().println(message);
      reply = nameServer.get_buffered_reader().readLine();
    } catch (IOException e) {
      throw new NameServerContactException();
    }
	
    return reply;      
  }
  
  /* Return the server object for the given hostname.
  ** 
  ** @param host the hostname of the server to retrieve
  ** @return the Server object associated with hostname
  */
  public Server get_server(String host) {
    return servers.get(host);
  }
}

/* A class used to store information about a remote Server, connect to a remote
** server and store information about and tools related to the connection.
*/
class Server {
  private String host; //hostname of server
  private String ip; //ip address of server
  private int port; // port of server
  private Socket socket; //socket of connected server
  private PrintWriter out; //out stream to server
  private BufferedReader in; //in stream from server
  
  /* Create a new Server object. Store information about the server (host, ip,
  ** port and then attempt to connect to the server and open streams into and 
  ** out of the new connection. Store information about these streams.
  **
  ** @param host the server hostname
  ** @param ip the server ip
  ** @param port the server port
  */
  public Server(String host, String ip, int port) 
      throws ServerConnectException{
    this.host = host;
    this.ip = ip;
    this.port = port;
    
    try {
      this.socket  = new Socket(ip, port);
    
      this.out = new PrintWriter(socket.getOutputStream(), true);
      this.in = new BufferedReader(new InputStreamReader(
        socket.getInputStream()));
    } catch (IOException e) {
      throw new ServerConnectException();
    }
  }
  
  /* Return the ip address of server object.
  **
  ** @return the ip address of the server
  */  
  public String get_ip() {
    return this.ip;
  }
  
  /* Return a print writer stream associated with server connection.
  **
  ** @return the print writer of the server connection
  */   
  public PrintWriter get_print_writer() {
    return this.out;
  }
  
  /* Return a buffered reader stream associated with server connection.
  **
  ** @return the buffered reader of the server connection
  */     
  public BufferedReader get_buffered_reader() {
    return this.in;
  }
  
  /* Return the hostname of server object.
  **
  ** @return the hostname of the server
  */    
  public String get_host() {
    return this.host;
  }
  
  /* Return the port of server object.
  **
  ** @return the port of the server
  */   
  public int get_port() {
    return this.port;
  }
  
  /* Return the socket of the server connection.
  **
  ** @return the socket of the server
  */ 
  public Socket get_socket() {
    return this.socket;
  }
}

/* Definitions of some commonly used communication Strings.
*/
class ComsFormat {
  private static final String DEFAULT_IP = "localhost";
  
  public static final String separator = " ";
  public static final String newline = System.getProperty("line.separator");
  public static final String fileSep = System.getProperty("file.separator");
  public static final String registration = "REG";
  public static final String regSucesss = "REGISTRATION_SUCCESS";
  public static final String lookup = "LOOKUP";
  public static final String lookupError = "Error: Process has not registered"
      + " with the Name Server";
  public static final String listRequest = "LIST";
  public static final String buyRequest = "BUY";
  public static final String listStart = "LIST_START";
  public static final String listEnd = "LIST_END";
  public static final String purchase_success = "1";
  public static final String purchase_fail = "0";
  public static final String transaction_fail = "\"transaction aborted\"";
  public static final String request_content = "REQ";
  public static final String store_hostname = "Store";
  public static final String bank_hostname = "Bank";
  public static final String content_hostname = "Content";
  public static final String nameserver_hostname = "NameServer";
  public static final String nameserver_ip = DEFAULT_IP;
  public static final String bank_ip = DEFAULT_IP;
  public static final String content_ip = DEFAULT_IP;
  public static final String store_ip = DEFAULT_IP;
}

/* An exception to throw if a NameServer lookup fails.
*/
class LookupException extends Exception {
  public LookupException(String message) {
        super(message);
    }
    private static final long serialVersionUID = 154297963212548754L;
}

/* An exception to throw if a NameServer registration fails.
*/
class RegistrationException extends Exception {
    
    private static final long serialVersionUID = 342343243245433L;
}

/* An exception to throw if a sever connection attempt fails.
*/
class ServerConnectException extends Exception {
    
    private static final long serialVersionUID = 649841691563749879L;
}

/* An exception to throw if a NamSever connection attempt fails.
*/
class NameServerContactException extends Exception {
    
    private static final long serialVersionUID = 345643534534879L;
}

