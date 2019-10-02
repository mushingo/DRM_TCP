package drmtcp;


import java.nio.file.Files;
import java.nio.file.Paths;
import java.net.*;
import java.io.*;
import java.util.*;


/* The Content Server provides content in response to requests from clients.
**
** The Content Server takes three command line arguments. The first is the 
** port the server is to listen to for incoming connections. The third is the 
** nameServer port which is used to register the Content Server's 
** ip/port/hostname details. The second is the path to a file containing details
** about the content.
** 
*/
public class Content {
  //Exit Status codes
  private static final int BAD_ARGS = 1;
  private static final int REGISTRATION_FAILURE = 2;  
  private static final int LISTEN_FAILURE = 3;
  private static final int ACCEPT_FAILURE = 4;  

  //Instance variables  
  private ServerSocket serverSocket = null; //listening socket
  private Socket connSocket = null; //connection socket
  private ServerMap servers = null; //map of servers 
  
  private String message; //message read from remote client process
  private BufferedReader in; //stream in from remote client process
  private PrintWriter out; //stream out to remote client process
  
  private StockContent stockContent; //The stored content info read from file

  /* Creates a new Content Object using the command line arguments.
  **
  ** @param args The arguments supplied on the command line. This should be a 
  ** a port for the for the server to listen for connections, the content file 
  ** path and the NameServer port.
  */  
  public static void main (String[] args) {
    new Content(args);
  }
  
  /* Creates a new Content Server Object which registers its details with the 
  ** nameServer and loads up and stores the content details from the supplied 
  ** file. The server then starts listening for new connections and then replies 
  ** to incoming messages. 
  **
  ** @param args Command line arguments supplied to constructor and should be a
  ** port for the Content Server to listen to, the path to the content file and 
  ** the NameServer port.
  */
  public Content(String[] args) {
    int contentPort;
    int nameServerPort;
    
    if (args.length != 3) {
      exit(BAD_ARGS);  
    }
    
    if ((contentPort = check_valid_port(args[0])) < 0) {
      exit(LISTEN_FAILURE);  
    }
    
    if ((nameServerPort = check_valid_port(args[2])) < 0) {
      exit(BAD_ARGS);  
    }
    
    String currentDir = new File("").getAbsolutePath();
    String path = currentDir + ComsFormat.fileSep + args[1];
    
    try {
      stockContent = new StockContent(Files.readAllLines(
          Paths.get(path))); 
    } catch (IOException e) {
      System.out.println("Could not find \"" 
          + args[1] + "\" in directory: " + currentDir);
      exit(BAD_ARGS);
    }
    
    try {
      servers = new ServerMap(nameServerPort);
      servers.register(ComsFormat.content_hostname, contentPort
          , ComsFormat.content_ip);
    } catch (ServerConnectException | RegistrationException e){
      exit(REGISTRATION_FAILURE);
    }
    
    servers.close_nameserver();
    
    listen(contentPort);
    System.err.print("Content waiting for incoming connections\n");
	
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
  
  /* Listen for new connections on the Content's port or exit if this fails.
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
      System.out.println("New connection accepted by Content");
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
  ** temporarily store the message within the Content instance.
  */     
  private void read_message () throws IOException {
    String line = in.readLine();
    System.out.println("Message from client: " + line);
    message = line;
  }

  /* Attempt to close the connection established between another remote process 
  ** and the Content Server and any file streams opened for communication.  
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
  ** content request the Content replies to the client with the appropriate 
  ** content otherwise the message is ignored. If there is no content available
  ** for given Item ID the message is also ignored.
  **/  
  private void process_message () {
    String[] messageParts = message.split(" ");  
    long itemId;
    String content;
	
    if (messageParts.length != 2 
        || !(messageParts[0].equals(ComsFormat.request_content))) {
      return;
    }
    
    try {
      itemId = Long.parseLong(messageParts[1]);
    } catch (NumberFormatException e) {
      return;
    }
	
	content = stockContent.get_content(itemId);
	if (content != null) {
      System.out.println("Content retrieved: " + content);
      out.println(content);
    }
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
  
  /* Exit the Content server with the appropriate error message and status.
  **
  ** @param status the exit status to exit with
  */  
  private void exit(int status) {
    switch (status) {
      case BAD_ARGS: 
        System.err.print("Invalid command line arguments for Content\n");
        System.exit(BAD_ARGS);
      case REGISTRATION_FAILURE: 
        System.err.print("Registration with NameServer failed\n");
        System.exit(REGISTRATION_FAILURE);
      case LISTEN_FAILURE:
        System.err.print("Content unable to listen on given port\n");
      case ACCEPT_FAILURE:
        System.err.print("Encountered IOEXCEPTION after blocking on "
            + "accept, this may indicate that there are no longer "
            + "any file descriptors left to use\n");
        System.exit(ACCEPT_FAILURE);

    }
  }
}

/* A class used to store the content information read from the content file.
*/
class StockContent {
  //A map of the itemId to the content. Tree map is used to preserve ordering
  private TreeMap<Long, String> contentMap;
  
  /* The StockContent object is created and maps item-ID to content for each 
  ** stock item.
  **
  ** @param A list of strings with each containing the item-ID and content for a 
  ** certain item.
  */
  public StockContent(List<String> content) {
    contentMap = new TreeMap<Long, String>();
    for (int i = 0; i < content.size(); i++) {
      String contentParts[] = content.get(i).split(ComsFormat.separator);
      long itemId = Long.parseLong(contentParts[0]);
      String contentName = contentParts[1];
      contentMap.put(itemId, contentName);
    }
  }
  
  /* Return the string representation of the stock content.
  **
  ** @return the string representation of the stock content.
  */
  public String toString() {
    String string = "";
    for (Long itemId : contentMap.keySet()) {
      string = string + itemId + " " + contentMap.get(itemId) 
	      + ComsFormat.newline;
    }
    return string;
  }
  
  /* Returns the content registered for the given itemId
  **
  ** @param itemId the item id associated with the desired content
  ** @return the content associated with the given itemId
  */
  public String get_content (long itemId) {
    return contentMap.get(itemId);
  }
}

