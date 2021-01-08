package app.mq.testharness;

import java.io.File;
/*
 * 
 * 1) In IBM MQ classes for Java, set the property 
 *    MQConstants.USE_MQCSP_AUTHENTICATION_PROPERTY to true in the properties hashtable passed to the com.ibm.mq.MQQueueManager constructor.
      Is the property value USE_MQCSP_AUTHENTICATION_PROPERTY?
      
   2) In IBM MQ classes for JMS, set the property 
      JMSConstants.USER_AUTHENTICATION_MQCSP to true, on the appropriate connection factory 
      prior to creating the connection.
      Is the property value USER_AUTHENTICATION_MQCSP?
   
   3) Globally, set the System Property 
      com.ibm.mq.cfg.jmqi.useMQCSPauthentication to a value indicating true, 
      for example, by adding -Dcom.ibm.mq.cfg.jmqi.useMQCSPauthentication=Y to the command line. 

 */
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.List;
import java.util.concurrent.*;

import javax.management.InstanceNotFoundException;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;

import com.ibm.mq.constants.MQConstants;

import app.mq.testharness.Constants;

public class TestHarness {

	private static String hostname;
	static public String HostName() {
		return hostname;
	}
	public static void HostName(String v) {
		hostname = v;
	}
	
	private static int port = 1501;
	static public int Port() {
		return port;
	}
	public static void Port(int v) {
		port = v;
	}

	private static String queuemanagername;
	static public String QueueManagerName() {
		return queuemanagername;
	}
	public static void QueueManagerName(String v) {
		queuemanagername = v;
	}
	
	private static String channel;
	static public String ChannelName() {
		return channel;
	}
	public static void ChannelName(String v) {
		channel = v;
	}
	
	private static String userid;
	static public String UserId() {
		return userid;
	}
	public static void UserId(String v) {
		userid = v;
	}
	private static String password;
	static public String Password() {
		return password;
	}
	public static void Password(String v) {
		password = v;
	}
	//  = "LOAD.TEST.Q1"
	private static String queuename;
	static public String QueueName() {
		return queuename;
	}
	public static void QueueName(String v) {
		queuename = v;
	}
	
	private static boolean usetlsconnection = false;
	static public boolean UseTLSConnection() {
		return usetlsconnection;
	}
	static public void UseTLSConnection(boolean v) {
		usetlsconnection = v;
	}
	
	private static boolean usemqcsp;
	static public boolean UseMQCSP() {
		return usemqcsp;
	}
	static public void UseMQCSP(boolean v) {
		usemqcsp = v;
	}

	private static boolean simulation;
	static public boolean Simulation() {
		return simulation;
	}
	static public void Simulation(boolean v) {
		simulation = v;
	}

	private static boolean persistent;
	static public boolean Persistent() {
		return persistent;
	}
	static public void Persistent(boolean v) {
		persistent = v;
	}
	private static String function;
	static public String Function() {
		return function;
	}
	static public void Function(String v) {
		function = v;
	}

	private static int numberofmessages;
	static public int NumberOfMessages() {
		return numberofmessages;
	}
	static public void NumberOfMessages(int v) {
		numberofmessages = v;
	}

	
	//"TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256"
	//private static String cipher = "TLS_RSA_WITH_AES_256_CBC_SHA256";

	private static String cipher = "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256";
	static public String Cipher() {
		return cipher;
	}
	public static void Cipher(String v) {
		cipher = v;
	}

	private static int numberofprodcuerthreads = 1;
	static public int NumberOfProducerThreads() {
		return numberofprodcuerthreads;
	}
	public static void NumberOfProducerThreads(int v) {
		numberofprodcuerthreads = v;
	}
	private static int numberofconsumerthreads;
	static public int NumberOfConsumerThreads() {
		return numberofconsumerthreads;
	}
	public static void NumberOfConsumerThreads(int v) {
		numberofconsumerthreads = v;
	}
	private static int decimalorbinary;
	static public int DecimalOrBinary() {
		return decimalorbinary;
	}
	public static void DecimalOrBinary(int v) {
		decimalorbinary = v;
	}
	private static int messagesize;
	static public int MessageSize() {
		return messagesize;
	}
	public static void MessageSize(int v) {
		messagesize = v;
	}
	
	private static long stats;
	static public long Stats() {
		return stats;
	}
	static public void Stats(long v) {
		stats = v;
	}

	private static int waitinterval;
	static public int WaitInterval() {
		return waitinterval;
	}
	static public void WaitInterval(int v) {
		waitinterval = v;
	}

	private static int codepage;
	static public int CodePage() {
		return codepage;
	}
	static public void CodePage(int v) {
		codepage = v;
	}
	
	private static String keystore;
	static public String KeyStore() {
		return keystore;
	}
	static public void KeyStore(String v) {
		keystore = v;
	}
	private static String storepass;
	static public String StorePass() {
		return storepass;
	}
	static public void StorePass(String v) {
		storepass = v;
	}
	private static boolean ibmcipher;
	static public boolean IBMCipher() {
		return ibmcipher;
	}
	static public void IBMCipher(boolean v) {
		ibmcipher = v;
	}

	private static int taskshutdown;
	static public int TaskShutDown() {
		return taskshutdown;
	}
	static public void TaskShutDown(int v) {
		taskshutdown = v;
	}
	private static int threadpool;
	static public int ThreadPool() {
		return threadpool;
	}
	static public void ThreadPool(int v) {
		threadpool = v;
	}

	private static int sync;
	static public int Sync() {
		return sync;
	}
	static public void Sync(int v) {
		sync = v;
	}

	private static boolean splitmessages;
	static public boolean SplitMessages() {
		return splitmessages;
	}
	static public void SplitMessages(boolean v) {
		splitmessages = v;
	}
	private static int splitmessagesize;
	static public int SplitMessageSize() {
		return splitmessagesize;
	}
	static public void SplitMessageSize(int v) {
		splitmessagesize = v;
	}
	private static boolean groupmessages;
	static public boolean GroupMessages() {
		return groupmessages;
	}
	static public void GroupMessages(boolean v) {
		groupmessages = v;
	}
	private static int groupmessagesize;
	static public int GroupMessageSize() {
		return groupmessagesize;
	}
	static public void GroupMessageSize(int v) {
		groupmessagesize = v;
	}

	private static boolean log;
	static public boolean Log() {
		return log;
	}
	static public void Log(boolean v) {
		log = v;
	}
	private static boolean verbose;
	static public boolean Verbose() {
		return verbose;
	}
	static public void Verbose(boolean v) {
		verbose = v;
	}

	private static String inputfilename;
	static public String InputFileName() {
		return inputfilename;
	}
	static public void InputFileName(String v) {
		inputfilename = v;
	}
	
	static private int sleepinterval;
	public static int SleepInterval() {
		return sleepinterval;
	}
	public static void SleepInterval(int v) {
		sleepinterval = v;
	}
	static private int distribute;
	public static int Distribute() {
		return distribute;
	}
	public static void Distribute(int v) {
		distribute = v;
	}
	static private int distsleep;
	public static int DistSleep() {
		return distsleep;
	}
	public static void DistSleep(int v) {
		distsleep = v;
	}
	
	private static ExecutorService pool;
	
	private static List<MessageProcessor> mqProcessor = null;
	private List<Object> mqReadList = null;
	
	protected static boolean shutDown = false;
	
	final private static String version="mqth_1.0.0.1";
	public static String Version() {
		return version;
	}
	
	/*
	 No certs
	 	-m QMAP01 -h localhost -p 1501 -c NOCRED_WITHCHLAUTH -q LOAD.TEST.Q1 -msgs 100 -pt 1 -ct 1 -dec 1024 -func both -csp true -u MQmon01 -w Passw0rd -stat 10
	 
	 With certs
	 	-m QMAP01 -h localhost -p 1501 -c MQ.MONITOR.SVRCONN -q LOAD.TEST.Q1 -msgs 100 -pt 10 -ct 5 
	       -dec 1024 -func producer -csp true -u MQmon01 -w Passw0rd -stat 10 
	       -wait 5000 -cp 1148 -tls true 
	       -keystore "C:\ProgramData\IBM\MQ\qmgrs\QMAP01\ssl\qmap01jks.jks" 
	       -keypass Passw0rd 
	      -cipher TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256 -ibmcipher false
	      
         -m QMAP01 -h localhost -p 1501 -c MQ.MONITOR.SVRCONN -q LOAD.TEST.Q1 -msgs 100 -pt 2 -ct 5 -tp 10 
           -dec 1024 -func producer -csp true -u MQmon01 -w Passw0rd -stat 10 
           -wait 5000 -cp 1148 -tls true 
           -keystore "C:\ProgramData\IBM\MQ\qmgrs\QMAP01\ssl\qmap01jks.jks" 
           -keypass Passw0rd -cipher TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256 
           -ibmcipher false	      
           
       -v true -m QMAP01 -q LOAD.TEST.Q2  -h localhost -p 1501 -c NOCRED_WITHCHLAUTH -u MQmon01 -w Passw0rd -func consumer -log true -msgs 500 -pt 1 -msgsize 500
           
	*/
	
	public static void main(String[] args) throws MalformedObjectNameException, NullPointerException, InstanceNotFoundException, ReflectionException, InterruptedException {

		System.out.println(String.format("IBM MQ Test Harness verion:(%s)", Version()));
		System.out.println("------------------------------------------------------------------");
		
		int prc = ParseCommands(args);
		if (prc != 0) {
			System.out.println("Usage: [-m       QmgrName]                       # Queue manager name ");
			System.out.println("       [-q       Queue name]                     # The name of the queue to write/read messages from");
			System.out.println("       -----------------");	
			System.out.println("       Client Connection");			
			System.out.println("        [-h      Host name]                      # Host name - IP address, DNS, localhost");
			System.out.println("        [-p      Port]                           # Listening port of queue manager");
			System.out.println("        [-c      Channel]                        # Client channel name");
			System.out.println("       -----------------");			
			System.out.println("        [-u      UserId]                         # UserId to authenticate");
			System.out.println("        [-w      Password]                       # Password for user");
			System.out.println("        [-csp    use MQ CSP]                     # Default: true, true or false, Use MQ Security Parameter header");			
			System.out.println("       -----------------");			

			System.out.println("        [-tls    TLS connection]                 # true or false, default false - set if using a TLS channel");
			System.out.println("        [-keystore Name of the keystore]         # Name of application key store");
			System.out.println("        [-keypass Password for the key store]    # Password for keystore");
			System.out.println("        [-cipher Cipher on the certificate]      # Name of cipher spec");
			System.out.println("        **** Keystore and truststore should be the same **** ");
			System.out.println("        [-ibmcipher Use IBM Cipher]              # true or false, default false - use IBM ciphers when using IBMs JVM ");
			
			System.out.println("       -----------------");
			System.out.println("       [-func    Fuction]                        # Default : producer, The function to run; producer or consumer");			
			System.out.println("       [-pt      Number of producer threads]     # Default : 1, The number of producer threads to create");
			System.out.println("       [-ct      Number of consumer threads]     # Default : 1, The number of consumer threads to create");
			System.out.println("       [-file    Input file name]                # Default : null, Fully qualified file name containing message");
			System.out.println("       [-msgs    Number of messages per          # Default : 100, The number of messages to produce per thread");
			System.out.println("		            producer thread]");
			System.out.println("       [-msgsize                                 # Default : 0, The size of the message to produce");
			System.out.println("       [-dec     Bytes per kilobytes]            # Default : 1024, Bytes to use per kilo byte (1024 or 1000)");
			
			System.out.println("       [-per     Messages are persistent         # Default : false, Messages are written persistently to the queue");
			System.out.println("       [-stats   Statistics]                     # Default : 50, Display statistics info every x message");		
			System.out.println("       [-wait    Wait interval]                  # Default : 5000, The time in milli seconds to wait after MQ reads ");
			System.out.println("       [-sync    Syncpoint]                      # Default : 5, Messages are committed to the queue manager every 'x' message");
			
			System.out.println("       -----------------");	
			System.out.println("       [-cp      Code page]                      # Default : 437, Code page of data, default 437 (Ascii, 819) - (Edcidic, 1148)");
			System.out.println("       [-ts      Thread shutdown]                # Default : 600, The number of seconds to wait for each thread to shutdown");
			System.out.println("       [-tp      Thread pool]                    # Default : 5, The maximum number of threads in thread pool");
			System.out.println("       -----------------");	
			System.out.println("       [-sm      Split messages]                 # Default : false, true or false - Split messages"); 
			System.out.println("       [-sms     Split message size]             # Default : 1000, The number of bytes to split messages"); 
			System.out.println("       [-gm      Group messages]                 # Default : false, true or false - Group messages on the queue");
			System.out.println("       [-gms     Group message size]             # Default : 5, Group size");
			System.out.println("       -----------------");	
			System.out.println("       [-sleep   Sleep value between messages]   # Default : 0 - Sleep value between each message");
			System.out.println("       [-ramp    Ramp up throughput]             # Default : 0 - Ramp up message producers, every x ");
			System.out.println("       [-rampsleep Sleep time between threads]   # Default : 0 - Sleep between threads being created"); 
			System.out.println("       -----------------");	
			System.out.println("       [-log     Logging]                        # Default : false, true or false - Display logging");
			System.out.println("       [-sim     Simulation]                     # Default : false, true or false - Does everything, except write messages to the queue");
			System.out.println("       [-v       Verbose]                        # Default : false, true or false - Additional logging information");
			
			System.exit(prc);
		}
		
		if (Verbose()) {
			System.out.println(String.format("Version %s", Version()));
			System.out.println("-----------------------------------------------------------");
			System.out.println(String.format("Queue manager name          -m        : %s", QueueManagerName()));
			System.out.println(String.format("Queue name                  -q        : %s", QueueName()));
			System.out.println(String.format("Host name                   -h        : %s", HostName()));
			System.out.println(String.format("Port number                 -p        : %s", Port()));
			System.out.println(String.format("Channel name                -c        : %s", ChannelName()));
			System.out.println("-----------------------------------------------------------");
			System.out.println(String.format("UserId                      -u        : %s", UserId()));
			System.out.println(String.format("User password               -w        : ************"));
			System.out.println(String.format("MQCSP                       -csp      : %s", UseMQCSP()));
			System.out.println("-----------------------------------------------------------");
			System.out.println(String.format("Using a TLS connection      -tls      : %s", UseTLSConnection()));
			System.out.println(String.format("Keystore                    -keystore : %s", KeyStore()));
			System.out.println(String.format("User password               -keypass  : ************"));
			System.out.println(String.format("Cipher                      -cipher   : %s", Cipher()));
			System.out.println(String.format("Using IBM Ciphers           -ibmcipher: %s", IBMCipher()));			
			System.out.println("-----------------------------------------------------------");

			System.out.println(String.format("Function                    -func     : %s", Function()));
			System.out.println(String.format("Number of producer threads  -pt       : %s", NumberOfProducerThreads()));
			System.out.println(String.format("Number of consumer threads  -ct       : %s", NumberOfConsumerThreads()));
			System.out.println(String.format("Input file name             -file       : %s", InputFileName()));
			System.out.println(String.format("Number of messages          -msgs     : %s", NumberOfMessages()));
			System.out.println(String.format("Message size                -msgsize  : %s", MessageSize()));
			System.out.println(String.format("Kilobyte size               -dec      : %s", DecimalOrBinary()));
			System.out.println(String.format("Persistent                  -per      : %s", Persistent()));
			System.out.println(String.format("Show statistics every x     -stats    : %s", Stats()));			
			System.out.println(String.format("Wait interval               -wait     : %s", WaitInterval()));
			System.out.println(String.format("Commit MQ messages every x  -sync     : %s", Sync()));
			System.out.println("-----------------------------------------------------------");
			System.out.println(String.format("Code page                   -cp       : %s", CodePage()));			
			System.out.println(String.format("Thread shutdown in x secs   -ts       : %s", TaskShutDown()));
			System.out.println(String.format("Thread pool                 -tp       : %s", ThreadPool()));
			System.out.println("-----------------------------------------------------------");
			System.out.println(String.format("Split messages              -sm       : %s", SplitMessages()));
			System.out.println(String.format("Split message size          -sms      : %s", SplitMessageSize()));
			System.out.println(String.format("Group messages              -gm       : %s", GroupMessages()));
			System.out.println(String.format("Group messages every        -gms      : %s", GroupMessageSize()));
			System.out.println("-----------------------------------------------------------");
			System.out.println(String.format("Sleep between msg interval  -sleep    : %s", SleepInterval()));
			System.out.println(String.format("Rampup interval             -ramp     : %s", Distribute()));
			System.out.println(String.format("Rampup sleep interval       -rampsleep: %s", DistSleep()));
			System.out.println("-----------------------------------------------------------");			
			System.out.println(String.format("Logging enabled             -log      : %s", Log()));
			System.out.println(String.format("Simulation mode             -sim      : %s", Simulation()));
			System.out.println(String.format("Verbose                     -v        : %s", Verbose()));
			System.out.println("");
			System.out.println("");

		}
		// https://www.blazemeter.com/blog/advanced-load-testing-scenarios-jmeter-part-4-stepping-thread-group-and-concurrency-thread
		
		//-m QMAP01 -h localhost -p 1501 -c NOCRED_WITHCHLAUTH 
		//     -q LOAD.TEST.Q1 -u MQmon01 -w Passw0rd 
		//     -msgs 100 -pt 5 -ct 10 -dec 1024 -func both
		
		System.out.println(String.format("DRIVER: Processing - version: %s", Version()));
		
		pool = getExecutors();

		boolean prod = false;
		boolean consumer = false;
		//if ((Function().equals("PRODUCER")) || Function().equals("BOTH")) {

		if (Function().equals("PRODUCER")) {
			prod = true;
		}
		if (Function().equals("CONSUMER")) {
			consumer = true;
		}
		
		
		if (prod) {
			for (int producercount = 1; producercount <= NumberOfProducerThreads(); producercount++ ) {
			
				MessageProcessor producer = new MessageProcessor();		
				producer.HostName(HostName());
				producer.ChannelName(ChannelName());
				producer.Port(Port());
				producer.QueueManager(QueueManagerName());
				producer.Cipher(Cipher());
				producer.QueueName(QueueName());
				//producer.Msg(msg);
				producer.UserId(UserId());
				producer.Password(Password());
				producer.UseTLS(UseTLSConnection());
				producer.MQCSP(UseMQCSP());
				producer.MessageSize(MessageSize());			// kb
				producer.DecimalOrBinary(DecimalOrBinary());	// default to 1024
				if (Persistent()) {
					producer.Persistent(MQConstants.MQPER_PERSISTENT);
				} else {
					producer.Persistent(MQConstants.MQPER_NOT_PERSISTENT);					
				}
				producer.Function(Constants.PRODUCER);
				producer.NumberOfMessages(NumberOfMessages());
				producer.Simulation(Simulation());
				producer.CodePage(CodePage());
				producer.KeyStore(KeyStore());
				producer.StorePass(StorePass());
				producer.IBMCipher(IBMCipher());
				//
				producer.GroupMessages(GroupMessages());
				producer.GroupMessageSize(GroupMessageSize());
				producer.SplitMessages(SplitMessages());
				// 153600 - test message size
				producer.SplitMessageSize(SplitMessageSize());				// not used when SplitMessage is false 2,097,152
				producer.Statistics(Stats());			
				producer.Sync(Sync());
				producer.Verbose(Verbose());
				producer.Log(Log());
				producer.InputFileName(InputFileName());
				producer.Version(Version());
				producer.SleepInterval(SleepInterval());
				
				pool.submit(producer);
				//int running = ((ThreadPoolExecutor) pool).getActiveCount();
				if (Distribute() > 0) {
					if ((producercount % Distribute()) == 0) {
				//		System.out.println(String.format("%s threads have been created, waiting " ,producercount ));
						int running = ((ThreadPoolExecutor) pool).getActiveCount();
						System.out.println(String.format("PRODUCER: There are currently %d threads running " ,running ));
						Thread.sleep(DistSleep());
					}
				}
			}
		} 
				
		if (consumer) {					
			for (int consumercount = 1; consumercount <= NumberOfConsumerThreads(); consumercount++ ) {

				MessageProcessor con = new MessageProcessor();		
				con.HostName(HostName());
				con.ChannelName(ChannelName());
				con.Port(Port());
				con.QueueManager(QueueManagerName());
				con.Cipher(Cipher());
				con.QueueName(QueueName());
				//con.Msg(msg);
				con.UserId(UserId());
				con.Password(Password());
				con.UseTLS(UseTLSConnection());
				con.KeyStore(KeyStore());
				con.StorePass(StorePass());
				con.IBMCipher(IBMCipher());				
				con.MQCSP(UseMQCSP());
				con.SplitMessages(SplitMessages());
				// 153600 - test message size
				con.SplitMessageSize(SplitMessageSize());	// not used when SplitMessage is false
				con.MessageSize(MessageSize());			// kb
				con.DecimalOrBinary(DecimalOrBinary());	// default to 1024
				if (Persistent()) {
					con.Persistent(MQConstants.MQPER_PERSISTENT);
				} else {
					con.Persistent(MQConstants.MQPER_NOT_PERSISTENT);					
				}
				con.Function(Constants.CONSUMER);
				con.NumberOfMessages(NumberOfMessages());
				con.Simulation(Simulation());
				con.Statistics(Stats());
				con.WaitInterval(WaitInterval());
				con.GroupMessages(GroupMessages());
				con.GroupMessageSize(GroupMessageSize());
				con.Log(Log());
				con.Sync(Sync());
				con.Verbose(Verbose());
				con.Version(Version());
				
				pool.submit(con);
								
				if (Distribute() > 0) {
					if ((consumercount % Distribute()) == 0) {
						System.out.println(String.format("%s threads have been created, waiting " ,consumercount ));
						int running = ((ThreadPoolExecutor) pool).getActiveCount();
						System.out.println(String.format("There are currently %d running " ,running ));
						Thread.sleep(DistSleep());
					}
				}
				

			}
			
		}	// end of while loop
		
		
		ShutdownAndWaitTerminateion(pool);
		
		
		//memoryMetrics();
		//cpuUsage();
		//storage();
		
		//System.exit(0);
	}
	
	/*
	 * Parse commands
	 * 
	 */	
	private static int ParseCommands(String[] args) {
		
		int invalidArgument = 1;
		boolean invalidPos = false;
		String parg;
		
		KeyStore("**** NOT SET ****");
		Cipher("**** NOT SET ****");
		
		UseTLSConnection(false);
		UseMQCSP(false);
		Persistent(false);
		Function("PRODUCER");
		Simulation(false);
		NumberOfMessages(100);
		NumberOfProducerThreads(1);
		NumberOfConsumerThreads(1);
		DecimalOrBinary(1024);
		MessageSize(0);
		Stats(50);
		WaitInterval(5000);
		CodePage(437);
		IBMCipher(false);
		TaskShutDown(600);
		ThreadPool(5);
		Sync(5);
		SplitMessages(false);
		SplitMessageSize(524288);
		GroupMessages(GroupMessages());
		GroupMessageSize(5);
		Log(true);
		Verbose(false);
		SleepInterval(0);
		Distribute(0);
		DistSleep(0);
		
		int counter = 0;
		try {
			for (counter = 0; ((counter < args.length) && !invalidPos); counter++) {
				
				parg = args[counter];
				if (!parg.startsWith("-")) {
					invalidPos = true;
					invalidArgument = counter+1;
					continue;
				}
				
				switch (parg) {
				
				case "-m":
					counter++;
					QueueManagerName(args[counter]);
					invalidArgument = 0;
					break;
				case "-q":
					counter++;
					QueueName(args[counter]);
					invalidArgument = 0;
					break;					
				case "-h":
					counter++;
					HostName(args[counter]);
					break;
				case "-p":
					counter++;
					int p = Integer.parseInt(args[counter]);
					Port(p);
					break;
				case "-c":
					counter++;
					ChannelName(args[counter]);
					break;
				case "-u":
					counter++;
					UserId(args[counter]);
					break;
				case "-w":
					counter++;
					Password(args[counter]);
					break;	
				case "-csp":
					counter++;
					if (args[counter].toUpperCase().equals("TRUE")) {
						UseMQCSP(true);
					} else {
						UseMQCSP(false);
					}
					break;
					
				case "-tls":
					counter++;
					if (args[counter].toUpperCase().equals("TRUE")) {
						UseTLSConnection(true);
					} else {
						UseTLSConnection(false);
					}
					break;
				case "-keystore":
					counter++;
					KeyStore(args[counter]);
					break;				
				case "-keypass":
					counter++;
					StorePass(args[counter]);
					break;				
				case "-cipher":
					counter++;
					Cipher(args[counter].toUpperCase());
					break;					
				case "-ibmcipher":
					counter++;
					if (args[counter].toUpperCase().equals("TRUE")) {
						IBMCipher(true);
					} else {
						IBMCipher(false);
					}
					break;
										
				case "-func":
					counter++;
					if (args[counter].toUpperCase().equals("PRODUCER")) {
						Function("PRODUCER");
					} else if (args[counter].toUpperCase().equals("CONSUMER")) {
						Function("CONSUMER");						
					} else {
						invalidPos = true;
						invalidArgument = counter;						
					}
					break;

				case "-sim":
					counter++;
					if (args[counter].toUpperCase().equals("TRUE")) {
						Simulation(true);
					} else if (args[counter].toUpperCase().equals("FALSE")) {
						Simulation(false);
					} else {
						invalidPos = true;
						invalidArgument = counter;						
					}
					break;
				case "-file":
					counter++;
					InputFileName(args[counter]);
					break;
					
				case "-msgs":
					counter++;
					int msgs = Integer.parseInt(args[counter]);
					if (msgs <= 0) {
						msgs = 100;
					}
					NumberOfMessages(msgs);
					break;
				case "-per":
					counter++;
					if (args[counter].toUpperCase().trim().equals("TRUE")) {
						Persistent(true);						
					} else if (args[counter].toUpperCase().trim().equals("FALSE")) {
						Persistent(false);												
					} else {
						invalidPos = true;
						invalidArgument = counter;						
					}
					break;
					
				case "-pt":
					counter++;
					int pt = Integer.parseInt(args[counter]);
					if (pt <= 0) {
						pt = 1;
					}
					NumberOfProducerThreads(pt);
					break;				
				case "-ct":
					counter++;
					int ct = Integer.parseInt(args[counter]);
					if (ct <= 0) {
						ct = 1;
					}
					NumberOfConsumerThreads(ct);
					break;				
				case "-dec":
					counter++;
					int dec = Integer.parseInt(args[counter]);
					if ((dec != 1024) && (dec != 1000)) {
						dec = 1024;
					}
					DecimalOrBinary(dec);
					break;				
				case "-msgsize":
					counter++;
					int msize = Integer.parseInt(args[counter]);
					if (msize < 0) {
						msize = 0;
					}
					MessageSize(msize);
					break;				
				case "-stats":
					counter++;
					long stat = Long.parseLong(args[counter]);
					if (stat < 0) {
						stat = 50;
					}
					Stats(stat);
					break;				
				case "-wait":
					counter++;
					int wait = Integer.parseInt(args[counter]);
					if (wait <= 0) {
						wait = 5000;
					}
					WaitInterval(wait);
					break;				
				case "-cp":
					counter++;
					int codepage = Integer.parseInt(args[counter]);
					if (codepage <= 0) {
						codepage = 437;
					}
					CodePage(codepage);
					break;				
				case "-sync":					// MQ sync point
					counter++;
					int sync = Integer.parseInt(args[counter]);
					if (sync <= 0) {
						sync = 5;
					}
					Sync(sync);
					break;				

				case "-ts":					// task shutdown in seconds
					counter++;
					int taskshutdown = Integer.parseInt(args[counter]);
					if (taskshutdown <= 0) {
						taskshutdown = 600;		// 10 mins
					}
					TaskShutDown(taskshutdown);
					break;				
					
				case "-tp":					// thread pool
					counter++;
					int threadpool = Integer.parseInt(args[counter]);
					if (threadpool <= 0) {
						threadpool = 5;
					}
					ThreadPool(threadpool);
					break;				
				case "-sleep":					// Split message size
					counter++;
					int sleep = Integer.parseInt(args[counter]);
					if (sleep <= 0) {
						sleep = 0;
					}
					SleepInterval(sleep);
					break;				
				case "-ramp":					// ramp up
					counter++;
					int d = Integer.parseInt(args[counter]);
					if (d <= 0) {
						d = 0;
					}
					Distribute(d);
					break;				
				case "-rampsleep":				// Rampup sleep
					counter++;
					int ds = Integer.parseInt(args[counter]);
					if (ds <= 0) {
						ds = 0;
					}
					DistSleep(ds);
					break;				
					
				case "-gm":						// Group messages
					counter++;
					if (args[counter].toUpperCase().trim().equals("TRUE")) {
						GroupMessages(true);						
					} else if (args[counter].toUpperCase().trim().equals("FALSE")) {
						GroupMessages(false);												
					} else {
						invalidPos = true;
						invalidArgument = counter;						
					}
					break;
				case "-gms":					// Split message size
					counter++;
					int grp = Integer.parseInt(args[counter]);
					if (grp <= 0) {
						grp = 5;
					}
					GroupMessageSize(grp);
					break;				
				case "-sm":						// Split messages
					counter++;
					if (args[counter].toUpperCase().trim().equals("TRUE")) {
						SplitMessages(true);						
					} else if (args[counter].toUpperCase().trim().equals("FALSE")) {
						SplitMessages(false);												
					} else {
						invalidPos = true;
						invalidArgument = counter;						
					}
					break;
				case "-sms":					// Split message size
					counter++;
					int sms = Integer.parseInt(args[counter]);
					if (sms <= 0) {
						sms = 1000;
					}
					SplitMessageSize(sms);
					break;				
				case "-log":						// Group messages
					counter++;
					if (args[counter].toUpperCase().trim().equals("TRUE")) {
						Log(true);						
					} else if (args[counter].toUpperCase().trim().equals("FALSE")) {
						Log(false);												
					} else {
						invalidPos = true;
						invalidArgument = counter;						
					}
					break;
				case "-v":
					counter++;
					if (args[counter].toUpperCase().trim().equals("TRUE")) {
						Verbose(true);						
					} else if (args[counter].toUpperCase().trim().equals("FALSE")) {
						Verbose(false);												
					} else {
						invalidPos = true;
						invalidArgument = counter;						
					}
					break;
					
				default:
					invalidPos = true;
					invalidArgument = counter;
					break;
				
				
				}
			}
			if (invalidArgument != 0) {
				System.out.println(String.format("Argument %d has an error", invalidArgument));
			}
		} catch (Exception e) {
			invalidArgument = counter;			
		}
		return invalidArgument;
	}
	
	/*
	 * Shutdown and await termination
	 */
	private static void ShutdownAndWaitTerminateion(ExecutorService pool2) {

		pool.shutdown();
		try {
			if (!pool.awaitTermination(TaskShutDown(), TimeUnit.SECONDS)) {
				List<Runnable> x = pool.shutdownNow();
				System.out.println("WARNING: Thread pool timeout is too low");
				System.out.println(String.format("WARNING: The number of running threads is : %d, the number of awaiting threads is %d", ((ThreadPoolExecutor) pool).getActiveCount(), x.size()) );
				System.out.println(String.format("Increase the thread pool (-tp) or reduce the rampup value (-rampup) and/or wait interval (-wait) values "));
				System.out.println(String.format("Waiting for running threads to stop ...") );
				
			}
		} catch (InterruptedException ie) {
			pool.shutdownNow();
			Thread.currentThread().interrupt();
		}
	}

	/*
	 * Executor service
	 */
	private static ExecutorService getExecutors() {

		// https://examples.javacodegeeks.com/core-java/util/concurrent/threadfactory/java-util-concurrent-threadfactory-example/
		// ScheduledExecutorService
		ExecutorService ex = Executors.newFixedThreadPool(ThreadPool());
		//ThreadPoolExecutor ex = (ThreadPoolExecutor) Executors.newCachedThreadPool();
		
		//ScheduledExecutorService ex = Executors.newScheduledThreadPool(ThreadPool()); 
		//ex.setCorePoolSize(3);
		//ex.setMaximumPoolSize(4);
		//ex.setThreadNamePrefix("mq-events");
		//ex.initialize();
		return ex;
		
		
	}
	
	/*
	 * Stroage
	 */
	private static void Storage() {
		System.out.println("*********************************");
		File cDrive = new File("C:");
		System.out.println(String.format("Total space: %.2f GB",
		  (double)cDrive.getTotalSpace() /1073741824));
		System.out.println(String.format("Free space: %.2f GB", 
		  (double)cDrive.getFreeSpace() /1073741824));
		System.out.println(String.format("Usable space: %.2f GB", 
		  (double)cDrive.getUsableSpace() /1073741824));		
	}

	/*
	 * CPU
	 */
	private static void CPUUsage() {
		System.out.println("*********************************");

		ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
		for(Long threadID : threadMXBean.getAllThreadIds()) {
		    ThreadInfo info = threadMXBean.getThreadInfo(threadID);
		    System.out.println("Thread name: " + info.getThreadName());
		    System.out.println("Thread State: " + info.getThreadState());
		    System.out.println(String.format("CPU time: %s ns", 
		      threadMXBean.getThreadCpuTime(threadID)));
		  }		
	}

	/*
	 * Memory
	 */
	private static void MemoryMetrics() {
		System.out.println("*********************************");
		
		MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
		System.out.println(String.format("Initial memory: %.2f GB", 
			  (double)memoryMXBean.getHeapMemoryUsage().getInit() /1073741824));
		System.out.println(String.format("Used heap memory: %.2f GB", 
			  (double)memoryMXBean.getHeapMemoryUsage().getUsed() /1073741824));
		System.out.println(String.format("Max heap memory: %.2f GB", 
			  (double)memoryMXBean.getHeapMemoryUsage().getMax() /1073741824));
		System.out.println(String.format("Committed memory: %.2f GB", 
			  (double)memoryMXBean.getHeapMemoryUsage().getCommitted() /1073741824));		
		
		
	}
}
