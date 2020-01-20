import java.net.DatagramSocket;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import tcdIO.*;

/**
 *
 * Worker class
 * 
 * An instance accepts user input
 *
 */
public class Worker extends Node {
	static int workerSRCPort;
	static final String DEFAULT_DST_NODE = "localhost";
	static InetSocketAddress dstAddress;
	static boolean availableForWork;
	static boolean currentlyChangingSubscription;
	static String workerName;
	static int nextSentPackNum;		//keeping track of sender window
	int nextExpectedPackNum;	//keeping track of receiver window
	static int goBackNWindowSize;		//making sure that goBackNWindowSize <= (2^m)-1
	static ArrayList<SNDContent> currentJobs;
	static Timer[] goBackNWindow = new Timer[16]; //Window will be of size 15, but we make array of size 16
												  //and an int value goBackNWindowSize, in order to make
										   		  //array accesses more straight forward.
	static SNDContent[] goBackNWindowContent = new SNDContent[16]; 
		 // had to make a string array that works in conjunction with goBackNWindow
		// as I needed an easy way to access the packet contents.
	static Terminal terminal;
	Timer doJobTimer;
	WorkerDoJob doJobClass;

	/**
	 * Constructor
	 * 
	 * Attempts to create socket at given port and create an InetSocketAddress for
	 * the destinations
	 */
	Worker(Terminal terminal, String dstHost, int dstPort, int srcPort) {
		try {
			Worker.terminal = terminal;
			workerSRCPort = srcPort; // this worker's source port (as int)
			dstAddress = new InetSocketAddress(dstHost, dstPort); // This will always be Broker dstAddress for workers
			socket = new DatagramSocket(srcPort); // socket for this worker (from Node class)
			currentJobs = new ArrayList<SNDContent>(); // list of all currentJobs, this is an array list 
													   // so we can easily add and remove from joblist
			availableForWork = false;
			currentlyChangingSubscription = false;
			nextSentPackNum = 0;
			nextExpectedPackNum = 0;
			goBackNWindowSize = 0;	
			doJobTimer = new Timer();
			doJobClass = new WorkerDoJob();
			// worker will do a job every so often, randomly generated for each worker
			// to be between 10 and 15 seconds.
			doJobTimer.schedule(doJobClass, 0, new Random().nextInt(5000) + 10000);
			listener.go();
		} 
		catch (java.lang.Exception e) { e.printStackTrace(); }
	}

	// Assume that incoming packets contain a String, create PacketContent which
	// sets the variables.

	public synchronized void onReceipt(DatagramPacket packet) {
		
		ACKContent potentialACK = new ACKContent(packet);
		if(potentialACK.isValidACK())
			acceptACKs(potentialACK.getACKNumber());
		else
		{	
			// It is an information packet
			SNDContent content = new SNDContent(packet);
			if(content.isValid() && content.getPacketNumber() == nextExpectedPackNum)
			{
				if (content.getContentType().equals("JOB")) // if new Job Listing
				{
					// add job to currentJobs ArrayList
					currentJobs.add(content); 
					// immediately print out job listing for worker
					terminal.println("\nNEW JOB: " + content.getPacketContent() 
									+ " (Job ID " + content.getJobID() + ")"); 
					ACKContent jobACK = new ACKContent(nextExpectedPackNum);
					sendACK(jobACK);
				}
				nextExpectedPackNum = (nextExpectedPackNum + 1 ) % 16;
			}
			else
			{
				ACKContent resendACK = new ACKContent(nextExpectedPackNum);
				sendACK(resendACK);
			}
		}
		this.notify();
	}
	
	private static void changeAvailability() {

		SNDContent availabilityNotification = null;
		if (!availableForWork) // if subscribing
		{
			terminal.println("\nSubscribing to BROKER...");
			availabilityNotification = new SNDContent("SNDWVA0000000000" + workerName + '\u0003');
		} 
		else if (availableForWork) // if unsubscribing
		{
			terminal.println("\nUnsubscribing from BROKER...");
			availabilityNotification = new SNDContent("SNDWVU0000000000" + workerName + '\u0003');
			currentJobs.clear();
		}
		
		terminal.println("\nNotifying Broker...");
		currentlyChangingSubscription = true;
		availableForWork = !availableForWork;
		sendPacket(availabilityNotification);
	}
	
	private static void sendPacket(SNDContent packetContent){		
		// One element on the goBackN window array must always be null.
		while(goBackNWindowSize >= 15)  
		{
			try { Thread.sleep(500); 
			} catch (InterruptedException e) { e.printStackTrace(); }
		}
		
		// reset the PacketNumber of the Packet so it matches up with the 
		// Broker's next expected Packet
		packetContent.resetPacketNumber(nextSentPackNum);
		
		// turn into DatagramPacket and set destination for packet
		DatagramPacket packetToSend = packetContent.toDatagramPacket(); 
		packetToSend.setSocketAddress(dstAddress);
				
		 // start new timer for Go-Back-N
		Timer packetTimer = new Timer();
		TimerFlowControl ARQ = new TimerFlowControl(socket, packetToSend);
		packetTimer.schedule(ARQ, 0, 3000);
		
		// Make 2 arrays where any given i on both arrays are for the 
		// matching PacketContent and timer.
		goBackNWindow[nextSentPackNum] = packetTimer;
		goBackNWindowContent[nextSentPackNum] = packetContent; 
		goBackNWindowSize++;
		
		//iterate nextSentPackNum
		nextSentPackNum = (nextSentPackNum + 1) % 16;	
	}
	
	private void sendACK(ACKContent ackContent)
	{
		// ACKs are typically only sent once, so we do not have to designate a timer
		// to them, and we do not have to add them to our Go-Back-N window.

		DatagramPacket ackPacket = ackContent.toDatagramPacket(); 
		ackPacket.setSocketAddress(dstAddress); 
		try {
			socket.send(ackPacket);
		} catch (IOException e) { e.printStackTrace(); }; 
	}
	
	
	/*
	 *  If packet 1, 2, 3 are sent, and only an ACK for 3 is received, this means
	 *  that the receiver got packet 1, 2, and 3, but the ACKs for 1 and 2 were lost.
	 *  This method goes through the goBackNWindow and cancels the timers for all the 
	 *  previous packets in the window. Size the window will be of size (2^m)-1 (15), 
	 *  which means that there will always been an element on our size 16 array that
	 *  will be null, and as a result there is no risk of cancelling a packet that 
	 *  has not actually been ACKed.
	 */
	
	private void acceptACKs(int latestACK) {	
		int iterationACK = (15 + latestACK) % 16; 
		
		Timer packetTimerIteration = goBackNWindow[iterationACK]; // find last timer placed in window
		SNDContent packetContentIteration = goBackNWindowContent[iterationACK];
		while (packetTimerIteration != null)
		{
			packetTimerIteration.cancel(); // cancel timer and nullify it on array
			goBackNWindow[iterationACK] = null;

			if(currentlyChangingSubscription)
			{
				if (packetContentIteration.getContentType().equals("WVA")) 
				{
					terminal.print("\nSuccessfully subscribed!");
					currentlyChangingSubscription = false;
					
				}
				else if (packetContentIteration.getContentType().equals("WVU")) 
				{					
					terminal.println("\nRemoving your joblist...");
					for(int i = 0; i < goBackNWindowSize - 1; i++)
					{
						goBackNWindow[i].cancel();
						goBackNWindow[i] = null;
						goBackNWindowContent = null;
					}
					nextSentPackNum = 0;
					terminal.print("\nSuccessfully unsubscribed!");
				}	
				currentlyChangingSubscription = false;
				nextExpectedPackNum = 0;
			}
			else if (packetContentIteration.getContentType().equals("CMP")) // if ACK of Job Complete
			{
				terminal.print("\nBroker has accepted our work for Job ID " + packetContentIteration.getJobID());
			}

			goBackNWindowContent[iterationACK] = null;
			
			iterationACK = (15 + iterationACK) % 16; 
			packetTimerIteration = goBackNWindow[iterationACK];
			packetContentIteration = goBackNWindowContent[iterationACK];
			goBackNWindowSize--;
		}
	}
	
	// For a worker to actually do a job, I created a class WorkerDoJob, which was required as a
	// timer-task.  All this class does it call this Worker's doAJob method. The doAJob method 
	// will generate a random number every time it's called. This number will determine if the worker
	// decides to quit (currently 1 in 20 chance of quitting any time this method is called, this can 
	// be increased or reduced by changing the nextInt() value). If they are not quitting, then a job
	// from their current job list is picked randomly. We create a new packet, with all the information
	// from this chosen job, but change it to "CMP" (complete) and notify the broker. This job is then
	// removed from the job list. 
	public class WorkerDoJob extends TimerTask {
		@Override
		public void run() {
				Worker.doAJob();
		}
	}	
	
	public static void doAJob()
	{
		if(availableForWork && !currentlyChangingSubscription)
		{
			if(new Random().nextInt(100) == 15)
				changeAvailability();
			else if(currentJobs.size() > 0)
			{
				SNDContent jobChosen = currentJobs.get(new Random().nextInt(currentJobs.size()));
				SNDContent jobCompleted = new SNDContent(jobChosen.toString());
				jobCompleted.resetContentType("CMP");
				sendPacket(jobCompleted);
				terminal.print("\nJOB COMPLETE: " + jobChosen.getPacketContent() + " (Job ID " + jobChosen.getJobID());
				currentJobs.remove(jobChosen);
			}
		}
	}
	
	public synchronized void start() throws Exception {
		while(true) 
		{
			if(!availableForWork && !currentlyChangingSubscription && workerName == null)
			{
				workerName = terminal.readString("What is your name? "
					+ "\n(Entering name will volunteer you for work):\n").toString() + '\u0003';
				changeAvailability();
			}
			else if(!availableForWork && !currentlyChangingSubscription)
			{
				String userResubscribe = "";
				while(!userResubscribe.equalsIgnoreCase("y"))
				{
					userResubscribe = terminal.readString("\nDo you want to resubscribe? (y/n):\n").toString();
				}
				changeAvailability();
			}
		this.wait();
		}
	}
	
	public static void main(String[] args) {
		// The arguments for this node are expected to be the port number of the node.												
		int portNumber = Integer.parseInt(args[0]);
		try 
		{
			Terminal terminal1 = new Terminal("Worker");
			Worker thisWorker1 = new Worker(terminal1, DEFAULT_DST_NODE, 50000, portNumber);
			thisWorker1.start();
		} catch (java.lang.Exception e) { e.printStackTrace(); }
	}
}
	
	
