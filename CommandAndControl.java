import java.net.DatagramSocket;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Timer;

import tcdIO.*;

/**
 *
 * Worker class
 * 
 * An instance accepts user input
 *
 */
public class CommandAndControl extends Node {
	static int commandAndControlSRCPort;
	static final String DEFAULT_DST_NODE = "localhost";
	int dstPort;
	int srcPort;
	static InetSocketAddress dstAddress;
	static String commandAndControlName;
	int nextSentPackNum;		//keeping track of sender window
	int nextExpectedPackNum;	//keeping track of receiver window
	int goBackNWindowSize;		//making sure that goBackNWindowSize <= (2^m)-1
	boolean recognizedByBroker;	//At the start, when C&C needs to contact Broker
	String idNumber;			//Broker's ID number for this C&C
	ArrayList<SNDContent> currentJobs;
	ArrayList<SNDContent> jobsPendingDistribution;
	static Timer[] goBackNWindow = new Timer[16]; //Window will be of size 15, but we make array of size 16
												  //and an int value goBackNWindowSize, in order to make
										   		  //array accesses more straight forward.
	static SNDContent[] goBackNWindowContent = new SNDContent[16]; 
		// had to make a string array that works in conjunction with goBackNWindow
		// as I needed an easy way to access the packet contents.
	Terminal terminal;
	int jobID;
	boolean waitForNumWorkers;

	/**
	 * Constructor
	 * 
	 * Attempts to create socket at given port and create an InetSocketAddress for
	 * the destinations
	 */
	CommandAndControl(Terminal terminal, String dstHost, int dstPort, int srcPort) {
		try {
			this.terminal = terminal;
			commandAndControlSRCPort = srcPort; // this worker's source port (as int)
			this.dstPort = dstPort;
			this.srcPort = srcPort;
			recognizedByBroker = false;
			dstAddress = new InetSocketAddress(dstHost, dstPort); // This will always be Broker dstAddress for workers
			socket = new DatagramSocket(srcPort); // socket for this worker (from Node class)
			currentJobs = new ArrayList<SNDContent>(); // list of all currentJobs, this is an array list so we can easily add and remove from joblist
			jobsPendingDistribution = new ArrayList<SNDContent>();
			nextSentPackNum = 0;	 // set to -1 at the start, so that worker cannot do anything other than subscribe when this program start
									 // due to this being used as a boolean in start(). Will be set to 0 when worker volunteering is ACKed by Broker.
			nextExpectedPackNum = 0;
			goBackNWindowSize = 0;	
			jobID = 0;
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
				processBrokerInstructions(content);
			else
			{
				ACKContent resendACK = new ACKContent(nextExpectedPackNum);
				sendACK(resendACK);
			}
		}
		this.notify();
	}

	private SNDContent createNewJob(String jobContent, String numWorkers)
	{
		// When the C&C wants to create a new job, we just need the String of the job content,
		// and the number of workers they want to do this job. From there, we can construct 
		// a PacketContent with this information.
		int number = Integer.parseInt(numWorkers);
		
		if(number < 10 && number >= 0)
			numWorkers = "0" + numWorkers;
		SNDContent newJob = new SNDContent("SNDJOB00" + idNumber + numWorkers 
											+ getJobIDToString() + jobContent);
		
		jobID++;
		if(jobID > 9999) jobID = 0;
		
		return newJob;
	}
	
	private void processBrokerInstructions(SNDContent content) {
		
		// Find the relevant Job first
		SNDContent relevantJob = null;
		for(SNDContent iterationJob : currentJobs)
		{
			if (iterationJob.getOriginatingCAndC().equals(content.getOriginatingCAndC())
					&& iterationJob.getJobID().equals(content.getJobID()))
			{
				relevantJob = iterationJob;
				break;
			}
		}
		
		if (content.getContentType().equals("CMP")) 
		{
			// If the Broker is notifying us that a worker has completed a task...
			
			// Decrements the amount of tasks required until this job is done
			relevantJob.markOneTaskDone();
			terminal.print("\n" + (relevantJob.getNumWorkersForJob() - relevantJob.getNumTasksUntilJobComplete()) + " of " 
							+ relevantJob.getNumWorkersForJob() + " tasks are done for job " 
							+ relevantJob.getPacketContent() + " (Job ID " + relevantJob.getJobID() + ")");
			
			
			if(relevantJob.getNumTasksUntilJobComplete() <= 0)
			{
				terminal.print("\nJOB: " + relevantJob.getJobID() + " is 100% complete!");
				currentJobs.remove(relevantJob);
			}			
		}
		else if (content.getContentType().equals("JOB"))
		{
			// If Broker is telling us how many workers have been designated this job...
			relevantJob.resetNumWorkersForJob(content.getNumWorkersForJob());
			if(relevantJob.getNumWorkersForJob() == 1)
				terminal.print("1 worker is working on job " + relevantJob.getPacketContent() 
								+ " (Job ID " + relevantJob.getJobID() + ")");
			else
				terminal.print(relevantJob.getNumWorkersForJob() + " workers are working on job " 
								+ relevantJob.getPacketContent() + " (Job ID " + relevantJob.getJobID() + ")");		
			
			waitForNumWorkers = false;
		}
		else if (content.getContentType().equals("CRI"))
		{
			idNumber = content.getOriginatingCAndC();
			terminal.print("Successfully connected to Broker (PORT: " + dstPort + ")");
			recognizedByBroker = true;
		}
		
		// Send ACK in increment nextExpectedPackNum
		ACKContent ackPacket = new ACKContent(nextExpectedPackNum + 1);
		sendACK(ackPacket);
		nextExpectedPackNum = (nextExpectedPackNum + 1) % 16;
	}
	
	private void sendPacket(SNDContent packetContent){
		
		// One element on the goBackN window array must always be null.
		while(goBackNWindowSize >= 15)  
		{
			try { Thread.sleep(500); 
			} catch (InterruptedException e) { e.printStackTrace(); }
		}
		
		// reset the PacketNumber of the Packet so it matches up with the 
		// Broker's next expected Packet
		packetContent.resetPacketNumber(nextSentPackNum);
		
		// turn into DatagramPacket and create destination for packet (this will be the Broker socket)
		DatagramPacket packetToSend = packetContent.toDatagramPacket(); 
		packetToSend.setSocketAddress(dstAddress);
		
		Timer packetTimer = new Timer(); // start new timer
		TimerFlowControl ARQ = new TimerFlowControl(socket, packetToSend);
		packetTimer.schedule(ARQ, 0, 3000);
		
		// Make 2 arrays where any given i on both arrays are for the matching PacketContent and timer.
		goBackNWindow[nextSentPackNum] = packetTimer;
		goBackNWindowContent[nextSentPackNum] = packetContent; 
		nextSentPackNum = (nextSentPackNum + 1) % 16;	//iterate nextSentPackNum
		goBackNWindowSize++;
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
		// if a SND is sent from our worker with packet number "01", then the ACK from Broker will have a 
		// packet number of 02, so we need to decrement first to find the right packet.
		int iterationACK = (15 + latestACK) % 16; 
		
		Timer packetTimerIteration = goBackNWindow[iterationACK]; // find last timer placed in window
		SNDContent packetContentIteration = goBackNWindowContent[iterationACK];
		while (packetTimerIteration != null)
		{
			packetTimerIteration.cancel(); // cancel timer and nullify it on array
			goBackNWindow[iterationACK] = null;

			/*
			 * Dealing with different types of ACKs and how they affect the worker. ACKs
			 * with the Content Type "PAA" are pacify ACKs, meaning that a packet from this
			 * node has been delivered successfully, but the returning ACK was lost. Since
			 * the Worker itself remembers the content of the packets it delivered.
			 */

			if (packetContentIteration.getContentType().equals("JOB"))			
			{
				terminal.print("Job " + packetContentIteration.getPacketContent() + " successfully sent to Broker!\n");
			}

			// nullify element on the array, so there is always at least one null element
			goBackNWindowContent[iterationACK] = null;

			// equation for cycling backwards through a cyclical array of size 16
			waitForNumWorkers = packetContentIteration.getContentType().equals("JOB");
			iterationACK = (15 + iterationACK) % 16; 
			packetTimerIteration = goBackNWindow[iterationACK];
			packetContentIteration = goBackNWindowContent[iterationACK];
			goBackNWindowSize--;
		}
	}
	
	private String getJobIDToString()
	{
		if(jobID < 10) 
			return "000" + jobID;
		if(jobID < 100)
			return "00" + jobID;
		if(jobID < 1000)
			return "0" + jobID;
		
		return "" + jobID;
	}

	public synchronized void start() throws Exception {
		terminal.println("C&C (PORT " + srcPort + "): \n");
		terminal.println("Connecting to Broker...");
		SNDContent notifyBroker = new SNDContent("SNDCRI0000000000" + '\u0003');
		sendPacket(notifyBroker);
		
		while(idNumber == null) {
			this.wait();
		}

		while (recognizedByBroker) {
			while(waitForNumWorkers)
				this.wait();
			
			String newJobDescription = "";
			String numWorkersForJobString = "0";
			int numWorkersForJob = 0;
			while (newJobDescription.length() <= 2) 
			{
				newJobDescription = terminal.readString("\nEnter Job Description:\n").toString() + '\u0003';
				if (newJobDescription.length() <= 2)
					terminal.print("\nSorry, job description too short.");
			}
			while (numWorkersForJob == 0 || numWorkersForJob > 99 || numWorkersForJob < -1) 
			{
				numWorkersForJobString = terminal.readString("\n How many workers do you want to "
						+ "do this job? (-1 for all, 1+ for number. 0, <-1 and >99 are invalid):\n");
				numWorkersForJob = Integer.parseInt(numWorkersForJobString);
			}

			SNDContent newJob = createNewJob(newJobDescription, numWorkersForJobString);
			currentJobs.add(newJob);
			sendPacket(newJob);
			this.wait();
		}
	}
	
	public static void main(String[] args) { 
		// The arguments for this node are expected to be the port number of the node.
		int portNumber = Integer.parseInt(args[0]);
		try 
		{
			Terminal terminal = new Terminal("C&C");
			CommandAndControl thisCAndC = new CommandAndControl(terminal, DEFAULT_DST_NODE, 50000, portNumber);
			thisCAndC.start();
		} catch (java.lang.Exception e) { e.printStackTrace(); }
	}
}
