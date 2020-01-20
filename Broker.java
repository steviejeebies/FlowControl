import java.net.DatagramSocket;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Random;
import java.util.Timer;

import tcdIO.*;

/**
 *
 * Broker class
 * 
 * An instance accepts user input 
 *
 */
public class Broker extends Node {
	static final String DEFAULT_DST_NODE = "localhost";	
	static final int BROKER_SRC_PORT = 50000;
	static ArrayList<NodeData> connectedWorkers = new ArrayList<NodeData>();	//for multiple workers and multiple C&Cs
	static ArrayList<NodeData> connectedCAndCs = new ArrayList<NodeData>();
	static ArrayList<SNDContent> allJobs = new ArrayList<SNDContent>();
	static ArrayList<SNDContent> jobsPendingDistribution = new ArrayList<SNDContent>();
	
	Terminal terminal;
	InetSocketAddress dstAddress;
	
	/**
	 * Constructor
	 * 	 
	 * Attempts to create socket at given port and create an InetSocketAddress for the destinations
	 */
	Broker(Terminal terminal, String dstHost, int srcPort) {
		try {
			this.terminal = terminal;
			socket= new DatagramSocket(srcPort);
			listener.go();
		}
		catch(java.lang.Exception e) { e.printStackTrace(); }
	}

	/**
	 * Assume that incoming packets contain a String and print the string.
	 */
	public synchronized void onReceipt(DatagramPacket receivedPacket) {
		// First, we need to find the relevant Node
		int portDeliveredFrom = receivedPacket.getPort();
		NodeData nodeDeliveredFrom = findNode(portDeliveredFrom);
		
		// If nodeDeliveredFrom is null, we haven't communicated to it before, 
		// and have to register it.
		if (nodeDeliveredFrom == null) 
			registerNewNode(receivedPacket);
		else
		{
			//Check if it is an ACK		
			ACKContent potentialACK = new ACKContent(receivedPacket);
			if(potentialACK.isValidACK())
				acceptACKs(nodeDeliveredFrom, potentialACK.getACKNumber());
			else
			{	
				// It is an information packet
				SNDContent content = new SNDContent(receivedPacket);
				
				if(content.isValid())
				{
					if (content.getPacketNumber() == nodeDeliveredFrom.getNextExpectedPackNum()) 
					{
						if (nodeDeliveredFrom.getNodeType().equalsIgnoreCase("Worker")) 
							processWorkerInstruction(nodeDeliveredFrom, content);
						else if (nodeDeliveredFrom.getNodeType().equals("C&C")) 
							processCAndCInstruction(nodeDeliveredFrom, content);
						
						nodeDeliveredFrom.incrementNextExpectedPackNum();
					}
					else
					{
						ACKContent resendACK = new ACKContent(nodeDeliveredFrom.getNextExpectedPackNum());
						sendACK(nodeDeliveredFrom.getDstAddress(), resendACK);
					}
				}
			}
		}
		this.notify();
	}
	
	private void acceptACKs(NodeData nodeDeliveredFrom, int latestACK) {
		// Decrement ACK Packet Number by 1
		int iterationACK = (15 + latestACK) % 16;

		// find last timer placed in Go-Back-N Window
		Timer packetTimerIteration = nodeDeliveredFrom.goBackNWindow[iterationACK];
		SNDContent packetContentIteration = nodeDeliveredFrom.goBackNWindowContent[iterationACK];
		
		// until we reach the null element...
		while (packetTimerIteration != null) 
		{
			// cancel timer and nullify it on array
			packetTimerIteration.cancel(); 
			nodeDeliveredFrom.goBackNWindow[iterationACK] = null;
			
			// specifically in the case of worker
			if(nodeDeliveredFrom.getNodeType().equals("Worker"))
			{
				// We must find the ACKed job in the Broker's list of all jobs, then add it to the
				// NodeData of that worker's currentJob list, in order for the Broker to keep track
				// of all the jobs that it has successfully designated to a given worker.
				SNDContent relevantJob = null;
				for(SNDContent iterationJob : allJobs)
				{
					if (iterationJob.getOriginatingCAndC().equals(packetContentIteration.getOriginatingCAndC())
							&& iterationJob.getJobID().equals(packetContentIteration.getJobID()))
					{
						relevantJob = iterationJob;
						break;
					}
				}
				
				// We only add the job to the NodeData's list of designated jobs after
				// we get the ACK back from the worker
				if(relevantJob != null)
				{
					nodeDeliveredFrom.designatedJobs.add(relevantJob);
					
					terminal.print("\nWorker " + nodeDeliveredFrom.getNodeName() + " has accepted " +
							"Job ID " + relevantJob.getJobID() + " from C&C" + relevantJob.getOriginatingCAndC());
				}
			}
			else if(nodeDeliveredFrom.getNodeType().equals("C&C"))
			{
				if(packetContentIteration.getContentType().equals("CMP"))
				{
					terminal.print("\n" + nodeDeliveredFrom.getNodeName() + " has been notified about task "
						+ packetContentIteration.getPacketContent() + " (Job ID " 
						+ packetContentIteration.getJobID() + ")");
				}
				else if (packetContentIteration.getContentType().equals("CRI"))
				{
					terminal.print("\n" + nodeDeliveredFrom.getNodeName() + " has accepted ID Number "
							+ nodeDeliveredFrom.getIDNumber());
				}
			}
				
			// Finally, we can nullify goBackNWindowContent, so that it always
			// lines up with goBackNWindow
			nodeDeliveredFrom.goBackNWindowContent[iterationACK] = null;
			
			// Continue the while loop, so we can also ACK all previous messages from Broker
			// to Worker that haven't yet been ACKed.
			packetTimerIteration = nodeDeliveredFrom.goBackNWindow[iterationACK];
			packetContentIteration = nodeDeliveredFrom.goBackNWindowContent[iterationACK];
			nodeDeliveredFrom.goBackNWindowSize--;
			
			// equation for cycling backwards through a cyclical array of size 16
			iterationACK = (15 + iterationACK) % 16;
		}
	}
	
	private void registerNewNode(DatagramPacket receivedPacket)
	{
		SNDContent content = new SNDContent(receivedPacket);
		
		// If the Broker doesn't recognise this node, we have to register and
		// categorise it
		if(content.isValid())
		{
			if (content.getContentType().equalsIgnoreCase("WVA")) 
			{
				// If packet is a request from a Worker to volunteer...
	
				// create a new NodeData and add it to the list of connected nodes
				NodeData newWorker = new NodeData(socket, receivedPacket, "Worker");
				connectedWorkers.add(newWorker);
				newWorker = connectedWorkers.get(connectedWorkers.size()-1);
				newWorker.incrementNextExpectedPackNum();
				
				terminal.println("\nNEW VOLUNTEER: " + newWorker.getNodeName() 
					+ " (PORT " + newWorker.getDSTPort() + ") \nCurrent Available Workers = " 
					+ connectedWorkers.size());
				
				// Send ACK to worker
				ACKContent workerACK = new ACKContent(newWorker.getNextExpectedPackNum());
				sendACK(newWorker.getDstAddress(), workerACK);
			} 
			else if (content.getContentType().equalsIgnoreCase("CRI")) 
			{
				// If packet is a request from a C&C to get the ID number that was designated to
				// it by the Broker...
				
				// Create a new NodeData and add it to the list of connected nodes
				NodeData newCAndC = new NodeData(socket, receivedPacket, "C&C", connectedCAndCs.size() + 1);
				connectedCAndCs.add(newCAndC);
				newCAndC = connectedCAndCs.get(connectedCAndCs.size()-1);
				newCAndC.incrementNextExpectedPackNum();
				
				// Send ACK
				ACKContent CAndCACK = new ACKContent(newCAndC.getNextExpectedPackNum());
				sendACK(newCAndC.getDstAddress(), CAndCACK);
				
				// Create a InfoPacket that will contain the designated ID for the C&C
				SNDContent idForCAndCPacket = new SNDContent("SNDCRI0000000000" + '\u0003');
				idForCAndCPacket.setCAndCID(newCAndC.getIDNumber());
				
				// Notify Broker in terminal about new C&C
				terminal.println("\nNEW C&C: " + newCAndC.getNodeName() 
								+ " (PORT " + newCAndC.getDSTPort() + ")");
				
				// Return the ID that the Broker has assigned to the C&C with ACK
				newCAndC.sendPacket(idForCAndCPacket);
			}
		}
	}
	
	
	public NodeData findNode(int portDeliveredFrom)
	{
		// First we iterate through workers to find if the port matches any of them
		NodeData nodeDeliveredFrom = null;
		for(NodeData iterationNode : connectedWorkers)	
		{
			if(iterationNode.getDSTPort() == portDeliveredFrom)
			{
				nodeDeliveredFrom = iterationNode;
				break;
			}
		}

		// if nodeDeliveredFrom is null after checking the workers, then it may be a C&C, 
		// so we check the connectedCAndCs ArrayList.
		if(nodeDeliveredFrom == null)	
		{
			for(NodeData iterationNode : connectedCAndCs)	//for finding the relevant worker node
			{
				if(iterationNode.getDSTPort() == portDeliveredFrom)
				{
					nodeDeliveredFrom = iterationNode;
					break;
				}
			}
		}
		return nodeDeliveredFrom;
	}
	
	private void processWorkerInstruction(NodeData nodeDeliveredFrom, SNDContent content)
	{
		if (content.getContentType().equals("CMP")) 
		{
			// if Worker has completed a job

			// First we have to find the C&C that requested the job
			NodeData relevantCAndC = null;

			for (NodeData iterationCAndC : connectedCAndCs) 
			{
				if (content.getOriginatingCAndC().equals(iterationCAndC.getIDNumber())) 
				{
					relevantCAndC = iterationCAndC;
					break;
				}
			}
			
			// Then, if the C&C has requested a job to be done 1+ times, we have
			// to decrement the amount of times that the job is left to do. If, after
			// this, numUntilJobComplete is 0, then we remove the job from the 
			// allJobs ArrayList, as it is completely done. We also have to remove
			// it from the workers NodeData's ArrayList of designatedJobs, which is done 
			// by calling the jobComplete() method.
			
			SNDContent relevantJob = null;
			for(SNDContent iterationJob : allJobs)
			{
				if(content.getJobID().equals(iterationJob.getJobID()) 
						&& content.getOriginatingCAndC().equals(iterationJob.getOriginatingCAndC()))
				{
					// if Job ID and originating C&C match
					relevantJob = iterationJob;
					break;
				}
			}
			if(relevantJob != null)
			{
				terminal.print("\nWorker " + nodeDeliveredFrom.getNodeName() + " has completed task "
						+ relevantJob.getPacketContent() + " (Job ID " + relevantJob.getJobID() + ")");
				
				// Decrements the amount of tasks required until this job is done
				// (this depends on how many workers the C&C asked to do the job)
				relevantJob.markOneTaskDone();
				
				// If the above for a given job has reached 0, then we must mark
				// the job as completely done for that NodeData, and also remove 
				// from the Broker's allJobs ArrayList.
				if(relevantJob.getNumTasksUntilJobComplete() <= 0) 
				{
					nodeDeliveredFrom.jobFullyComplete(relevantJob);
					allJobs.remove(relevantJob);
					terminal.print("\nJOB COMPLETE:" + relevantJob.getPacketContent() + " (Job ID "
							+ relevantJob.getJobID() + ")");
					terminal.print("\nNumber of Jobs left = " + allJobs.size());
				}
				
				// Then we have to create a packet in order to tell the C&C that 
				// the job is complete
				SNDContent completedJobToCAndC = new SNDContent(
								"SNDCMP00" + relevantCAndC.getIDNumber() 
								+ relevantJob.getNumWorkersForJobToString() 
								+ relevantJob.getJobID() + relevantJob.getPacketContent() + '\u0003');
				relevantCAndC.sendPacket(completedJobToCAndC);

				// Then we have to create an ACK for the worker, telling them that we know
				// they are finished the job
				ACKContent completedJobACKToWorker = new ACKContent(content.getPacketNumber() + 1);
				sendACK(nodeDeliveredFrom.getDstAddress(), completedJobACKToWorker);
			}
		} 
		else if (content.getContentType().equals("WVU")) 
		{
			// if Worker has notified the broker that they are no longer available for work

			// remove the worker entirely from our list of workers
			connectedWorkers.remove(nodeDeliveredFrom);
			
			terminal.println("Worker " + nodeDeliveredFrom.getNodeName() + " has unsubscribed.");
			
			if(nodeDeliveredFrom.designatedJobs.size() > 0)
				terminal.println("Redistributing jobs...");
			// we need to collected all the uncompleted jobs that were designated to this
			// worker, and mark them for redistribution.
			for (int i = 0; i < nodeDeliveredFrom.designatedJobs.size(); i++) 
			{
				nodeDeliveredFrom.designatedJobs.get(i).resetDistributionsLeft(1);
				jobsPendingDistribution.add(nodeDeliveredFrom.designatedJobs.get(i));
			}

			// And finally, ACK this to the worker so that they know they are 
			// successfully unsubscribed.
			ACKContent workerUnsubscribeACK = new ACKContent(content.getPacketNumber() + 1);
			sendACK(nodeDeliveredFrom.getDstAddress(), workerUnsubscribeACK);
		}
	}
	
	private void processCAndCInstruction(NodeData nodeDeliveredFrom, SNDContent content)
	{
		// if packet is coming from a C&C
		if (content.getContentType().equals("JOB"))
		{
			// if it is a new job from the C&C
			int numWorkersCAndCWants = content.getNumWorkersForJob();
			
			if(numWorkersCAndCWants == -1 || numWorkersCAndCWants > connectedWorkers.size())	
			{	
				// If C&C wants all workers to do task, OR if C&C wants more workers to
				// do the task than currently available, then we simply assign the total
				// number of workers we have as the numWorkersForJob variable for this job.
				content.resetNumWorkersForJob(connectedWorkers.size());
				content = new SNDContent(content.toString());
			}
			// else if C&C has requested 1+ workers, and we have enough workers for this,
			// then we don't need to change anything about this PacketContent.
			
			// We now need to add this job to the Broker's total list of jobs,
			// Brokers start() method.
			
			allJobs.add(content);
			jobsPendingDistribution.add(content);
			terminal.print("\nNew Job from " + nodeDeliveredFrom.getNodeName() + ": " 
			+ content.getPacketContent() + " (Job ID " + content.getJobID() +")");
			
			// Send an ACK back to C&C, with the number of workers assigned this job
			ACKContent jobReceived = new ACKContent(content.getPacketNumber() + 1);
			sendACK(nodeDeliveredFrom.getDstAddress(), jobReceived);
			
			// Send a job packet to the C&C, telling them how many workers have 
			// been designated to the job
			SNDContent numWorkersDesignated = new SNDContent("SNDJOB00" + nodeDeliveredFrom.getIDNumber() 
															 + "00" + content.getJobID() + '\u0003');
			numWorkersDesignated.resetNumWorkersForJob(content.getNumWorkersForJob());
			nodeDeliveredFrom.sendPacket(numWorkersDesignated);
		}
	}
	
	private void sendACK(InetSocketAddress destination, ACKContent ackContent)
	{
		// ACKs are typically only sent once, so we do not have to designate a timer
		// to them, and we do not have to add them to our Go-Back-N window.
		
		DatagramPacket ackPacket = ackContent.toDatagramPacket(); 
		ackPacket.setSocketAddress(destination); 
		try {
			socket.send(ackPacket);
		} catch (IOException e) { e.printStackTrace(); }; 
	}
	
	/**
	 * Sender Method
	 * 
	 */
	public synchronized void start() throws Exception {
		ArrayList<Integer> randomWorkers = new ArrayList<Integer>();
		
		terminal.println("BROKER (PORT " + BROKER_SRC_PORT + "):");
		terminal.println("Awaiting C&Cs...");
		
		while(true)
		{
			if(connectedCAndCs.size() > 0 && connectedWorkers.size() > 0)
			{
				if(jobsPendingDistribution.size() > 0)
				{
					// I wanted a way to pick random workers, but not pick
					// the same one twice, and only randomly generated within
					// the range left instead of the entire range of the number
					// of workers, which could take far too long if it kept getting
					// the wrong numbers.
					randomWorkers.clear();
					for(int i = 0; i < connectedWorkers.size(); i++)
					{
						randomWorkers.add(i);
					}
					
					for(int i = 0; i < jobsPendingDistribution.size(); i++)
					{
						SNDContent jobToDistribute = jobsPendingDistribution.get(i);
						int randomGenerator;
						int selectedWorker;
						for(int distr = 0; distr < jobToDistribute.getDistributionsLeft(); distr++)
						{
							randomGenerator = new Random().nextInt(randomWorkers.size());
							selectedWorker = randomWorkers.get(randomGenerator);
							connectedWorkers.get(selectedWorker).sendPacket(jobToDistribute);
							randomWorkers.remove(randomGenerator);
						}
						jobToDistribute.resetDistributionsLeft(0);
					}
					//All jobs have been distributed, so we can clear the ArrayList
					jobsPendingDistribution.clear();
				}
			}
			this.wait();
		}
	}

	/**
	 * Test method
	 * 
	 * Sends a packet to a given address
	 */
	public static void main(String[] args) {
		try {					
			Terminal terminal= new Terminal("Broker");		
			(new Broker(terminal, DEFAULT_DST_NODE, BROKER_SRC_PORT)).start();
		} catch(java.lang.Exception e) {e.printStackTrace();}
	}
}
