import java.io.IOException;
import java.net.DatagramPacket;
import java.util.TimerTask;
import java.net.DatagramSocket;

public class TimerFlowControl extends TimerTask {

	DatagramSocket hostSocket;
	DatagramPacket packetToResend;

	public TimerFlowControl(DatagramSocket hostSocket, DatagramPacket packetToResend) {
		this.hostSocket = hostSocket;
		this.packetToResend = packetToResend;
		
	}

	@Override
	public void run() {
		try {
			hostSocket.send(packetToResend);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}