package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import android.os.AsyncTask;
import android.util.Log;

/***
 * ClientTask is an AsyncTask that should send a string over the network.
 * It is created by ClientTask.executeOnExecutor() call 
 * on detecting the click of a Send key button
 * @author sarrafan
 */
class ClientTask extends AsyncTask<String, Void, Void> {
	//Tag for Logging Activity
	static final String TAG = SimpleDhtActivity.class.getSimpleName();

	@Override
	protected Void doInBackground(String ... msgs) {
		try {
			PrintWriter printWriterOut;

			Log.i(TAG, "In client");

			//Get the message I need to send across the socket
			String msgToSend = msgs[0];
			Log.i(TAG, "Message to Send => " + msgToSend);

			//Get the Node where I need to send the message
			String receiver = msgs[1];
			Log.i(TAG, "Receiver Node => " + msgs[1]);

			Socket socket = new Socket(InetAddress.getByAddress(
					new byte[]{10, 0, 2, 2}), 
					Integer.parseInt(receiver));

			printWriterOut = new PrintWriter(socket.getOutputStream(), true);
			printWriterOut.write(msgToSend);

			printWriterOut.close();
			socket.close();
		} catch (UnknownHostException e) {
			Log.e("ANKIT", "ClientTask UnknownHostException");
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
}