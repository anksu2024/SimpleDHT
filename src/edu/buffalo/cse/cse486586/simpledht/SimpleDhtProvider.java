/**
 * NAME				: ANKIT SARRAF
 * EMAIL			: sarrafan@buffalo.edu
 * PROJECT			: IMPLEMENTING SIMPLE DISTRIBUTED HASH TABLES USING CHORD
 * ASSUMPTIONS		: 1) ANY NEW NODE SENDS JOIN REQUEST TO THE INITIAL AVD (PORT # 5554)
 * 					  2) NO KEY VALUE PAIR HAS A COLON (:) IN IT
 * IMPLEMENTATION	: 1) JOIN OF NEW NODE
 * 					  2) APPROPRIATE INSERTION OF NEW KEY-VALUE BASED ON HASH VALUE OF THE KEY
 * 					  3) QUERYING GLOBAL (*) , LOCAL (@) , SPECIFIC KEY VALUE PAIRS FROM NETWORK
 * 					  4) DELETION OF ALL, LOCAL, SPECIFIC KEY VALUE PAIRS FROM THE NETWORK
 * RESOURCES		: http://conferences.sigcomm.org/sigcomm/2001/p12-stoica.pdf
 * 					  Lecture slides on Chord, Prof. Steve Ko (SUNY Buffalo)
 * COPYRIGHT		: THIS IS MY ORIGINAL PIECE OF ART, BETTER KNOWN AS CODING.
 * 					  I RESERVE THE COPYRIGHTS TO THE USAGE OF THIS CODE FOR ACADEMIC
 * 					  AND/OR PROFESSIONAL PURPOSES. 
 * 					  I BELIEVE CONSTRUCTIVE USAGE OF KNOWLEDGE.
 * 					  SO EVERYONE HAS THE RIGHT TO USE THIS CODE.
 * 					  BUT AS DEVELOPER OF THIS CODE, I BELIEVE PROPER MENTION OF BELOW SOURCE IS MUST.
 * 					  SOURCE: "SimpleDht Implementation by Ankit Sarraf (sarrafan@buffalo.edu)"
 */


package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {
	//Tag for Logging Activity
	static final String TAG = SimpleDhtActivity.class.getSimpleName();

	DatabaseHelper databaseHelper;

	public static String mySuccessor;
	public static String myPredecessor;

	// Query Related Locking
	private static volatile Cursor remoteCursor = null;
	/*static private Object lock;*/
	private static volatile boolean receivedResponse = false;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// Retrieve the database name to work on
		SQLiteDatabase myDB = databaseHelper.getWritableDatabase();

		int deletedRows = 0;

		if(selection.equals("@") || selection.equals("*")) {
			deletedRows = myDB.delete(DatabaseHelper.TABLE_NAME, null, null);

			if(selection.equals("*") && !getMyPort().equals(SimpleDhtProvider.mySuccessor)) {
				sendMessage("delt:" + getMyPort() + ":*" , SimpleDhtProvider.mySuccessor);
			}
		} else {

			String [] columns = {DatabaseHelper.KEY, DatabaseHelper.VALUE};

			selectionArgs = new String [] {selection};

			deletedRows = myDB.delete(DatabaseHelper.TABLE_NAME, columns[0] + "=?", selectionArgs);
			if(deletedRows == 0) {
				String msgToSend = "delt:" + getMyPort() + ":" + selection;
				sendMessage(msgToSend, SimpleDhtProvider.mySuccessor);
			}
		}

		// To indicate how many rows (row with key value as selectionArgs[0]
		/**Log.d(TAG, "Inside delete. Deleted # " + deletedRows + " Row with Key - " + selectionArgs[0]);*/

		// Return 0 if no row deleted
		return deletedRows;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Implement the insert method

		try {
			if(genHash(getMyPort()).toString().compareTo(genHash(SimpleDhtProvider.mySuccessor).toString()) == 0) {
				// There is only one node in the Chord

				SQLiteDatabase myDB = databaseHelper.getWritableDatabase();

				Log.e("ANKIT", values.toString());
				// id variable stores the result of insert
				// If id is -1 there was some error while inserting the row. So no row inserted
				// If some positive value represents the row where insertion was done
				long id = 0;

				synchronized(this) {
					id = myDB.replace(DatabaseHelper.TABLE_NAME, null, values);
				}

				if(id == -1) {
					Log.e(TAG, "Error while inserting => " + values.toString());
				} else {
					Log.d(TAG, "Insertion Successful at Row => " + id);
				}
			} else {
				String msg = "find:" + values.get("key").toString() + ":" + values.get("value").toString();

				Log.i(TAG, "Message to sent across the network => " + msg);

				// Send it to self
				sendMessage(msg, getMyPort());
			}
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Log.v("insert", values.toString());
		return uri;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		// If you need to perform any one-time initialization task, please do it here.

		// Global Lock object
		//lock = new Object();

		//Create the instance of the DatabaseHelper class
		databaseHelper = new DatabaseHelper(getContext());

		//Retrieve the Database which this code will work on
		SQLiteDatabase myDB = databaseHelper.getWritableDatabase();

		//Delete all the contents of the myDB
		myDB.delete(DatabaseHelper.TABLE_NAME, null, null);

		//Log the name of the Database
		Log.d(TAG, "Fetch myDB => " + myDB.toString());


		try {
			ServerSocket serverSocket = new ServerSocket(Constants.SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch(IOException e) {
			Log.e(TAG, "Can't create a ServerSocket : " + getMyPort());
			return false;
		}

		SimpleDhtProvider.mySuccessor = getMyPort();
		SimpleDhtProvider.myPredecessor = getMyPort();

		if(!Constants.REMOTE_PORT0.equals(getMyPort())) {
			sendMessage("join:" + getMyPort(), Constants.REMOTE_PORT0, getMyPort());
		}

		return true;
	}

	private void sendMessage(String msg, String receiver, String ... myPort) {
		Log.i(TAG, "Send the following data : [" + msg + "] To node => " + receiver);

		// Passing the message to the Client code to send it to sequencer
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, "" + (Integer.parseInt(receiver) * 2));

		Log.i(TAG, "Message Sent");
		return;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
			String sortOrder) {
		// TODO Auto-generated method stub
		// Retrieve the Database to work on
		SQLiteDatabase myDB = databaseHelper.getReadableDatabase();
		Cursor cursor = null;

		try {
			String [] columns = {DatabaseHelper.KEY, DatabaseHelper.VALUE};

			selectionArgs = new String [] {selection};

			if(selection.equals("*")) {
				// GDumpSearch
				Log.d("GDUMP", "Inside GDUMP");

				cursor = myDB.query(DatabaseHelper.TABLE_NAME, columns, null, null,
						null, null, null);

				Log.d("GDUMP", "# of Rows Retrieved : " + cursor.getCount());

				boolean checkCondition = false;
				try {
					checkCondition = genHash(SimpleDhtProvider.mySuccessor).compareTo(genHash(getMyPort())) == 0;
				} catch(Exception e) {
					e.printStackTrace();
				}
				if(checkCondition) {
					Log.d("GDUMP", "" + cursor.getCount());
					myDB.close();
					try {
						return cursor;
					}
					catch(Exception e) {
						Log.e("GDUMP", e.getMessage());
					}
				}

				Log.d(TAG, "Construct Waiter @ " + getMyPort());
				Waiter waiter = new Waiter("gqry:" + getMyPort() + ":" + serialized(cursor),
						SimpleDhtProvider.mySuccessor);

				Log.d(TAG, "Starting Waiter @ " + getMyPort());
				waiter.start();

				try {
					waiter.join();
				} catch(InterruptedException e) {
					e.printStackTrace();
				}

				MatrixCursor temp = new MatrixCursor(new String[] {"key", "value"});
				while(remoteCursor.moveToNext()) {
					String [] row = {remoteCursor.getString(0), 
							remoteCursor.getString(1)};
					temp.addRow(row);
				}
				/*
				while(cursor.moveToNext()) {
					String [] row = {cursor.getString(0),
							cursor.getString(1)};
					temp.addRow(row);
				}
				 */
				SimpleDhtProvider.remoteCursor = null;
				cursor = (Cursor) temp;
				temp.close();
			} else {
				if(selection.equals("@")) {
					// LDumpSearch
					cursor = myDB.query(DatabaseHelper.TABLE_NAME, columns, null, null,
							null, null, null);

					Log.d("LDUMP", "# of Rows Retrieved : " + cursor.getCount());

					//selectionArgs = new String [] {"key15"};
					//cursor = myDB.query(DatabaseHelper.TABLE_NAME, columns, columns[0] + "=?",
					//	selectionArgs, null, null, null);
				} else {
					// Key value pair search

					Log.d("ZZZZZZ" , "Inside " + getMyPort() + " Searching " + columns[0] +
							" for Key " + selection);

					cursor = myDB.query(DatabaseHelper.TABLE_NAME, columns, columns[0] + "=?",
							selectionArgs, null, null, null);

					String tempP = cursor.getCount() + ":" + getMyPort() + ":" + selectionArgs[0];

					Log.d("ZZZZZZ", "# Of specific keyed value found # " + tempP);

					if(cursor.getCount() == 0 || cursor == null) {
						// Send message to successor asking for Key

						Log.e("ZZZZZZ", "selection - " + selection);

						// We don not use Client Task this time for sending
						Waiter waiter = new Waiter("qkey:" + getMyPort() + ":" + selection,
								SimpleDhtProvider.mySuccessor);
						waiter.start();

						try {
							waiter.join();
						} catch(InterruptedException e) {
							e.printStackTrace();
						}

						synchronized(this) {
							cursor = SimpleDhtProvider.remoteCursor;
							SimpleDhtProvider.remoteCursor = null;
						}
					}
				}
			}

			//cursor.setNotificationUri(getContext().getContentResolver(), 
			//	buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider"));
		} catch(Exception e) {
			e.printStackTrace();
			return null;
		}

		myDB.close();
		return cursor;
	}

	private String serialized(Cursor cursor) {
		String serializedCursor = "";
		while(cursor.moveToNext()) {
			serializedCursor += cursor.getString(0) + "_" + cursor.getString(1) + " ";
		}

		return serializedCursor.trim();
	}

	private Cursor unserialized(String receivedMesage) {

		String [] keyValPairs = receivedMesage.trim().split(" ");
		Cursor receivedCursor = null;

		for(int i = 0 ; i < keyValPairs.length; i++) {
			String [] keyValPair = keyValPairs[i].split("_");
			receivedCursor = new MergeCursor(new Cursor[] {receivedCursor, 
					getMyCursor(keyValPair[0],
							keyValPair[1])});
		}

		return receivedCursor;
	}

	class Waiter extends Thread {
		String queryMsgToSend;
		String receiver;

		Waiter(String queryMsgToSend, String receiver) {
			this.queryMsgToSend = queryMsgToSend;
			this.receiver = receiver;
		}

		@Override
		public void run() {
			synchronized(this) {
				try {
					Log.d(TAG, "Sent Message " + queryMsgToSend + " From " + getMyPort() + " To " + receiver);

					Socket socket = new Socket(InetAddress.getByAddress(
							new byte[]{10, 0, 2, 2}), 
							Integer.parseInt(receiver) * 2);

					PrintWriter printWriterOut = new PrintWriter(socket.getOutputStream(), true);
					printWriterOut.write(queryMsgToSend);

					Log.d(TAG, "Send the Message " + queryMsgToSend + " From " + getMyPort() + " To " + receiver);
					printWriterOut.close();
					socket.close();
				} catch (NumberFormatException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (UnknownHostException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				while(true) {
					// Waiting for SimpleDhtProvider.recievedReponse = true
					// Here the SimpleDhtProvider.receivedResponse is a shared resource

					if(SimpleDhtProvider.receivedResponse) {
						Log.d(TAG, "ReceivedNode got set to " + SimpleDhtProvider.receivedResponse);
						break;
					}
				}

				// As soon as out of this loop make it false to make it available for new Query
				SimpleDhtProvider.receivedResponse = false;
			}
		}
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();

		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}

		String hashValue = formatter.toString();

		formatter.close();
		return hashValue;
	}

	protected final class DatabaseHelper extends SQLiteOpenHelper {
		//Initialize the Database Name
		private static final String DATABASE_NAME = "DHTdatabase.db";

		//Initialize the Table Name
		private static final String TABLE_NAME = "DHTTABLE";

		//Initialize the Database Version
		private static final int DATABASE_VERSION = 1;

		// Columns in KEYVALUETABLE
		// Column Key
		private static final String KEY = "key";
		// Column Value
		private static final String VALUE = "value";

		// Query String for Creating KEYVALUETABLE
		private static final String CREATE_TABLE = "CREATE TABLE " + TABLE_NAME + " (" +
				KEY + " VARCHAR(255) PRIMARY KEY, " +
				VALUE + " VARCHAR(255));";

		// Query string if we require to drop the table before upgrading it
		private static final String DROP_TABLE = "DROP TABLE IF EXISTS " + TABLE_NAME;

		Context context;

		DatabaseHelper(Context context) {
			// Call the super() class constructor
			super(context, DATABASE_NAME, null, DATABASE_VERSION);

			// Log to indicate the presence in the DatabaseHelper constructor
			Log.d(TAG, "Inside DatabaseHelper Constructor");

			// Setting the context data-member
			this.context = context;
		}

		@Override
		public void onCreate(SQLiteDatabase myDB) {
			// Go ahead and create the table
			myDB.execSQL(CREATE_TABLE);

			// Log to indicate creation of the table
			Log.d(TAG, "Table created => " + CREATE_TABLE);
		}

		@Override
		public void onUpgrade(SQLiteDatabase myDB, int arg1, int arg2) {
			// If the table already exists and it needs to be modified, drop the table first
			myDB.execSQL(DROP_TABLE);

			// Log to indicate Table was Dropped
			Log.d(TAG, "Table Dropped => " + TABLE_NAME);

			// call the onCreate() to create the new modified table
			onCreate(myDB);
		}
	}

	public String getMyPort() {
		/*Professor's Hack - Taken from SimpleMessenger*/
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);

		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

		String myPort = String.valueOf((Integer.parseInt(portStr)));

		return myPort;
	}

	class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		@Override
		protected Void doInBackground(ServerSocket ... sockets) {
			Log.i(TAG, "Inside Server Method");
			ServerSocket serverSocket = sockets[0];

			Socket clientSocket;
			BufferedReader bufferIn;

			try {
				while(true) {
					clientSocket = serverSocket.accept();

					bufferIn = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

					final String inputLine = bufferIn.readLine();
					if(inputLine.equals(null) || inputLine.equals("") || inputLine == null) {
						//If blank input entered, just break
						Log.i(TAG, "Exit client");
						break;
					}

					/**
					 * SERVER TASK BEGINS
					 */

					String [] messageParts = inputLine.split(":");

					if(messageParts[0].equals("join")) {
						String newNode = messageParts[1];
						String myNode = getMyPort();
						String succNode = SimpleDhtProvider.mySuccessor;

						if(myNode.equals(SimpleDhtProvider.mySuccessor)) {
							// There is only one node currently in the chord
							// Add this node as the second node in the chord

							// Inform the new Node about its Successors and Predecessors
							sendMessage("conf:" + myNode + ":" + myNode, newNode);

							// Update my Successor
							SimpleDhtProvider.mySuccessor = newNode;
							SimpleDhtProvider.myPredecessor = newNode;
						} else {
							String myNodeHash = ""; 
							String nwNodeHash = "";
							String mySuccHash = "";

							try {
								myNodeHash = genHash(myNode);
								nwNodeHash = genHash(newNode);
								mySuccHash = genHash(succNode);
							} catch (NoSuchAlgorithmException e) {
								// Error in calculating Hash
								e.printStackTrace();
								return null;
							}

							if(nwNodeHash.compareTo(myNodeHash) > 0) {
								if(mySuccHash.compareTo(nwNodeHash) > 0 ||
										myNodeHash.compareTo(mySuccHash) > 0) {
									// SuccNode will become the successor of the current Node
									sendMessage("conf:" + succNode + ":" + myNode, newNode);
									sendMessage("conf:-1:" + newNode, succNode);

									// Update the current Node's successor
									SimpleDhtProvider.mySuccessor = newNode;
								} else {
									// Forward the Request to Successor
									sendMessage("join:" + newNode, succNode);
								}
							} else {
								if(myNodeHash.compareTo(mySuccHash) > 0 &&
										mySuccHash.compareTo(nwNodeHash) > 0) {
									// New Node will become the successor of the Current Node
									// MyNode will become predecessor of New Node
									sendMessage("conf:" + succNode + ":" + myNode, newNode);
									sendMessage("conf:-1:" + newNode, succNode);

									// Update the current Node's successor
									SimpleDhtProvider.mySuccessor = newNode;
								} else {
									// Forward the Request to Successor
									sendMessage("join:" + newNode, succNode);
								}
							}
						}
					} else {
						if(messageParts[0].equals("conf")) {
							/**
							 * Configuration or Reconfiguration Message
							 * These Messages are of the type [conf:S]
							 * S => The new Successor of this Node
							 */

							// Set my successor as messageParts[1]
							if(!messageParts[1].equals("-1")) {
								// Set the new Successor
								SimpleDhtProvider.mySuccessor = messageParts[1];
							}

							if(!messageParts[2].equals("-1")) {
								// Set the new Predecessor
								SimpleDhtProvider.myPredecessor = messageParts[2];
							}

							// And it is done -- Configured. I know my new Successor
						} else {
							if(messageParts[0].equals("find")) {
								// To find if my new Message belongs to this node

								String myNodeHash = "";
								String mySuccHash = "";
								String myDataHash = "";

								try {
									myNodeHash = genHash(getMyPort());
									mySuccHash = genHash(SimpleDhtProvider.mySuccessor);
									myDataHash = genHash(messageParts[1]);
								} catch (NoSuchAlgorithmException e) {
									// Error in calculating Hash
									e.printStackTrace();
									return null;
								}

								String msg = ":" + messageParts[1] + ":" + messageParts[2];

								if(myDataHash.compareTo(myNodeHash) > 0) {			// DataHash > NodeHash
									if(mySuccHash.compareTo(myDataHash) > 0) {		// SuccHash > DataHash
										msg = "inst" + msg;							// Send inst req to Succ
									} else {
										if(myNodeHash.compareTo(mySuccHash) > 0) {	// NodeHash > SuccHash
											msg = "inst" + msg;						// Insert request to Succ
										} else {
											// Need to forward to successor node
											msg = "find" + msg;						// Else forward to Succ
										}
									}
								} else {											// DataHash < NodeHash
									if(mySuccHash.compareTo(myNodeHash) > 0) {		// Succ > NodeHash
										msg = "find" + msg;							// Forward to Succ
									} else {										// Succ < NodeHash
										if(mySuccHash.compareTo(myDataHash) > 0) {	// Succ > DataHash
											msg = "inst" + msg;						// Insert Req to Succ
										} else {									// Succ < DataHash
											msg = "find" + msg;						// Forward to Succ
										}
									}
								}
								// All the messages go to the Successor
								sendMessage(msg, SimpleDhtProvider.mySuccessor);
							} else {
								if(messageParts[0].equals("inst")) {
									// It is an insert message with start as inst
									// Retrieve the Database to work on
									SQLiteDatabase myDB = databaseHelper.getWritableDatabase();

									try {
										// Get the key Value from the contentValues
										String key = messageParts[1];
										String value = messageParts[2];

										Log.d(TAG, "Receival at Port => " + getMyPort());
										Log.d(TAG, "Key Received : " + key);
										Log.d(TAG, "Value Received : " + value);

										ContentValues values = new ContentValues();
										values.put("key", key);
										values.put("value", value);

										// id variable stores the result of insert
										// If id is -1 no row inserted
										// If some positive value => the row where insertion was done
										long id = 0;

										synchronized(this) {
											id = myDB.replace(DatabaseHelper.TABLE_NAME, null, values);
										}

										if(id == -1) {
											Log.e(TAG, "Error while inserting => " + key);
										} else {
											Log.d(TAG, "Insertion Successful at Row => " + id);
										}
									} catch(Exception e) {
										e.printStackTrace();
									} finally {
										myDB.close();
									}
								} else {
									if(messageParts[0].equals("qkey")) {
										Log.e("ZZZZZZ", "Inside qkey " + getMyPort() + " " + inputLine);

										if(messageParts[1].equals(getMyPort())) {
											Log.e("ZZZZZZ", "Inside qkey : chk originator : " + getMyPort() + " " + inputLine);
											// Single rotation across the Ring over
											// Now time to display results
											if(messageParts.length > 3) {
												// One key value pair was procured from AVDs
												// This is the result from the previous AVDs
												// The result reaches to the originator AVD

												SimpleDhtProvider.remoteCursor = getMyCursor(messageParts[2], messageParts[3]);
											} else {
												// End of 1 rotation across the Ring
												// No corresponding value found for the given key
												SimpleDhtProvider.remoteCursor = null;
											}

											// Notify lock
											// lock.notifyAll();
											SimpleDhtProvider.receivedResponse = true;
										} else {
											String forwardMessage = inputLine;
											if(messageParts.length <= 3) {
												// If no matching key-value pair based on the key was not found

												Log.d("ZZZZZZ", "Query Request Reached the new node : " + 
														inputLine + " @ " + getMyPort());

												SQLiteDatabase myDB = databaseHelper.getReadableDatabase();

												Log.d("ZZZZZZ", "After Writable Database create" + myDB.getPath());

												String [] columns = {DatabaseHelper.KEY, DatabaseHelper.VALUE};

												String [] searchArguments = {messageParts[2]};

												Log.d("ZZZZZZ", "Before query Cursor " + messageParts[2]);

												Cursor cursor = myDB.query(DatabaseHelper.TABLE_NAME, columns,
														columns[0] + "=?", searchArguments,
														null, null, null);

												Log.d("ZZZZZZ", "Get Cursor : " + cursor.getCount());

												if(cursor.getCount() != 0) {
													// A key value pair was found
													cursor.moveToFirst();

													Log.d("ZZZZZZ", "Below Cursor");

													forwardMessage += ":" + cursor.getString(1);

													Log.d("ZZZZZZ", "Cursor Value - " + cursor.getString(0) +
															", " + cursor.getString(1));
												}
											}

											sendMessage(forwardMessage, SimpleDhtProvider.mySuccessor);
										}
									} else {
										if(messageParts[0].equals("gqry")) {
											Log.d(TAG, "Received Message : " + inputLine);
											for(String a : messageParts) {
												Log.d(TAG, "Part " + a);
											}

											try{
												if(messageParts[1].equals(getMyPort())) {
													if(messageParts.length >= 3) {
														SimpleDhtProvider.remoteCursor = unserialized(messageParts[2].trim());
													} else {
														SimpleDhtProvider.remoteCursor = null;
													}

													SimpleDhtProvider.receivedResponse = true;
												} else {
													// This is a general node who got request from my previous node.
													// Just add my Content Provider Data and send the message further

													SQLiteDatabase myDB = databaseHelper.getReadableDatabase();

													String [] columns = {DatabaseHelper.KEY, DatabaseHelper.VALUE};

													//Select all the rows
													Cursor cursor = myDB.query(DatabaseHelper.TABLE_NAME, columns,
															null, null, null, null, null);

													String serializedCursor = serialized(cursor);

													String msgToSend = inputLine;
													if(messageParts.length < 3 || serializedCursor.equals("")) {
														msgToSend += serializedCursor.trim();
													} else {
														msgToSend += " " + serializedCursor.trim();
													}

													sendMessage(msgToSend, SimpleDhtProvider.mySuccessor);
												}
											}catch(Exception e){
												Log.v(getMyPort(),e.toString());
											}
										} else {
											if(messageParts[0].equals("delt")) {
												if(!messageParts[0].equals(getMyPort())) {
													// Not the originator
													int deletedRows = 0;

													SQLiteDatabase myDB = databaseHelper.getWritableDatabase();

													if(messageParts[2].equals("*")) {
														deletedRows = myDB.delete(DatabaseHelper.TABLE_NAME,
																null, null);

														sendMessage(inputLine,
																SimpleDhtProvider.mySuccessor);
													} else {
														String [] columns = {DatabaseHelper.KEY, 
																DatabaseHelper.VALUE};
														String [] selectionArgs = {messageParts[2]};

														deletedRows = myDB.delete(DatabaseHelper.TABLE_NAME,
																columns[0] + "=?",
																selectionArgs);

														sendMessage(inputLine, SimpleDhtProvider.mySuccessor);
													}
													Log.d("delt", "# of Rows deleted : " + deletedRows);
												}
											}
										}
									}
								}
							}
						}
					}
					/**
					 * SERVER TASK COMPLETES
					 */

					bufferIn.close();
					Log.i(TAG, "bufferIn Closed");

					clientSocket.close();
					Log.i(TAG, "clientSocket Closed");
				}
			} catch(IOException ie) {
				Log.e(TAG, "Exception => " + ie.getMessage());
			}

			return null;
		}
	}

	private Cursor getMyCursor(String key, String value) {
		MatrixCursor matrixCursor = new MatrixCursor(new String[] {"key", "value"});
		matrixCursor.addRow(new String[] {key, value});

		return (Cursor) matrixCursor;
	}
}