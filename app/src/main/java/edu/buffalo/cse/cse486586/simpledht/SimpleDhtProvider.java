package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

public class SimpleDhtProvider extends ContentProvider {
    private static final int SERVER_PORT = 10000;
    private static final String PREDECESSOR = "PREDECESSOR";
    private static final String SUCCESSOR = "SUCCESSOR";
    private final String TAG = SimpleDhtProvider.class.getSimpleName();
    private int myPort;
    private Context context;
    private ServerSocket socket;
    private int joinPort = 5554;
    private Boolean joinReqSent = false;
    private Boolean joinedStatus = false;
    private int predecessorPort = 0;
    private int successorPort = 0;
    private String myHash = null;
    private String predecessorHash = null;
    private String successorHash = null;
    private ArrayList<String> chordNodes = new ArrayList<String>();
    private HashMap<String, Integer> hashValues = new HashMap<String, Integer>();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        context = getContext();
        TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = Integer.parseInt(portStr);

        int i = 5554;
        int j = 1;
        while (i < 5564) {
            try {
                hashValues.put(genHash(Integer.toString(i)), i);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            i += 2;
        }

        try {
            socket = new ServerSocket(SERVER_PORT);
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, socket);

        if (myPort != joinPort) {
            //Send join request to central joining port
            Request joinRequest = new Request("Join", Integer.toString(myPort));
            while (joinReqSent == false) {
                try {
                    joinReqSent = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, joinRequest, joinPort * 2).get();
                    if (joinReqSent == false) {
                        Thread.sleep(2000);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
            Log.d(TAG, "Join Req Sent");
        } else {
            try {
                chordNodes.add(genHash(Integer.toString(myPort)));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        return null;
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
        return formatter.toString();
    }

    private class ClientTask extends AsyncTask<Object, Void, Boolean> {
        @Override
        protected Boolean doInBackground(Object... msg) {
            Request request = (Request) msg[0];
            int remotePort = (Integer) msg[1];
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        remotePort);
                DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                dataOutputStream.writeUTF(request.getJson());
                dataOutputStream.flush();
                Log.d("MSG SENT", request.getJson());
                Log.d("REMOTE PORT", Integer.toString(remotePort));
                DataInputStream dataInputStream = new DataInputStream((socket.getInputStream()));
                socket.setSoTimeout(5000);
                String resp = dataInputStream.readUTF();
                if (resp.equals("OK")) {
                    socket.close();
                    return true;
                }
            } catch (SocketTimeoutException e) {
                Log.d("SocketTimeOut", "Exception for" + Integer.toString(remotePort));
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                Log.d("IOEXCEPTION", "Timeout on " + Integer.toString(remotePort));
                e.printStackTrace();
                return false;
            }

            return false;
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, Request, Void> {

        ServerSocket serverSocket;

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            serverSocket = sockets[0];
            Log.d(TAG, "Server Task Started");
            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                    DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                    String jsonString = dataInputStream.readUTF();
                    Request request = new Request(jsonString);
                    dataOutputStream.writeUTF("OK");
                    socket.close();
                    dataOutputStream.flush();
                    dataInputStream.close();
                    dataOutputStream.close();
                    Log.d("Request Received", jsonString);

                    //Process messages of this type: JOIN, NEWLOCATION, NEWSUCCESSOR,NEWPREDECESSOR
                    if (request.getType().equals("Join")) {
                        String response = insertNodeIntoList(genHash(request.getOriginalPort()));
                        request.setResponse(response);
                        request.setType("NewLocation");
                        publishProgress(request);
                    } else if (request.getType().equals("NewLocation")) {
                        JSONObject jsonObject = new JSONObject(request.getResponse());
                        predecessorHash = jsonObject.getString(PREDECESSOR);
                        successorHash = jsonObject.getString(SUCCESSOR);
                        myHash = genHash(Integer.toString(myPort));
                        predecessorPort = hashValues.get(jsonObject.getString(PREDECESSOR));
                        successorPort = hashValues.get(jsonObject.getString(SUCCESSOR));
                        Log.d("New Join - Predecessor",Integer.toString(predecessorPort));
                        Log.d("New Join - Successor",Integer.toString(successorPort));
                        Log.d("New Join - Hash",predecessorHash);
                    } else if(request.getType().equals("NewSuccessor")){
                        successorPort = Integer.parseInt(request.getResponse());
                        successorHash = genHash(request.getResponse());
                        Log.d("New Successor", Integer.toString(successorPort));
                    } else if(request.getType().equals("NewPredecessor")){
                        predecessorPort = Integer.parseInt(request.getResponse());
                        predecessorHash = genHash(request.getResponse());
                        Log.d("New Predecessor",Integer.toString(predecessorPort));
                        Log.d("New Predecessor Hash",predecessorHash);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    break;
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                } catch (JSONException e){
                    e.printStackTrace();
                }
            }

            return null;
        }

        protected void onProgressUpdate(Request... requests) {
            Request request = requests[0];
            if (request.getType().equals("NewLocation")) {
                //Send this new location to original port
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, request, Integer.parseInt(request.getOriginalPort()) * 2);
                //Send new info to successor and predecessor
                try {
                    Request newRequest = new Request("NewPredecessor", Integer.toString(myPort), request.getOriginalPort());
                    JSONObject jsonObject = new JSONObject(request.getResponse());
                    Integer port = hashValues.get(jsonObject.getString(SUCCESSOR));
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, newRequest, port * 2);
                    newRequest = new Request("NewSuccessor", Integer.toString(myPort), request.getOriginalPort());
                    port = hashValues.get(jsonObject.getString(PREDECESSOR));
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, newRequest, port * 2);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            }
            return;
        }

        private String insertNodeIntoList(String node) {
            chordNodes.add(node);
            Collections.sort(chordNodes);
            int index = chordNodes.indexOf(node);
            int n = chordNodes.size();
            Log.d("Ring order", chordNodes.toString());
            JSONObject jsonObject = new JSONObject();
            try {
                if (index == 0) {
                    //Inserted at 1st position. Successor is at 1 and predecessor is at n-1
                    jsonObject.put(SUCCESSOR, chordNodes.get(1));
                    jsonObject.put(PREDECESSOR, chordNodes.get(n - 1));
                } else if (index == n - 1) {
                    //Inserted at last position. Successor is at 0 and predecessor at n-2
                    jsonObject.put(SUCCESSOR, chordNodes.get(0));
                    jsonObject.put(PREDECESSOR, chordNodes.get(n - 2));
                } else {
                    //Inserted in between somewhere. Successor is at index+1 and predecessor at index-1
                    jsonObject.put(SUCCESSOR, chordNodes.get(index + 1));
                    jsonObject.put(PREDECESSOR, chordNodes.get(index - 1));
                }
            } catch (JSONException e) {
                e.printStackTrace();
                return null;
            }

            return jsonObject.toString();
        }

    }


}
