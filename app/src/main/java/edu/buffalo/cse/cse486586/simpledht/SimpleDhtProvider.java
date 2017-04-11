package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.JSONArray;
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

import static edu.buffalo.cse.cse486586.simpledht.DatabaseHelper.COLUMN_KEY;
import static edu.buffalo.cse.cse486586.simpledht.DatabaseHelper.COLUMN_VALUE;
import static edu.buffalo.cse.cse486586.simpledht.DatabaseHelper.TABLE_NAME;

public class SimpleDhtProvider extends ContentProvider {
    private static final int SERVER_PORT = 10000;
    private static final String PREDECESSOR = "PREDECESSOR";
    private static final String SUCCESSOR = "SUCCESSOR";
    private final String TAG = SimpleDhtProvider.class.getSimpleName();
    private SQLiteDatabase sqLiteDatabase;
    private DatabaseHelper databaseHelper;
    private int myPort;
    private Context context;
    private ServerSocket socket;
    private int joinPort = 5554;
    private int predecessorPort = 0;
    private int successorPort = 0;
    private String myHash = null;
    private String predecessorHash = null;
    private String successorHash = null;
    private Uri myUri;
    private ArrayList<String> chordNodes = new ArrayList<String>();
    private HashMap<String, Integer> hashValues = new HashMap<String, Integer>();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        sqLiteDatabase = databaseHelper.getWritableDatabase();
        if (predecessorPort == 0 && successorPort == 0) {
            int response;
            if (selection.equals("*") || selection.equals("@")) {
                //Delete all data in tables
                response = sqLiteDatabase.delete(TABLE_NAME, null, null);
            } else {
                String[] whereArgs = {selection};
                response = sqLiteDatabase.delete(TABLE_NAME, COLUMN_KEY + "=?", whereArgs);
            }
            Log.d("Response for delete", Integer.toString(response));
            return response;
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        sqLiteDatabase = databaseHelper.getWritableDatabase();
        /*Log.d("InsertHashes-P",predecessorHash);
        Log.d("InsertHashes-S",successorHash);*/

        String hashedKey = "";
        try {
            hashedKey = genHash(values.getAsString("key"));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        Log.d(values.getAsString("key"), hashedKey);
        if (predecessorPort == 0 && successorPort == 0) {
            //Single AVD Running and must insert all values.
            Log.d(values.getAsString("key"), "Inserted locally");
            try {
                sqLiteDatabase.insertWithOnConflict(TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
                //Log.v("insert", values.toString());
                sqLiteDatabase.close();
                return uri;
            } catch (SQLException e) {
                Log.e(TAG, "SQL Insert Content Provider Error");
                e.printStackTrace();
            } catch (IllegalStateException e) {
                e.printStackTrace();
            }
        } else if (hashedKey.compareTo(predecessorHash) >= 0 && hashedKey.compareTo(myHash) < 0) {
            //Insert into own space
            Log.d(values.getAsString("key"), "Inserted locally by key hash");
            try {
                sqLiteDatabase.insertWithOnConflict(TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
                Log.v("insert", values.toString());
                sqLiteDatabase.close();
                return uri;
            } catch (SQLException e) {
                Log.e(TAG, "SQL Insert Content Provider Error");
                e.printStackTrace();
            } catch (IllegalStateException e) {
                e.printStackTrace();
            }
        } else if (predecessorHash.compareTo(myHash) > 0 && successorHash.compareTo(myHash) > 0) {
            //Kind of like the first node in the ring.
            Log.d(values.getAsString("key"), "checking last node");
            if (hashedKey.compareTo(predecessorHash) >= 0 || hashedKey.compareTo(myHash) < 0) {
                //Insert into own space
                Log.d(values.getAsString("key"), "Locally inserted because last node");
                try {
                    sqLiteDatabase.insertWithOnConflict(TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
                    Log.v("insert", values.toString());
                    sqLiteDatabase.close();
                    return uri;
                } catch (SQLException e) {
                    Log.e(TAG, "SQL Insert Content Provider Error");
                    e.printStackTrace();
                } catch (IllegalStateException e) {
                    e.printStackTrace();
                }
            } else {
                //Forward to next node
                Log.d(values.getAsString("key"), "Forwarded to " + Integer.toString(successorPort));
                JSONObject jsonObject = new JSONObject();
                try {
                    jsonObject.put("key", values.getAsString("key"));
                    jsonObject.put("value", values.getAsString("value"));
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                Request request = new Request("Insert", Integer.toString(myPort), jsonObject.toString());
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, request, successorPort * 2);
            }
        } else {
            //Forward to next node
            Log.d(values.getAsString("key"), "Forwarded to " + Integer.toString(successorPort));
            JSONObject jsonObject = new JSONObject();
            try {
                jsonObject.put("key", values.getAsString("key"));
                jsonObject.put("value", values.getAsString("value"));
            } catch (JSONException e) {
                e.printStackTrace();
            }
            Request request = new Request("Insert", Integer.toString(myPort), jsonObject.toString());
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, request, successorPort * 2);
        }
        return null;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        sqLiteDatabase = databaseHelper.getReadableDatabase();
        String[] colsFetch = {COLUMN_KEY, COLUMN_VALUE};
        String searchClause = COLUMN_KEY + " = ?";
        String[] searchQuery = {selection};
        String hashedKey = "";
        try {
            hashedKey = genHash(selection);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        if (predecessorPort == 0 && successorPort == 0) {
            //Single AVD running and everything must be done here
            if (selection.equals("@") || selection.equals("*")) {
                Cursor cursor = sqLiteDatabase.rawQuery("Select * from " + TABLE_NAME, null);
                cursor.moveToFirst();

                MatrixCursor matrixCursor = new MatrixCursor(colsFetch);

                while (!cursor.isAfterLast()) {
                    Object[] values = {cursor.getString(0), cursor.getString(1)};
                    matrixCursor.addRow(values);
                    cursor.moveToNext();
                }
                cursor.close();
                sqLiteDatabase.close();

                return matrixCursor;
            } else {
                Cursor cursor = sqLiteDatabase.query(TABLE_NAME, colsFetch, searchClause, searchQuery, null, null, null);
                //Log.v("query", selection);
                cursor.moveToFirst();
                Object[] values = {cursor.getString(0), cursor.getString(1)};
                MatrixCursor matrixCursor = new MatrixCursor(colsFetch);
                matrixCursor.addRow(values);
                cursor.close();
                sqLiteDatabase.close();
                return matrixCursor;
            }
        } else if (selection.equals("@")) {
            Cursor cursor = sqLiteDatabase.rawQuery("Select * from " + TABLE_NAME, null);
            cursor.moveToFirst();

            MatrixCursor matrixCursor = new MatrixCursor(colsFetch);

            while (!cursor.isAfterLast()) {
                Object[] values = {cursor.getString(0), cursor.getString(1)};
                matrixCursor.addRow(values);
                cursor.moveToNext();
            }
            cursor.close();
            sqLiteDatabase.close();

            return matrixCursor;
        } else if (selection.equals("*")) {
            //Fetch results from all other devices and combine with own results
            Request request = new Request("Query", Integer.toString(myPort), selection);
            try {
                String response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, request, successorPort * 2).get();
                JSONObject jsonObject = new JSONObject(response);
                JSONArray keysArray = jsonObject.getJSONArray("keys");
                JSONArray valuesArray = jsonObject.getJSONArray("values");
                MatrixCursor matrixCursor = new MatrixCursor(colsFetch);
                for (int i = 0; i < keysArray.length(); i++) {
                    Object[] values = {keysArray.getString(i), valuesArray.getString(i)};
                    matrixCursor.addRow(values);
                }
                Cursor cursor = sqLiteDatabase.rawQuery("Select * from " + TABLE_NAME, null);
                cursor.moveToFirst();
                while (!cursor.isAfterLast()) {
                    Object[] values = {cursor.getString(0), cursor.getString(1)};
                    matrixCursor.addRow(values);
                    cursor.moveToNext();
                }
                cursor.close();
                sqLiteDatabase.close();
                return matrixCursor;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (JSONException e) {
                e.printStackTrace();
            }

        } else if (selection.charAt(0) == '*' && selection.charAt(1) == '#') {
            //* Query at some other avd and just recursion here.
            String[] split = selection.split("#");
            MatrixCursor matrixCursor = new MatrixCursor(colsFetch);
            if (Integer.parseInt(split[1]) != successorPort) {
                try {
                    Request request = new Request("Query", split[1], split[0]);
                    String response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, request, successorPort * 2).get();
                    JSONObject jsonObject = new JSONObject(response);
                    JSONArray keysArray = jsonObject.getJSONArray("keys");
                    JSONArray valuesArray = jsonObject.getJSONArray("values");
                    for (int i = 0; i < keysArray.length(); i++) {
                        Object[] values = {keysArray.getString(i), valuesArray.getString(i)};
                        matrixCursor.addRow(values);
                    }
                } catch (JSONException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            Cursor cursor = sqLiteDatabase.rawQuery("Select * from " + TABLE_NAME, null);
            cursor.moveToFirst();
            while (!cursor.isAfterLast()) {
                Object[] values = {cursor.getString(0), cursor.getString(1)};
                matrixCursor.addRow(values);
                cursor.moveToNext();
            }
            cursor.close();
            sqLiteDatabase.close();
            return matrixCursor;

        } else if (hashedKey.compareTo(predecessorHash) >= 0 && hashedKey.compareTo(myHash) < 0 || ((predecessorHash.compareTo(myHash) > 0 && successorHash.compareTo(myHash) > 0) && (hashedKey.compareTo(predecessorHash) >= 0 || hashedKey.compareTo(myHash) < 0))) {
            //Key is stored locally
            Cursor cursor = sqLiteDatabase.query(TABLE_NAME, colsFetch, searchClause, searchQuery, null, null, null);
            //Log.v("query", selection);
            cursor.moveToFirst();
            Object[] values = {cursor.getString(0), cursor.getString(1)};
            MatrixCursor matrixCursor = new MatrixCursor(colsFetch);
            matrixCursor.addRow(values);
            cursor.close();
            sqLiteDatabase.close();
            return matrixCursor;
        } else {
            //Ask successor to send the key and wait for response
            Request request = new Request("Query", Integer.toString(myPort), selection);
            try {
                String response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, request, successorPort * 2).get();
                JSONObject jsonObject = new JSONObject(response);
                Object[] values = {jsonObject.getString("key"), jsonObject.getString("value")};
                MatrixCursor matrixCursor = new MatrixCursor(colsFetch);
                matrixCursor.addRow(values);
                sqLiteDatabase.close();
                return matrixCursor;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (JSONException e) {
                e.printStackTrace();
            }

        }
        return null;
    }

    @Override
    public boolean onCreate() {
        context = getContext();
        databaseHelper = new DatabaseHelper(context);
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority("edu.buffalo.cse.cse486586.simpledht.provider");
        uriBuilder.scheme("content");
        myUri = uriBuilder.build();
        TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = Integer.parseInt(portStr);
        try {
            myHash = genHash(Integer.toString(myPort));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

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
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, joinRequest, joinPort * 2);
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

    private class ClientTask extends AsyncTask<Object, Void, String> {
        @Override
        protected String doInBackground(Object... msg) {
            Request request = (Request) msg[0];
            int remotePort = (Integer) msg[1];
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        remotePort);
                DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                dataOutputStream.writeUTF(request.getJson());
                dataOutputStream.flush();
                //Log.d("MSG SENT", request.getJson());
                //Log.d("REMOTE PORT", Integer.toString(remotePort));
                DataInputStream dataInputStream = new DataInputStream((socket.getInputStream()));
                socket.setSoTimeout(5000);
                String resp = dataInputStream.readUTF();
                if (resp.equals("OK")) {
                    socket.close();
                } else {
                    socket.close();
                    return resp;
                }
            } catch (SocketTimeoutException e) {
                Log.d("SocketTimeOut", "Exception for" + Integer.toString(remotePort));
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                Log.d("IOEXCEPTION", "Timeout on " + Integer.toString(remotePort));
                e.printStackTrace();
                return null;
            }

            return null;
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
                    if (!request.getType().equals("Query")) {
                        dataOutputStream.writeUTF("OK");
                        socket.close();
                        dataOutputStream.flush();
                        dataInputStream.close();
                        dataOutputStream.close();
                    }

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
                        predecessorPort = hashValues.get(jsonObject.getString(PREDECESSOR));
                        successorPort = hashValues.get(jsonObject.getString(SUCCESSOR));
                        Log.d("New Join - Predecessor", Integer.toString(predecessorPort));
                        Log.d("New Join - Successor", Integer.toString(successorPort));
                        Log.d("New Join - Hash", predecessorHash);
                    } else if (request.getType().equals("NewSuccessor")) {
                        successorPort = Integer.parseInt(request.getResponse());
                        successorHash = genHash(request.getResponse());
                        Log.d("New Successor", Integer.toString(successorPort));
                    } else if (request.getType().equals("NewPredecessor")) {
                        predecessorPort = Integer.parseInt(request.getResponse());
                        predecessorHash = genHash(request.getResponse());
                        Log.d("New Predecessor", Integer.toString(predecessorPort));
                        Log.d("New Predecessor Hash", predecessorHash);
                    } else if (request.getType().equals("Insert")) {
                        JSONObject jsonObject = new JSONObject(request.getResponse());
                        ContentValues contentValues = new ContentValues();
                        try {
                            contentValues.put("key", jsonObject.getString("key"));
                            contentValues.put("value", jsonObject.getString("value"));
                        } catch (JSONException e) {
                            e.printStackTrace();
                        }
                        insert(myUri, contentValues);
                    } else if (request.getType().equals("Query")) {
                        JSONObject response = new JSONObject();
                        String query = request.getResponse();
                        if (query.equals("*")) {
                            Cursor cursor = query(myUri, null, "*#" + request.getOriginalPort(), null, null);
                            JSONArray keysArray = new JSONArray();
                            JSONArray valuesArray = new JSONArray();
                            cursor.moveToFirst();
                            int i = 0;
                            while (!cursor.isAfterLast()) {
                                keysArray.put(i, cursor.getString(cursor.getColumnIndex("key")));
                                valuesArray.put(i, cursor.getString(cursor.getColumnIndex("value")));
                                i++;
                                cursor.moveToNext();
                            }
                            response.put("keys", keysArray);
                            response.put("values", valuesArray);
                            cursor.close();
                        } else {
                            Cursor cursor = query(myUri, null, query, null, null);
                            cursor.moveToFirst();
                            response.put("key", cursor.getString(cursor.getColumnIndex("key")));
                            response.put("value", cursor.getString(cursor.getColumnIndex("value")));
                            cursor.close();
                        }

                        dataOutputStream.writeUTF(response.toString());
                        socket.close();
                        dataOutputStream.flush();
                        dataInputStream.close();
                        dataOutputStream.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    break;
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                } catch (JSONException e) {
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
