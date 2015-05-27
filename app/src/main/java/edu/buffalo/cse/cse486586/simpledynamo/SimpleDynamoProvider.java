package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider
{
    NodeInfo [] all_nodes = new NodeInfo[5];
    int current_port;
    Storage storage;
    int node_index = -1;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs)
    {
        Message msg_to_send = new Message(current_port);
        msg_to_send.msg_type = MessageType.DELETE_DATA;

		if(selection.equals("\"*\""))
        {
            msg_to_send.key = "\"@\"";
            for(int i = 0; i < 5; i++)
            {
                sendMessage(msg_to_send, all_nodes[i].port_number);
            }
        }
        else if(selection.equals("\"@\""))
        {
            deleteData(selection);
        }
        else
        {
            int index = findNode(selection);
            msg_to_send.key = selection;

            for(int i = index; i <= (index + 2); i++)
            {
                sendMessage(msg_to_send, all_nodes[i%5].port_number);
            }
        }

		return 0;
	}

    public void deleteData(String selection)
    {
        SQLiteDatabase db = storage.getWritableDatabase();

        if(selection.equals("\"@\""))
        {
            db.rawQuery("DELETE FROM UserData", null);
        }
        else
        {
            storage.delete(selection);
        }
    }

	@Override
	public String getType(Uri uri)
    {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values)
    {
		String key = (String)values.get("key");
        String value= (String)values.get("value");

        int index = findNode(key);
        Message msg_to_send = new Message(current_port);
        msg_to_send.key = key;
        msg_to_send.value = value;
        msg_to_send.msg_type = MessageType.ADD_DATA;

        for(int i = index; i <= (index + 2); i++)
        {
            sendMessage(msg_to_send, all_nodes[i%5].port_number);
        }

		return null;
	}

    public void copyData()
    {
        Log.v("copying data after crash recovery", "");
        HashMap<String, String> data_to_copy = new HashMap<String, String>();
        SQLiteDatabase writable_database = storage.getWritableDatabase();

        // deleting all data
        writable_database.rawQuery("DELETE FROM UserData", null);

        HashMap<String, String> received_data = sendMeData(node_index - 1, node_index - 1, node_index - 2);
        if(received_data != null)
        {
            data_to_copy.putAll(received_data);
        }

        received_data = sendMeData(node_index + 1, node_index, node_index);
        if(received_data != null)
        {
            data_to_copy.putAll(received_data);
        }

        Iterator<String> all_keys = data_to_copy.keySet().iterator();
        while(all_keys.hasNext())
        {
            String key = all_keys.next();
            String value = data_to_copy.get(key);
            Log.v(key, value);
            storage.insert(key, value);
        }
    }

    public HashMap<String, String> sendMeData(int index_of_node, int index_of_data, int second_index_of_data)
    {
        index_of_data = (index_of_data + 5)%5;
        second_index_of_data = (second_index_of_data + 5)%5;
        index_of_node = (index_of_node + 5)%5;

        Log.v("Copying data from node " + index_of_node, "Copying data for data " + index_of_data + " " + second_index_of_data);
        HashMap<String, String> received_data = null;

        Message msg = new Message(current_port);
        msg.msg_type = MessageType.SEND_ALL_OF_MY_DATA;
        msg.key = index_of_data + "";
        msg.value = second_index_of_data + "";

        Log.v("SENDING MSG TO PORT ", all_nodes[(index_of_node)].port_number + "");
        Message received_msg = sendMessage(msg, all_nodes[(index_of_node)].port_number);
        Log.v("RECEIVED data for my request", all_nodes[(index_of_node)].port_number + "");
        if(received_msg != null)
        {
            received_data = received_msg.data;
        }

        return received_data;
    }

    public HashMap<String, String> getDataOfIndex(int index_of_data, int second_index_of_data)
    {
        Log.v("GOT_REQUEST_TO_GET_DATA_RANGE", index_of_data + " " + second_index_of_data);
        HashMap<String, String> data_found = new HashMap<String, String>();
        SQLiteDatabase database = storage.getReadableDatabase();
        Cursor cursor = database.rawQuery("SELECT * FROM UserData", null);
        cursor.moveToFirst();

        while(cursor.isAfterLast() != true)
        {
            String key = cursor.getString(0);
            int index = findNode(key);
            String value = cursor.getString(1);

            if(index == index_of_data || index == second_index_of_data)
            {
                data_found.put(key,value);
            }

            cursor.moveToNext();
        }

        Log.v("SUCCEDED", "getDataOfIndex");
        return data_found;
    }

	@Override
	public boolean onCreate()
    {
        // {5562, 5556, 5554, 5558, 5560};
        int [] ports = {11124, 11112, 11108, 11116, 11120};
        Log.v("INSIDE ON CREATE", "INSIDE ON CREATE");

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String current_avd_id = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        current_port = 2*Integer.parseInt(current_avd_id);
        storage = new Storage(getContext());

        for(int i = 0; i < 5; i++)
        {
            if(current_port == ports[i])
            {
                node_index = i;
                break;
            }
        }

        for(int i = 0; i < 5; i++)
        {
            try
            {
                String hash = genHash((ports[i]/2) + "");
                all_nodes[i] = new NodeInfo(ports[i], hash);
            }
            catch (Exception e)
            {

            }
        }

        try
        {
            new ServerTask(current_port).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
        }
        catch (Exception e)
        {

        }

        copyData();
        Log.v("RECOVERED DATA", "RECOVERED DATA");

		return true;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder)
    {
        MatrixCursor macursor = new MatrixCursor(new String[] {"key", "value"});
        Message msg = new Message(current_port);
        msg.msg_type = MessageType.SEND_DATA;
        msg.key = selection;
        Message received_msg = null;
        HashMap<String, String> received_key_values = new HashMap<String, String>();

		if(selection.equals("\"*\""))
        {
            msg.key = "\"@\"";

            for(int i = 0; i < 5; i++)
            {
                try
                {
                    received_msg = sendMessage(msg, all_nodes[i].port_number);

                    if(received_msg != null)
                    {
                        Map<String, String> all_data = received_msg.data;
                        Iterator<String> all_received_keys = all_data.keySet().iterator();

                        while(all_received_keys.hasNext())
                        {
                            String current_key = all_received_keys.next();
                            String [] value_and_version = all_data.get(current_key).split(",");
                            String value = value_and_version[0];
                            received_key_values.put(current_key, value);
                        }
                    }
                }
                catch (Exception e)
                {

                }
            }
        }
        else if(selection.equals("\"@\""))
        {
            Map<String, String> received_data = null;
            received_data = getData(selection);
            Iterator<String> all_keys = received_data.keySet().iterator();

            while(all_keys.hasNext())
            {
                String key = all_keys.next();
                String [] value_and_version = received_data.get(key).split(",");
                received_key_values.put(key, value_and_version[0]);
            }
        }
        else
        {
            try
            {
                int index = findNode(selection);
                int current_version = -1;

                for(int i = index; i <= (index + 2); i++)
                {
                    received_msg = sendMessage(msg, all_nodes[(i + 5)%5].port_number);

                    if(received_msg != null)
                    {
                        Map<String, String> received_data = received_msg.data;
                        String [] value_and_version = received_data.get(selection).split(",");
                        String value = value_and_version[0];
                        int version = Integer.parseInt(value_and_version[1]);

                        if(version > current_version)
                        {
                            received_key_values.put(selection, value);
                        }
                    }
                    else
                    {
                        Log.v("Requested data not found", selection);
                    }
                }
            }
            catch (Exception e)
            {

            }
        }

        Iterator<String> keys_iterator = received_key_values.keySet().iterator();

        while(keys_iterator.hasNext())
        {
            String key = keys_iterator.next();
            String value = received_key_values.get(key);
            macursor.addRow(new String [] {key, value});
        }

		return macursor;
	}

    public int findNode(String key)
    {
        key = genHash(key);

        int return_index = 0;

        if(key.compareTo(all_nodes[0].hash) < 0 || key.compareTo(all_nodes[4].hash) > 0)
        {
            return 0;
        }

        for(int i = 1; i < 5; i++)
        {
            String previous_node_hash = all_nodes[i - 1].hash;
            String current_node_hash = all_nodes[i].hash;

            if (key.compareTo(previous_node_hash) > 0 && key.compareTo(current_node_hash) <= 0)
            {
                return_index  = i;
                break;
            }
        }

        return return_index;
    }

    public HashMap<String, String> getData(String selection)
    {
        HashMap<String, String> data = new HashMap<String, String>();
        SQLiteDatabase liteDB = storage.getReadableDatabase();

        try
        {
            if(selection.equals("\"@\""))
            {
                Cursor cursor = liteDB.rawQuery("SELECT * FROM UserData", null);
                cursor.moveToFirst();
                while(cursor.isAfterLast() != true)
                {
                    String key = cursor.getString(0);
                    String value = cursor.getString(1);
                    int version = cursor.getInt(2);
                    value = value + "," + version;

                    data.put(key, value);
                    cursor.moveToNext();
                }
            }
            else
            {
                Cursor cursor = liteDB.rawQuery("SELECT * FROM UserData WHERE key = '" + selection + "'", null);
                cursor.moveToFirst();

                if(cursor.getCount() > 0)
                {
                    String value = cursor.getString(1);
                    String key = cursor.getString(0);
                    int version = cursor.getInt(2);
                    value = value + "," + version;
                    data.put(key, value);
                }
            }
        }
        catch (Exception e)
        {
            Log.v("exception in getting data", e.toString());
        }

        return data;
    }

    public void addData(ContentValues cv)
    {
        String key = cv.getAsString("key");
        String value = cv.getAsString("value");

        storage.insert(key, value);
    }

    public Message sendMessage(final Message msg, final int port)
    {
        Message msg_received = null;
        ExecutorService executor_service = Executors.newSingleThreadExecutor();

        Callable<Message> caller = new Callable<Message>()
        {
            @Override
            public Message call() throws Exception
            {
                Message received_msg = null;

                try
                {
                    Socket socket = new Socket();
                    socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port));
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    oos.writeObject(msg);
                    oos.flush();
                    ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                    received_msg = (Message)ois.readObject();
                    socket.close();
                }
                catch (Exception e)
                {
                    Log.v("Exception sending message to " + port, e.toString());
                }

                return received_msg;
            }
        };

        try
        {
            Future<Message> future = executor_service.submit(caller);
            msg_received = future.get();
        }
        catch (Exception e)
        {

        }

        return msg_received;
    }

	@Override
	public int update(Uri uri, ContentValues values, String selection, String [] selectionArgs)
    {
		return 0;
	}

    private String genHash(String input)
    {
        Formatter formatter = null;

        try
        {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] sha1Hash = sha1.digest(input.getBytes());
            formatter = new Formatter();

            for (byte b : sha1Hash)
            {
                formatter.format("%02x", b);
            }
        }
        catch (Exception e)
        {

        }

        return formatter.toString();
    }

    class ServerTask extends AsyncTask<Void, Void, Void>
    {
        int current_port;

        public ServerTask(int current_port)
        {
            this.current_port = current_port;
        }

        @Override
        protected Void doInBackground(Void... params)
        {
            try
            {
                ServerSocket server_socket = new ServerSocket(10000);

                while(true)
                {
                    Socket accepted_socket = server_socket.accept();
                    Message received_msg = null;

                    ObjectInputStream oin = new ObjectInputStream(accepted_socket.getInputStream());
                    received_msg = (Message)oin.readObject();
                    ObjectOutputStream oos = new ObjectOutputStream(accepted_socket.getOutputStream());

                    if(received_msg.msg_type == MessageType.ADD_DATA)
                    {
                        ContentValues cv = new ContentValues();
                        cv.put("key", received_msg.key);
                        cv.put("value", received_msg.value);
                        addData(cv);

                        Message msg_to_send = new Message(current_port);
                        msg_to_send.key = "data added";
                        oos.writeObject(msg_to_send);
                        oos.flush();
                        oos.close();
                    }
                    else if(received_msg.msg_type == MessageType.DELETE_DATA)
                    {
                        deleteData(received_msg.key);

                        Message msg_to_send = new Message(current_port);
                        msg_to_send.key = "data deleted";
                        oos.writeObject(msg_to_send);
                        oos.flush();
                        oos.close();
                    }
                    else if(received_msg.msg_type == MessageType.SEND_DATA)
                    {
                        HashMap<String, String> data = getData(received_msg.key);
                        Message sent_msg = new Message(current_port);
                        sent_msg.data = data;
                        oos.writeObject(sent_msg);
                        oos.flush();
                        oos.close();
                    }
                    else if(received_msg.msg_type == MessageType.SEND_ALL_OF_MY_DATA)
                    {
                        int index_to_get = Integer.parseInt(received_msg.key);
                        int second_index_to_get = Integer.parseInt(received_msg.value);

                        HashMap<String, String> data = getDataOfIndex(index_to_get, second_index_to_get);
                        Message sent_msg = new Message(current_port);
                        sent_msg.data = data;
                        oos.writeObject(sent_msg);
                        oos.flush();
                        oos.close();
                    }

                    accepted_socket.close();
                }
            }
            catch (Exception e)
            {
                Log.v("Exception in server", e.toString());
            }

            return null;
        }
    }
}