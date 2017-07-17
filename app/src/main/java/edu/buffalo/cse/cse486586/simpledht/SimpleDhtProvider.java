package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

import edu.buffalo.cse.cse486586.simpledht.OnTestClickListener;

import android.app.Activity;
import android.content.ContentResolver;
import android.database.MatrixCursor;
import android.os.Bundle;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.ContentResolver;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.net.Uri.Builder;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDhtProvider extends ContentProvider {

    String myport;
    static final int SERVER_PORT = 10000;
    String TAG ="TAG";
    String my_id="";
    String  predecessor_id="";
     int predecessor_port=0;
    String  successor_id="";
     int successor_port=0;
    int my_port=0;

    List<String> ports = new ArrayList<String>();

    HashMap<String,Integer> map=new HashMap<String,Integer>();




    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        if(selection.equals("@") || selection.equals("*")){
            String[] files=getContext().fileList();
            for(String file: files){
                getContext().deleteFile(file);
            }
        }
        else{
            getContext().deleteFile(selection);
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
        // TODO Auto-generated method stub

        String filename = (String) values.get("key");


     //   Log.i("file_name1",filename);
        try {
            filename=genHash(filename);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        int sz=ports.size();
            Log.i("size_3",Integer.toString(sz));

        if(values.get("value")!=null && values.get("key")!=null) {

            if (ports.size() == 1  || ports.size() == 0) {
                filename = (String) values.get("key");
                String val = (String) values.get("value");
                FileOutputStream outputStream;
                try {
                    outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                    outputStream.write(val.getBytes());
                    outputStream.close();
                } catch (Exception e) {
                    Log.e(TAG, "File write failed");
                }
                Log.v("insert", values.toString());
                return uri;
            } else if (my_id.equals(ports.get(0)) && (ports.get(ports.size()-1).compareTo(filename) < 0 || my_id.compareTo(filename) > 0)) {
                filename = (String) values.get("key");
                String val = (String) values.get("value");
                FileOutputStream outputStream;
                try {
                    outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                    outputStream.write(val.getBytes());
                    outputStream.close();
                } catch (Exception e) {
                    Log.e(TAG, "File write failed");
                }
                Log.v("insert", values.toString());
                return uri;
            }  else if (predecessor_id.compareTo(filename) < 0 && my_id.compareTo(filename) > 0) {
                filename = (String) values.get("key");
                String val = (String) values.get("value");
                FileOutputStream outputStream;
                try {
                    outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                    outputStream.write(val.getBytes());
                    outputStream.close();
                } catch (Exception e) {
                    Log.e(TAG, "File write failed");
                }
                Log.v("insert", values.toString());
                return uri;
            } else {
                filename = (String) values.get("key");
                String val = (String) values.get("value");
                String msg = "";
                msg += "Insert";
                msg += "#";
                msg += filename;
                msg += "#";
                msg += val;
                msg += "#";
                msg += "\n";
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
            }
        }
       return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        TelephonyManager tel=(TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myport = String.valueOf((Integer.parseInt(portStr)*2 ));
      // String  myport_1=String.valueOf((Integer.parseInt(portStr)));

        int arr[] = {5554, 5556, 5558, 5560, 5562};

        int p=0;

        for(;p<=4;p++){
            try {
                String hash=genHash(Integer.toString(arr[p]));
              //  map.put(arr[p]*2,hash);
                map.put(hash,arr[p]*2);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }


        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }
        my_port = Integer.parseInt(myport);

        try {
            String str = genHash(portStr);
            Log.i("string_1",str);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        try {
            my_id=genHash(portStr);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        Log.i("my_id1",my_id);

      //  Log.i("port_str",portStr);
         if(my_port!=11108) {
             String msg = "";
             msg += "Node_join";
             msg += "#";
             msg +=myport;
             msg += "\n";
             new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
         }
        else{
             String hash= null;
             try {
                 hash = genHash(portStr);
             } catch (NoSuchAlgorithmException e) {
                 e.printStackTrace();
             }
             ports.add(hash);
         }

        return false;
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];
            Socket clientSocket = null;

            while (true) {
                try {
                    clientSocket = serverSocket.accept();
                    BufferedReader in = null;
                    PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                    in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    String msg = in.readLine();

                    String str = "";
                    String ptr = "";
                    int i = 0, j = 0;
                    if (msg!=null) {
                        for (i = 0; i < msg.length(); i++) {
                            if (msg.charAt(i) == '#') {
                             //   Log.i("str", str);
                                str = msg.substring(0, i);
                                j = i + 1;
                                break;
                            }
                        }

                        if (str.equals("Node_join") && my_port == 11108) {
                            ptr = msg.substring(i + 1);
                            int port_no = Integer.parseInt(ptr);
                            port_no = port_no / 2;
                            String hash = Integer.toString(port_no);
                            try {
                                hash = genHash(hash);
                            } catch (NoSuchAlgorithmException e) {

                            }
                            ports.add(hash);
                            Collections.sort(ports);
                            fun_pred_succ();
                            str = Integer.toString(predecessor_port);
                            String pr=Integer.toString(predecessor_port/2);
                            try {
                                predecessor_id =genHash(pr);
                            } catch (NoSuchAlgorithmException e) {
                                e.printStackTrace();
                            }
                     //       Log.i("predecessor_id",predecessor_id);
                     //       Log.i("predecssor_port", str);
                            str = Integer.toString(successor_port);
                            String scc= Integer.toString(successor_port/2);
                            try {
                                successor_id=genHash(scc);
                            } catch (NoSuchAlgorithmException e) {
                                e.printStackTrace();
                            }
                            Log.i("successor_port",successor_id);
                            Log.i("successor_port", str);
                            newtask(msg);
                        } else if (str.equals("Node_join")) {
                            ports.clear();
                            j = i + 1;
                            i++;
                            for (; i < msg.length(); i++) {
                                if (msg.charAt(i) == '#') {
                                    ptr = msg.substring(j, i);
                                    Log.i("ptr", ptr);
                                    ports.add(ptr);
                                    j = i + 1;
                                }
                            }
                            String ss = Integer.toString(ports.size());
                     //       Log.i("size", ss);
                            Collections.sort(ports);
                            fun_pred_succ();
                            str = Integer.toString(predecessor_port);
                            String p_port =Integer.toString(predecessor_port/2);
                            try {
                                predecessor_id =genHash(p_port);
                            } catch (NoSuchAlgorithmException e) {
                                e.printStackTrace();
                            }
                            Log.i("predecessor_port", str);
                            Log.i("predecessor_id",predecessor_id);
                            str = Integer.toString(successor_port);
                            String s_port =Integer.toString(successor_port/2);
                            try {
                                successor_id = genHash(s_port);
                            } catch (NoSuchAlgorithmException e) {
                                e.printStackTrace();
                            }
                     //       Log.i("successor_port", str);
                     //       Log.i("successor_id",successor_id);
                            out.println("ok");
                            out.flush();
                        } else if (str.equals("Insert")) {
                          //  Log.i("insert_1","insert");
                            try {
                                fun_insert(msg);
                            } catch (NoSuchAlgorithmException e) {
                                e.printStackTrace();
                            }
                        }
                        else if(str.equals("query")){
                     //       Log.i("it's in query..","it's...in.....query");
                     //       Log.i("message_4",msg);

                            String temp="";

                            for(int p=i+1 ;p<msg.length();p++){
                                if(msg.charAt(p)=='#'){
                                    temp=msg.substring(j,p);
                                    break;
                                }
                            }

                            int temp_port=0;

                       //     Log.i("temp_string",temp);

                            temp_port= Integer.parseInt(temp);

                            if(temp_port!=my_port) {
                                String[] files = getContext().fileList();
                                for (String file : files) {
                                    str = "";
                                    FileInputStream inputStream;

                                    try {
                                        inputStream = getContext().openFileInput(file);
                                        int k = inputStream.read();
                                        while (k != -1) {
                                            str += String.valueOf((char) k);
                                            k = inputStream.read();
                                        }
                                        inputStream.close();
                                    } catch (Exception e) {
                                        Log.e(TAG, "File write failed");
                                    }
                                    msg += file;
                                    msg += "#";
                                    msg += str;
                                    msg += "#";
                                }
                            }
                            try {
                                msg= queryy(msg);
                            } catch (ExecutionException e) {
                                e.printStackTrace();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            Log.i("final_message_1",msg);
                            out.println(msg);
                            out.flush();
                            if(in.readLine().equals("ok"))clientSocket.close();
                        }
                        else if(str.equals("query_1")){



                            String temp ="";



                            temp=msg.substring(i+1);
                      //      Log.i("message_5",temp);
                            str="";
                            FileInputStream inputStream;
                            try {
                                inputStream = getContext().openFileInput(temp);
                                int k = inputStream.read();
                                while (k != -1) {
                                    str += String.valueOf((char) k);
                                    k = inputStream.read();
                                }
                                inputStream.close();
                            } catch (Exception e) {
                                Log.e(TAG, "File write failed");
                            }
                            msg="";
                            msg+=temp;
                            msg+="#";
                            msg+=str;
                            msg+="#";
                            msg+="\n";
                      //      Log.i("return_message",msg);
                            out.println(msg);
                        }
                        clientSocket.close();

                    }
                    }catch(IOException e){
                    e.printStackTrace();
                }

            }
          //  return null;
        }
    }





    private String queryy(String msg) throws ExecutionException, InterruptedException {

     //   Log.i("queryy","it's.......in..........queryy.....");
        int k=1;
        int j=0;
        String str="";
        for(int i=0;i<msg.length();i++){
            if(msg.charAt(i)=='#'){
                if(k==1){k++;j=i+1;}
                else if(k==2){
                    k++;
                    str= msg.substring(j,i);
                    j=i+1;
                    break;
                }
            }
        }
        int ret_port_no=Integer.parseInt(str);

     //   Log.i("ret_port_no",Integer.toString(ret_port_no));

        if(ret_port_no==successor_port){
     //       Log.i("successsssss",Integer.toString(ret_port_no));
                 return msg;
        }
        else {
            msg = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg).get();
      //      Log.i("final_message",msg);
            return msg;
        }
    }



    private void fun_insert(String msg) throws NoSuchAlgorithmException {

        String key="";
        String value = "";
       int j=0;
        int k=1;
        for(int i=0;i<msg.length();i++){
            if(msg.charAt(i)=='#'){
                if(k==1) {j=i+1;k++;continue;}
                else if(k==2){
                    String str=msg.substring(j,i);
                    key=str;
                    j=i+1;
                    k++;
                }
                else if(k==3){
                    String str=msg.substring(j,i);
                    value=str;
                    j=i+1;
                    k++;
                }
            }
        }
        String hash="";
        try {
             hash =genHash(key);
           //Log.i("key",hash);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

     //   Log.i("key",key);
     //   Log.i("value_1",value);

        int port_1=predecessor_port/2;
       String  hash1 = genHash(Integer.toString(port_1));
        predecessor_id=hash1;


        port_1=successor_port/2;
       String hash2 = genHash(Integer.toString(port_1));
        successor_id=hash2;

         if(my_id.equals(ports.get(0)) && (my_id.compareTo(hash)>0 ||(my_id.compareTo(hash)<0 && ports.get(ports.size()-1).compareTo(hash)<0))){
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority("content");
            uriBuilder.scheme("edu.buffalo.cse.cse486586.simpledht.provider");
            Uri uri = uriBuilder.build();
            ContentValues cv = new ContentValues();
            cv.put("key", key);
            cv.put("value", value);
            insert(uri,cv);
        }
        else if(my_id.equals(ports.get(ports.size()-1)) && my_id.compareTo(hash)<0){
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
        }
        else if(predecessor_id.compareTo(hash)<0 && my_id.compareTo(hash)>0){
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority("content");
            uriBuilder.scheme("edu.buffalo.cse.cse486586.simpledht.provider");
            Uri uri = uriBuilder.build();
            ContentValues cv = new ContentValues();
            cv.put("key", key);
            cv.put("value", value);
            insert(uri,cv);
        }
        else{
         //   Log.i("insert_4","insert");
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
        }

    }

    private void fun_pred_succ(){

          String str="";

        int sz=ports.size();

            for (int i = 0; i < sz; i++) {
                if (ports.get(i).equals(my_id)) {
                    if (i == 0) {
                        predecessor_port = map.get(ports.get(sz - 1));
                        successor_port = map.get(ports.get(1));
                    } else if (i == sz - 1) {
                        predecessor_port = map.get(ports.get(sz - 2));
                        successor_port = map.get(ports.get(0));
                    } else {
                        predecessor_port = map.get(ports.get(i - 1));
                        successor_port = map.get(ports.get(i + 1));
                    }
                    break;

            }
        }

    }




    private class ClientTask extends AsyncTask<String, Void, String> {

        @Override
        protected String doInBackground(String... msgs) {

            String msg = msgs[0];
            String str="";
            String ptr="";

            for(int i=0;i<msg.length();i++){
                if(msg.charAt(i)=='#'){
                     str=msg.substring(0,i);
                     ptr=msg.substring(i+1);
                    break;
                }
            }
            try {
                if (str.equals("Node_join")) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            11108);
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    out.println(msg);
                    out.flush();
                }
                else if(str.equals("Insert")){
                  //  Log.i("insert_3","insert");
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            successor_port);
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    out.println(msg);
                    out.flush();
                }
                else if(str.equals("query")){
                  //  Log.i("client_task","it's.....in ....clent....task");
                  //  Log.i("successor_port_2",Integer.toString(successor_port));
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            successor_port);
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    out.println(msg);
                    out.flush();
                    msg=in.readLine();
                 //   Log.i("client_task",msg);
                    out.println("ok");
                    return msg;
                }

            }catch(UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            }catch(IOException e){
                Log.e(TAG, "ClientTask socket IOException");
            }

         return null;
        }
    }


   // node_join task

    private void newtask(String  msg){
     //   Log.i("newtask","newtask");
     //   Log.i("new_task_message",msg);
        msg="";
        int i=0;
        msg+="Node_join";
        msg+="#";
        for( i=0;i<ports.size();i++){
            String str=ports.get(i);
            msg+=str;
            msg+="#";
        }
         msg+="\n";
      //  Log.i("new_task_message_1",msg);
        new node_join().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
    }


    private class node_join extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

          //  Log.i("node_join",msgs[0]);

            String msg = msgs[0];

            int arr[] = {11112, 11116, 11120, 11124};

            try {
                for (int i = 0; i <4; i++) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            arr[i]);
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    out.println(msg);
                    out.flush();
                    if(in.readLine()=="ok")socket.close();
                }

            }catch(UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            }catch(IOException e){
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }
    }



    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub

        MatrixCursor cursor = new MatrixCursor(new String[] { "key", "value"});

     //   Log.i("selection_1","se;kadl;kasdasd");


      if( ports.size()==1  || ports.size()==0 || selection.equals("@") ) {
          if (selection.equals("@") || selection.equals("*")){
              Log.i("@selction", selection);
              String[] files = getContext().fileList();
          //    Log.i("if_loop","it's in if ");
          for (String file : files) {
              // MatrixCursor.RowBuilder builder = cursor.newRow();
              //  builder.add("key",selection);
              String str = "";
              FileInputStream inputStream;

              try {
                  inputStream = getContext().openFileInput(file);
                  int k = inputStream.read();
                  while (k != -1) {
                      str += String.valueOf((char) k);
                      k = inputStream.read();
                  }
                  inputStream.close();
              } catch (Exception e) {
                  Log.e(TAG, "File write failed");
              }
              cursor.addRow(new Object[]{file, str});
              Log.v("query", selection);
          }
          return cursor;
      }
       else{
              Log.i("*selection",selection);
              MatrixCursor.RowBuilder builder = cursor.newRow();
              builder.add("key", selection);
              String str = "";
              FileInputStream inputStream;
              try {
                  inputStream = getContext().openFileInput(selection);
                  int k = inputStream.read();
                  while (k != -1) {
                      str += String.valueOf((char) k);
                      k = inputStream.read();
                  }
                  inputStream.close();
              } catch (Exception e) {
                  Log.e(TAG, "File write failed");
              }
              builder.add("value", str);
              Log.v("query", selection);
              return cursor;
          }
      }
      else if(selection.equals("*")){
          Log.i("selection_is_star","selection.........is............here" );
          String msg="";
          msg+="query";
          msg+="#";
          msg+=myport;
          msg+="#";
          String[] files=getContext().fileList();
          for(String file : files) {
              String str = "";
              FileInputStream inputStream;

              try {
                  inputStream = getContext().openFileInput(file);
                  int k = inputStream.read();
                  while (k != -1) {
                      str += String.valueOf((char) k);
                      k = inputStream.read();
                  }
                  inputStream.close();
              } catch (Exception e) {
                  Log.e(TAG, "File write failed");
              }
              msg+=file;
              msg+="#";
              msg+=str;
              msg+="#";
              Log.v("query", selection);
          }
      //    Log.i("msg_to_send",msg);
          String res="";
          try {
             res= new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg).get();
          } catch (InterruptedException e) {
              e.printStackTrace();
          } catch (ExecutionException e) {
              e.printStackTrace();
          }
      //    Log.i("res_msg",res);
          int i=0,j=0,k=1;
          for( i=0;i<res.length();i++){
              if(res.charAt(i)=='#'){
                  k++;
                  String temp=res.substring(j,i);
                  j=i+1;
                  if(k==3)break;
              }
          }
          i++;
          int count=0;
          String u="";
          String v="";
          for(;i<res.length();i++){
              if(res.charAt(i)=='#'){
                  count++;
                  if(count==1){
                      u= res.substring(j,i);
                      j=i+1;
                  }
                  else if(count==2){
                      v=res.substring(j,i);
                   //   Log.i("key_value_pair",u+v);
                      cursor.addRow(new Object[]{u,v});
                      count=0;
                      j=i+1;
                  }

              }
          }


          return cursor;
      }
       else{
        //  Log.i("selection_is_here","selection......................");

          String hash =selection;

          try {
              hash=genHash(hash);
          } catch (NoSuchAlgorithmException e) {
              e.printStackTrace();
          }

          if(predecessor_id.compareTo(hash)<0 && my_id.compareTo(hash)>0) {
           //   Log.i("ports_size",Integer.toString(ports.size()));
              MatrixCursor.RowBuilder builder = cursor.newRow();
              builder.add("key", selection);
              String str = "";
              FileInputStream inputStream;
              try {
                  inputStream = getContext().openFileInput(selection);
                  int k = inputStream.read();
                  while (k != -1) {
                      str += String.valueOf((char) k);
                      k = inputStream.read();
                  }
                  inputStream.close();
              } catch (Exception e) {
                  Log.e(TAG, "File write failed");
              }
              builder.add("value", str);
              Log.v("query", selection);
              return cursor;
          }
          else{

              try {
                  if (hash.compareTo(ports.get(0)) < 0 || hash.compareTo(ports.get(ports.size()-1))>0) {
                      Log.i("ports_size",Integer.toString(ports.size()));
                      int port_no_1 = map.get(ports.get(0));
                      Log.i("it's here",Integer.toString(port_no_1));
                      Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                              port_no_1);
                      BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                      PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                      String msg ="";
                      msg+="query_1";
                      msg+="#";
                      msg+=selection;
                      msg+="\n";
                      out.println(msg);
                      String ret=in.readLine();

                   //   Log.i("retrun_line",ret);
                      int j=0,k=1;
                      String key_1="";
                      String value_1="";

                      for(int i=0;i<ret.length();i++){
                          if(ret.charAt(i)=='#'){
                              if(k==1){
                                  key_1=ret.substring(j,i);
                                  j=i+1;
                                  k++;
                              }
                              else if(k==2){
                                  value_1 =ret.substring(j,i);
                                  j=i+1;
                                  k++;
                                  break;
                              }

                          }

                      }
                  //    Log.i("key_2",key_1);
                    //  Log.i("value_2",value_1);
                      MatrixCursor.RowBuilder builder = cursor.newRow();
                      builder.add("key", key_1);
                      builder.add("value",value_1);
                      return cursor;
                  }
                  else {
                    //  Log.i("I_am_here","here_1");
                    //  Log.i("ports_size",Integer.toString(ports.size()));
                      for(int i=0;i<ports.size();i++){

                          if(hash.compareTo(ports.get(i))<0){
                             int port_no_1= map.get(ports.get(i));
                           //   Log.i("it's here",Integer.toString(port_no_1));
                              Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                      port_no_1);
                              BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                              PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                              String msg ="";
                              msg+="query_1";
                              msg+="#";
                              msg+=selection;
                              msg+="\n";
                              out.println(msg);
                              String ret=in.readLine();
                           //   Log.i("abhilash",ret);
                              int j=0,k=1;
                              String key_1="";
                              String value_1="";
                              for(i=0;i<ret.length();i++){
                                  if(ret.charAt(i)=='#'){
                                      if(k==1){
                                          key_1=ret.substring(j,i);
                                          j=i+1;
                                          k++;
                                      }
                                      else if(k==2){
                                          value_1 =ret.substring(j,i);
                                          j=i+1;
                                          k++;
                                          break;
                                      }

                                  }

                              }
                              MatrixCursor.RowBuilder builder = cursor.newRow();
                              builder.add("key", key_1);
                              builder.add("value",value_1);
                              return cursor;
                          }

                      }
                  }

              }catch(UnknownHostException e) {
                  Log.e(TAG, "ClientTask UnknownHostException");
              }catch(IOException e){
                  Log.e(TAG, "ClientTask socket IOException");
              }

          }

      }
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
}
