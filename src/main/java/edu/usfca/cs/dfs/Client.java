package edu.usfca.cs.dfs;
import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ByteString;

public class Client {
    final private int chunksize = 2 * 1024 * 1024;

    public static void main(String[] args)
    throws Exception{
        Client c = new Client();
        Socket sock = new Socket("localhost", 8080);

        List<StorageMessages.StoreChunk> chunkList = c.splitfile("file.text");
        for(StorageMessages.StoreChunk sc : chunkList){
            StorageMessages.StoreRequestToController srtc =
                    StorageMessages.StoreRequestToController.newBuilder()
                            .setFileName(sc.getFileName())
                            .setChunkId(sc.getChunkId())
                            .build();
            srtc.writeDelimitedTo(sock.getOutputStream());
            //after send storage request to controller, how to expect response
            StorageMessages.StoreResponseFromController srfc = StorageMessages.StoreResponseFromController.parseFrom(sock.getInputStream());
        } //for every chunk in this file, send request to controller, then controller will send back a list of nodes for replica of this chunk


        sock.close();



    }

    public List<StorageMessages.StoreChunk> splitfile(String filename){
        //where do the files in clients come from?
        int count = 0;
        byte[] buffer = new byte[chunksize];
        List<StorageMessages.StoreChunk> chunks = new ArrayList<StorageMessages.StoreChunk>();
        File file = new File(filename);
        try (FileInputStream fis = new FileInputStream(file);
             BufferedInputStream bis = new BufferedInputStream(fis)) {

            int bytesAmount = 0;
            while ((bytesAmount = bis.read(buffer)) > 0) {
                //write each chunk of data
                ByteString chunkdata = ByteString.copyFrom(buffer);
                StorageMessages.StoreChunk storeChunkMsg
                        = StorageMessages.StoreChunk.newBuilder()
                        .setFileName(filename)
                        .setChunkId(count)
                        .setData(chunkdata)
                        .build();
                count++;
                chunks.add(storeChunkMsg);
            }
        }catch (IOException e){

        }
        return chunks;
    }


}
