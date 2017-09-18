package edu.usfca.cs.dfs;
import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import edu.usfca.cs.dfs.StorageMessages.*;
import com.google.protobuf.ByteString;

public class Client {
    final private int chunksize = 2 * 1024 * 1024;

    public static void main(String[] args)
    throws Exception{
        Client c = new Client();
        c.writeFile("file.txt");
    }

    public boolean writeFile(String fileName) {
        boolean success = true;
        try {
            Socket controllerSock = new Socket("localhost", 8080);
            List<StoreChunk> chunkList = splitFile(fileName);

            for (StoreChunk sc : chunkList) {
                //for every chunk in this file, send request to controller,
                // then controller will send back a list of nodes for replica of this chunk
                StoreRequestToController srtc = StoreRequestToController.newBuilder()
                        .setFileName(sc.getFileName())
                        .setChunkId(sc.getChunkId())
                        .build();
                srtc.writeDelimitedTo(controllerSock.getOutputStream());
                StoreResponseFromController srfc = StoreResponseFromController.parseDelimitedFrom(controllerSock.getInputStream());
                List<StoreNodeInfo> nodeList = srfc.getInfoList();
                // write to first StoreNode and pass the remain of the list
                StoreNodeInfo targetNode = nodeList.remove(0);
                // setup a new socket to write to storageNode
                Socket storageSock = new Socket(targetNode.getIpaddress(), targetNode.getPort());
                StoreChunk chunk = StoreChunk.newBuilder()
                        .setFileName(fileName)
                        .setChunkId(sc.getChunkId())
                        .setData(sc.getData())
                        .addAllReplicaToStore(nodeList)
                        .build();
                chunk.writeDelimitedTo(storageSock.getOutputStream());

                // wait response from SN
                StoreResponseFromStorage storeResp = StoreResponseFromStorage.parseDelimitedFrom(storageSock.getInputStream());
                success = storeResp.getSuccess();
                storageSock.close();
            }
            controllerSock.close();
        } catch (Exception e) {
            // TODO: log exception
            success = false;
        }
        return success;
    }

    public List<StoreChunk> splitFile(String filename){
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
