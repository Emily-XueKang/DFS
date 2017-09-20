package edu.usfca.cs.dfs;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.StorageMessages.*;
import edu.usfca.cs.dfs.Controller;

import java.util.HashSet;
import java.util.List;

public class StorageNode {
    final public static int STORAGE_PORT = 8082;

    private ServerSocket srvSocket;
    private HashSet<String> localChunks =  new HashSet<String>();

    public static void main(String[] args) throws Exception {
        String hostname = getHostname();
        System.out.println("Starting storage node on " + hostname + "...");
        new StorageNode().start();
    }

    public void start() throws Exception {
        srvSocket = new ServerSocket(STORAGE_PORT);
        System.out.println("Listening...");
        while (true) {
            Socket socket = srvSocket.accept();
            StorageMessages.StorageMessageWrapper msgWrapper
                    = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(
                    socket.getInputStream());
            if (msgWrapper.hasStoreChunkMsg()) {
                boolean success = storeChunk(msgWrapper.getStoreChunkMsg());
                StoreResponseFromStorage resp = StoreResponseFromStorage.newBuilder()
                        .setSuccess(success)
                        .build();
                resp.writeDelimitedTo(socket.getOutputStream());
            } else if (msgWrapper.hasRetrieveChunkMsg()){
                RetrieveRequestToStorage request = msgWrapper.getRetrieveChunkMsg();
                ByteString data = retrieveChunk(request.getFileName(), request.getChunkId());
                RetrieveResponseFromStorage resp = RetrieveResponseFromStorage.newBuilder()
                        .setData(data)
                        .build();
                resp.writeDelimitedTo(socket.getOutputStream());
            }
        }
    }

    private boolean storeChunk(StoreChunk storeChunkMsg) {
        System.out.println("Storing file name: " + storeChunkMsg.getFileName());
        int chunkId = storeChunkMsg.getChunkId();
        String fileName = storeChunkMsg.getFileName();
        boolean success = storeChunkLocal(fileName, chunkId, storeChunkMsg.getData());
        if (success) {
            // after write to local sync with controller node
            try {
                Socket controllerSock = new Socket("localhost", Controller.CONTROLLER_PORT);
                StoreNodeInfo nodeInfo = StoreNodeInfo.newBuilder()
                        .setIpaddress(getHostname())
                        .setPort(getHostPort())
                        .build();
                UpdateChunkReplicaToController updateReq = UpdateChunkReplicaToController.newBuilder()
                        .setChunkId(chunkId)
                        .setFileName(fileName)
                        .setNodeInfo(nodeInfo)
                        .build();
                updateReq.writeDelimitedTo(controllerSock.getOutputStream());
                UpdateChunkReplicaResponseFromController res =
                        UpdateChunkReplicaResponseFromController.parseDelimitedFrom(controllerSock.getInputStream());
                if (!res.getSuccess()) {
                    return false;
                }

                List<StoreNodeInfo> nodeList = storeChunkMsg.getReplicaToStoreList();
                if (nodeList != null && nodeList.isEmpty()) {
                    StoreNodeInfo targetNode = nodeList.remove(0);
                    // setup a new socket to write to storageNode
                    Socket storageSock = new Socket(targetNode.getIpaddress(), targetNode.getPort());
                    StoreChunk chunk = StoreChunk.newBuilder()
                            .setFileName(storeChunkMsg.getFileName())
                            .setChunkId(storeChunkMsg.getChunkId())
                            .setData(storeChunkMsg.getData())
                            .addAllReplicaToStore(nodeList)
                            .build();
                    chunk.writeDelimitedTo(storageSock.getOutputStream());
                    storageSock.close();
                    // Don't wait for the response from pipeline writing
                    // i.e. return once the data is write to local successfully
                }
                return true;
            } catch(Exception e){
                // TODO: log exception
            }
        }
        return false;
    }

    private boolean storeChunkLocal(String fileName, int chunkId, ByteString data){
        FileOutputStream fs = null;
        boolean success = true;
        try {
            String chunkFileName = fileName+"_"+chunkId;
            fs = new FileOutputStream(chunkFileName);
            data.writeTo(fs);
            localChunks.add(chunkFileName);
        } catch (IOException ex) {
            // TODO: log exception
            success = false;
        } finally {
            try {fs.close();} catch (Exception ex) {/*ignore*/}
        }
        return success;
    }

    private ByteString retrieveChunk(String fileName, int chunkId) {
        FileInputStream fs = null;
        ByteString data = null;
        String chunkFileName = fileName+"_"+chunkId;
        if (!localChunks.contains(chunkFileName)) {
            return null;
        }
        try {
            fs = new FileInputStream(chunkFileName);
            byte[] buffer = new byte[Client.CHUNK_SIZE];
            fs.read(buffer);
            data = ByteString.copyFrom(buffer);
        } catch (IOException e) {

        } finally {
            try {fs.close();} catch (Exception ex) {/*ignore*/}
        }
        return data;
    }

    /**
     * Retrieves the short host name of the current host.
     *
     * @return name of the current host
     */
    private static String getHostname() throws UnknownHostException {
        return InetAddress.getLocalHost().getHostName();
    }

    private static int getHostPort() {
        // TODO: make port number const
        return 8080;
    }

}
