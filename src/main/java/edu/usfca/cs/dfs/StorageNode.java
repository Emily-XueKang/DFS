package edu.usfca.cs.dfs;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import edu.usfca.cs.dfs.StorageMessages.*;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import com.google.protobuf.ByteString;


public class StorageNode {
    final public static int STORAGE_PORT = 8082;

    private ServerSocket srvSocket;
    private HashSet<String> localChunks = new HashSet<String>(); //string : filename + chunkid

    public static void main(String[] args) {
        System.out.println("Starting storage node...");
        new StorageNode().start();
    }

    public void start() {
        try {
            srvSocket = new ServerSocket(STORAGE_PORT);
            System.out.println(" storage node started");
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
                } else if (msgWrapper.hasRetrieveChunkMsg()) {
                    RetrieveRequestToStorage request = msgWrapper.getRetrieveChunkMsg();
                    ByteString data = retrieveChunk(request.getFileName(), request.getChunkId());
                    RetrieveResponseFromStorage resp = RetrieveResponseFromStorage.newBuilder()
                            .setData(data)
                            .build();
                    resp.writeDelimitedTo(socket.getOutputStream());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private boolean storeChunk(StoreChunk storeChunkMsg) {
        System.out.println("Storing file name: " + storeChunkMsg.getFileName() + "chunk Id: " + storeChunkMsg.getChunkId());
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
                ControllerMessageWrapper msgWraper = ControllerMessageWrapper.newBuilder()
                        .setUpdateReplicaMsg(updateReq)
                        .build();
                msgWraper.writeDelimitedTo(controllerSock.getOutputStream());
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
                    StorageMessageWrapper msgWrapper = StorageMessageWrapper.newBuilder()
                            .setStoreChunkMsg(chunk)
                            .build();
                    msgWrapper.writeDelimitedTo(storageSock.getOutputStream());
                    storageSock.close();
                    // Don't wait for the response from pipeline writing
                    // i.e. return once the data is write to local successfull
                }
                return true;
            } catch(Exception e){
                // TODO: log exception
            }
        }
        return false;
    }
    public byte[] genChecksum(ByteString data){
        byte[] databyte = new byte[Client.CHUNK_SIZE];
        byte[] MD5data = new byte[16]; //MD5 HASH size = 128 bits
        data.copyTo(databyte,0);
        //Use MD5 algorithm
        try{
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.update(databyte);
            MD5data = md5.digest();
            return MD5data;
        }catch(java.security.NoSuchAlgorithmException e){
            //TODO: Print exception message
            System.out.println("fail to generate md5 for chunk");
        }finally {
            return MD5data;
        }
    }

    private boolean storeChunkLocal(String fileName, int chunkId, ByteString data){
        FileOutputStream fs = null;
        FileOutputStream fsmd5 = null;
        boolean success = true;
        try {
            String chunkFileName = fileName+"_"+chunkId;
            String chunkMD5Name = fileName+"_"+chunkId+"_MD5";
            fs = new FileOutputStream(chunkFileName);
            data.writeTo(fs);
            localChunks.add(chunkFileName);
            fsmd5 = new FileOutputStream(chunkMD5Name);
            byte[] chunkMD5= genChecksum(data);
            fsmd5.write(chunkMD5);
        } catch (IOException ex) {
            // TODO: log exception
            success = false;
        } finally {
            try {fs.close();
                 fsmd5.close();
            } catch (Exception ex) {/*ignore*/}
        }
        return success;
    }

    private ByteString retrieveChunk(String fileName, int chunkId) {
        FileInputStream fs = null;
        FileInputStream fschecksum = null;
        ByteString data = null;
        String chunkFileName = fileName+"_"+chunkId;
        String chunkChecksum = fileName+"_"+chunkId+"_MD5";
        if (!localChunks.contains(chunkFileName)) {
            return null;
        }
        try {
            fs = new FileInputStream(chunkFileName);
            fschecksum = new FileInputStream(chunkChecksum);

            data = ByteString.readFrom(fs,Client.CHUNK_SIZE);
            byte[] checksum_generated = genChecksum(data);
            byte[] checksum_from_disk  = new byte[16];
            fschecksum.read(checksum_from_disk);
            if(!Arrays.equals(checksum_from_disk,checksum_generated)){
                System.out.println("checksum failed, invalid file chunk");
                return null;
            }
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
        return STORAGE_PORT;
    }

}
