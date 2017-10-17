package edu.usfca.cs.dfs;
import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import edu.usfca.cs.dfs.StorageMessages.*;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import com.google.protobuf.ByteString;
import org.apache.commons.cli.*;

import javax.xml.bind.DatatypeConverter;


public class StorageNode {
    public static int STORAGE_PORT = 25101;
    public static int HEARTBEAT_PERIOD_MS = 5000;
    public static String CONTROLLER_IP = "bass01";
    private static Options options = new Options();

    private ServerSocket srvSocket;
    private HashSet<String> localChunks = new HashSet<String>(); //string : filename + chunkid, for checking purpose
    private ConcurrentLinkedQueue<SimplechunkInfo> chunkInfos= new ConcurrentLinkedQueue<SimplechunkInfo>(); //simple chunk info, updated and sent by backgroud thread for heartbeat message
    public static void main(String[] args) {
        System.out.println("Starting storage node...");
        options.addOption("p", "port", true, "port to use");
        options.addOption("c", "controller", true, "controller node ip");

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
            if (cmd.hasOption("p")) {
                STORAGE_PORT = Integer.parseInt(cmd.getOptionValue("p"));
            }
            if (cmd.hasOption("c")) {
                CONTROLLER_IP = cmd.getOptionValue("c");
                System.out.println(CONTROLLER_IP);
            }
        } catch (Exception e) {
            System.out.println("can't parse command line argument");
        }
        StorageNode sn = new StorageNode();
        Thread background = sn.new HeartBeat();
        background.start();
        sn.process();
    }

    //inner class, background thread for heartbeat
    public class HeartBeat extends Thread{
        @Override
        public void run(){
            //need to scan all data in SN and get the change
            while(true){
                File pathfile = new File("/home2/xkang3"); //unix/linux
                long freespace = pathfile.getUsableSpace();
                ArrayList<SimplechunkInfo> ci = new ArrayList<>();
                while(!chunkInfos.isEmpty()){
                    ci.add(chunkInfos.poll());
                } //empty chunkInfos queue, ensure every time we only send the changes in SN
                try {
                    SNHeartBeat heartBeat = SNHeartBeat.newBuilder()
                            .addAllChunks(ci)
                            .setSpace(freespace)
                            .setIpaddress(getHostname()) //may throw UnknownHostException
                            .setPort(getHostPort())
                            .build();
                    ControllerMessageWrapper msgWrapper = ControllerMessageWrapper.newBuilder()
                            .setHeartbeatMsg(heartBeat)
                            .build();
                    Socket controllerSock = new Socket(CONTROLLER_IP, Controller.CONTROLLER_PORT);
                    msgWrapper.writeDelimitedTo(controllerSock.getOutputStream());
                    System.out.println("freespace=="+freespace);
                    System.out.println("Sent heartbeat...");
                } catch (IOException e) {
                    System.out.println("Failed to send update info through heartbeat");
                    e.printStackTrace();
                }
                try {
                    Thread.sleep(HEARTBEAT_PERIOD_MS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public class RequestWorker implements Runnable{
        Socket socket = null;
        StorageMessageWrapper msgWrapper = null;

        public RequestWorker(Socket skt, StorageMessageWrapper msgw){
            socket = skt;
            msgWrapper = msgw;
        }
        public void run(){
            try {
            if (msgWrapper.hasStoreChunkMsg()) {
                boolean success = storeChunk(msgWrapper.getStoreChunkMsg());
                StoreResponseFromStorage resp = StoreResponseFromStorage.newBuilder()
                        .setSuccess(success)
                        .build();
                    resp.writeDelimitedTo(socket.getOutputStream());
            } else if (msgWrapper.hasRetrieveChunkMsg()) {
                RetrieveRequestToStorage request = msgWrapper.getRetrieveChunkMsg();
                ByteString data = retrieveChunk(request.getFileName(), request.getChunkId());
                System.out.println("got data size of "+data.size());
                RetrieveResponseFromStorage resp = RetrieveResponseFromStorage.newBuilder()
                        .setData(data)
                        .build();
                System.out.println("sent data to client.");
                resp.writeDelimitedTo(socket.getOutputStream());
            } else if (msgWrapper.hasRecoverReplicaCmd()) {
                recoverReplicaCmdFromController recoverCommand = msgWrapper.getRecoverReplicaCmd();
                boolean success = recoverReplica(recoverCommand);
                recoverReplicaRspFromSN recoverResponse = recoverReplicaRspFromSN.newBuilder()
                        .setReplicaSuccess(success)
                        .build();
                recoverResponse.writeDelimitedTo(socket.getOutputStream());
            }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void process() {
        try {
            srvSocket = new ServerSocket(STORAGE_PORT);
            System.out.println("Storage node started on port " + STORAGE_PORT);
            while (true) {
                Socket socket = srvSocket.accept();
                StorageMessages.StorageMessageWrapper msgWrapper
                        = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(
                        socket.getInputStream());
                Thread requestworker = new Thread(new RequestWorker(socket,msgWrapper));
                requestworker.start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private boolean storeChunk(StoreChunk storeChunkMsg) {
        int chunkId = storeChunkMsg.getChunkId();
        System.out.println("Storing file name: " + storeChunkMsg.getFileName() + ", chunk Id: " + chunkId);
        String fileName = storeChunkMsg.getFileName();
        boolean success = storeChunkLocal(fileName, chunkId, storeChunkMsg.getData());

        if (success) {
            System.out.println("Stored file name: " + storeChunkMsg.getFileName() + ", chunk Id: " + chunkId + " successfully");
            //update chunkInfos after store
            SimplechunkInfo ci = SimplechunkInfo.newBuilder()
                    .setChunkId(chunkId)
                    .setFileName(fileName)
                    .build();
            chunkInfos.offer(ci);

            try {
                // after write to local sync with controller node -- deprecated
                /* do not update replica for each storage, instead, us the chunkinfo queue to keep track of updates and sent
                via heartbeat msg
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
                    System.out.println("Failed to sync replica write success with Controller Node");
                    return false;
                }
                System.out.println("Succeed to sync replica write success with Controller Node");
                */
                //the pipeline replication process
                List<StoreNodeInfo> nodeList = storeChunkMsg.getReplicaToStoreList();
                if (nodeList != null && !nodeList.isEmpty()) {
                    StoreNodeInfo targetNode = nodeList.get(0);
                    List<StoreNodeInfo> remainNodes = new ArrayList<>(nodeList.size()-1);
                    for (int idx = 1; idx < nodeList.size(); idx++) { //remove first node which is nodeList.get(0), continue the pipeline from the second node which is nodeList.get(1)
                        remainNodes.add(nodeList.get(idx));
                    }
                    System.out.println("Remaining nodes size in pipeline: " + remainNodes.size());

                    // setup a new socket to write to storageNode
                    Socket storageSock = new Socket(targetNode.getIpaddress(), targetNode.getPort());
                    StoreChunk chunk = StoreChunk.newBuilder()
                            .setFileName(storeChunkMsg.getFileName())
                            .setChunkId(storeChunkMsg.getChunkId())
                            .setData(storeChunkMsg.getData())
                            .addAllReplicaToStore(remainNodes)
                            .build();
                    StorageMessageWrapper msgWrapper = StorageMessageWrapper.newBuilder()
                            .setStoreChunkMsg(chunk)
                            .build();
                    msgWrapper.writeDelimitedTo(storageSock.getOutputStream());
                    System.out.println("Forwarding to node: " + targetNode.getIpaddress() + ":" + targetNode.getPort());
                    storageSock.close();
                    // Don't wait for the response from pipeline writing
                    // i.e. return once the data is write to local successfully
                }
                return true;
            } catch(Exception e){
                e.printStackTrace();
            }
        }
        return false;
    }
    //replica recovery method, to target node using pipeline
    public boolean recoverReplica(recoverReplicaCmdFromController rrcmsg){
        StoreNodeInfo target = rrcmsg.getTarget();
        SimplechunkInfo sci = rrcmsg.getReplica();
        ByteString replicaData = retrieveChunk(sci.getFileName(),sci.getChunkId());
        try {
            Socket storageSock = new Socket(target.getIpaddress(), target.getPort());
            StoreChunk chunk = StoreChunk.newBuilder()
                    .setFileName(sci.getFileName())
                    .setChunkId(sci.getChunkId())
                    .setData(replicaData)
                    .build();
            StorageMessageWrapper msgWrapper = StorageMessageWrapper.newBuilder()
                    .setStoreChunkMsg(chunk)
                    .build();
            System.out.println("recovery socket connected with "+target.getIpaddress());
            msgWrapper.writeDelimitedTo(storageSock.getOutputStream());
            System.out.println("Sent replica to recovery target SN " + target.getIpaddress()+" at port "+target.getPort());
            StoreResponseFromStorage storeResp = StoreResponseFromStorage.parseDelimitedFrom(storageSock.getInputStream());
            storageSock.close();
            boolean recoverSuccess = storeResp.getSuccess();
            System.out.println("Recover chunk success: " + recoverSuccess);
            //then, send replica recovery execution response to controller
            Socket replysocket = new Socket(CONTROLLER_IP, Controller.CONTROLLER_PORT);
            recoverReplicaRspFromSN response = recoverReplicaRspFromSN.newBuilder()
                    .setReplicaSuccess(recoverSuccess)
                    .build();
            response.writeDelimitedTo(replysocket.getOutputStream());
            System.out.println("Sent recovery response to controller");
            replysocket.close();
            return true;
            } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public byte[] genChecksum(ByteString data){
        byte[] databyte = new byte[Client.CHUNK_SIZE];
        byte[] MD5data = new byte[1024]; //MD5 HASH size = 128 bits
        data.copyTo(databyte,0);
        //Use MD5 algorithm
        try{
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.update(databyte);
            MD5data = md5.digest();
            return MD5data;
        }catch(java.security.NoSuchAlgorithmException e){
            System.out.println("Fail to generate md5 for chunk");
        }finally {
            return MD5data;
        }
    }

    private boolean storeChunkLocal(String fileName, int chunkId, ByteString data){
        String chunkFileName = fileName+"_"+chunkId;
        String chunkMD5Name = fileName+"_"+chunkId+"_MD5.txt";
        FileOutputStream fs = null;
        //PrintWriter fsmd5 = null;
        FileOutputStream fsmd5 = null;
        boolean success = true;
        try {
            fsmd5 = new FileOutputStream(chunkMD5Name);
            fs = new FileOutputStream(chunkFileName);
            data.writeTo(fs);
            localChunks.add(chunkFileName);
            byte[] chunkMD5= genChecksum(data);
            fsmd5.write(chunkMD5);
            //String md5string = DatatypeConverter.printHexBinary(chunkMD5);
            //fsmd5.write(md5string);
        } catch (IOException ex) {
            System.out.println(ex);
            success = false;
        } finally {
            try {fs.close();
                fsmd5.close();
            } catch (Exception ex) {
                System.out.println(ex);
            }
        }
        return success;
    }

    private ByteString retrieveChunk(String fileName, int chunkId) {
        FileInputStream fs = null;
        FileInputStream fschecksum = null;
        ByteString data = null;
        String chunkFileName = fileName+"_"+chunkId;
        String chunkChecksum = fileName+"_"+chunkId+"_MD5.txt";
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
            if(!Arrays.equals(checksum_from_disk,checksum_generated)) {
                data = null;//current data corrupted
                System.out.println("Checksum failed, invalid file chunk");
                //send replica corrupt msg to controller
                StoreNodeInfo sni = StoreNodeInfo.newBuilder()
                        .setIpaddress(getHostname())
                        .setPort(getHostPort())
                        .build();
                replicaCorruptFromSN repCorruptMsg = replicaCorruptFromSN.newBuilder()
                        .setFileName(fileName)
                        .setChunkId(chunkId)
                        .setCorruptChunkInSN(sni)
                        .build();
                ControllerMessageWrapper msgWrapper = ControllerMessageWrapper.newBuilder()
                        .setReplicacorruptMsg(repCorruptMsg)
                        .build();
                Socket contrlSock = new Socket(CONTROLLER_IP, Controller.CONTROLLER_PORT);
                msgWrapper.writeDelimitedTo(contrlSock.getOutputStream());
                System.out.println("Need to recover chunk "+fileName+"_"+chunkId+" in node "+sni.getIpaddress()+" at port "+sni.getPort());
                System.out.println("Sent replica corrupt msg to controller");
                StorageMessageWrapper msgWrapper2 = StorageMessageWrapper
                        .parseDelimitedFrom(contrlSock.getInputStream());
                readRepairFromCtrl resp = msgWrapper2.getReadRepairRsp();
                System.out.println("Got response from Controler");
                boolean recovered = resp.getRepairSuccess();
                if(recovered){
                    //data = ByteString.readFrom(fs,Client.CHUNK_SIZE);
                    System.out.println("Recovered == " + recovered);
                }
                contrlSock.close();
            }
            else{
                System.out.println("Checksum succeed");
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
        return STORAGE_PORT;
    }
}
