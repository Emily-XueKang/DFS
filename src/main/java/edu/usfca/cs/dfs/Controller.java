package edu.usfca.cs.dfs;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import edu.usfca.cs.dfs.StorageMessages.*;

public class Controller {
    final public static int CONTROLLER_PORT = 25100;
    final public static long NODE_INACTIVE_THRESHOLD_MS = 10000;

    private static List<StoreNodeInfo> activeNodes = Collections.synchronizedList(new ArrayList<StoreNodeInfo>());
    private static ConcurrentHashMap<StoreNodeInfo, Long> activeNodesTsMap = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<StoreNodeInfo, Set<SimplechunkInfo>> SNToChunkMap = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, FileMetaData> files = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, ConcurrentHashMap<Integer, ChunkMetaData>> fileChunks =
            new ConcurrentHashMap<>();    //map filename to a map of chunkid--chunkmetadata
    private static ConcurrentHashMap<StoreNodeInfo, Long> activeNodesSpaceMap = new ConcurrentHashMap<>();
    private static long availableSpace = 0;
    private static Random rand = new Random();
    private static Socket socket;

    public static void main(String[] args) {
        System.out.println("Starting controller on port " + CONTROLLER_PORT + "...");
        Thread scanner = new Thread(new Scanner());
        scanner.start();
        ServerSocket serversock = null;
        try {
            serversock = new ServerSocket(CONTROLLER_PORT);
            System.out.println("Controller started");
        } catch (IOException e) {
            e.printStackTrace();
        }

        while (true) {
            try {
                socket = serversock.accept();
                ControllerMessageWrapper msgWrapper
                        = ControllerMessageWrapper.parseDelimitedFrom(socket.getInputStream());
                if (msgWrapper.hasStoreFileMsg()) {
                    handleStoreFile(msgWrapper.getStoreFileMsg());
                } else if (msgWrapper.hasRetrieveFileMsg()) {
                    handleRetrieveFile(msgWrapper.getRetrieveFileMsg());
//                } else if (msgWrapper.hasUpdateReplicaMsg()) {
//                    handleUpdateChunkReplica(msgWrapper.getUpdateReplicaMsg());
                } else if(msgWrapper.hasHeartbeatMsg()){
                    handleHeartBeat(msgWrapper.getHeartbeatMsg());
                } else if(msgWrapper.hasReplicacorruptMsg()){
                    handleRepCorruption(msgWrapper.getReplicacorruptMsg());
                } else if(msgWrapper.hasListfileMsg()){
                    handleFileList();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //inner class for a seperate thread in controller to scan active sn list
    public static class Scanner implements Runnable {
        //update active node list when one node fails, delete the failed node
        @Override
        public void run(){
            //need to scan all data in SN and get the change
            System.out.println("Background scanning thread started");
            while(true) {
                for (Map.Entry<StoreNodeInfo, Long> entry : activeNodesTsMap.entrySet()) {
                    if (System.currentTimeMillis() - entry.getValue() > NODE_INACTIVE_THRESHOLD_MS) {
                        // mark the storage as inactive
                        StoreNodeInfo inactiveNode = entry.getKey();
                        activeNodes.remove(inactiveNode);
                        activeNodesTsMap.remove(inactiveNode);
                        activeNodesSpaceMap.remove(inactiveNode);
                        System.out.println("Inactive node detected at ip " + inactiveNode.getIpaddress() + " port " + inactiveNode.getPort());

                        //1.iterate through all chunks in this dead node
                        Set<SimplechunkInfo> inactiveNodeChunks = SNToChunkMap.get(inactiveNode);
                        for(SimplechunkInfo sci:inactiveNodeChunks){
                            String filename = sci.getFileName();
                            int chunkid = sci.getChunkId();
                            System.out.println("Inactive node file-chunk: " + filename + "-" +chunkid);
                            //2.in fileChunks map, for each chunk that need to be replicated, find its backup nodes
                            ChunkMetaData oldChunkMetadata = fileChunks.get(filename).get(chunkid);
                            List<StoreNodeInfo> old_c_nodes = oldChunkMetadata.getReplicaLocationsList();
                            List<StoreNodeInfo> c_nodes = new ArrayList<>();
                            // remove the inactive node from filechunks map
                            //using .remove() method would fail, so use a for loop to copy all active nodes to a new c_nodes list
                            for(StoreNodeInfo ocn : old_c_nodes){
                                if(!ocn.getIpaddress().equals(inactiveNode.getIpaddress())){
                                    c_nodes.add(ocn);
                                    //System.out.println("Add node " + ocn + " to chunk list of replica backup node list");
                                }
                            }
                            //System.out.println("size of new c_nodes="+c_nodes.size());
                            //c_nodes is a list of node which contains this corrupted chunk in the inactive node
                            //get the source from updated c_nodes list
                            StoreNodeInfo source = c_nodes.get(rand.nextInt(c_nodes.size()));
                            // update the chunkMetadata in filechunks
                            ChunkMetaData newChunkMetadata = oldChunkMetadata.toBuilder()
                                    .clearReplicaLocations()
                                    .addAllReplicaLocations(c_nodes)
                                    .build();
                            fileChunks.get(filename).put(chunkid, newChunkMetadata);

                            // need to exclude the active nodes that already containing this chunk when selecting target
                            ArrayList<StoreNodeInfo> targetPool = new ArrayList<>();
                            for(StoreNodeInfo targetCandidate : activeNodes){
                                if(!c_nodes.contains(targetCandidate)){
                                    targetPool.add(targetCandidate);
                                }
                            }
                            StoreNodeInfo target = targetPool.get(rand.nextInt(targetPool.size()));
                            System.out.println("Source node for replica recovery: ip="+source.getIpaddress()+"port="+source.getPort());
                            System.out.println("Target node for replica recovery: ip="+target.getIpaddress()+"port="+target.getPort());
                            //4.build recover replica message
                            recoverReplicaCmdFromController rrmsg = recoverReplicaCmdFromController.newBuilder()
                                    .setTarget(target)
                                    .setSource(source)
                                    .setReplica(sci)
                                    .build();
                            StorageMessageWrapper msgWrapper = StorageMessageWrapper.newBuilder()
                                    .setRecoverReplicaCmd(rrmsg)
                                    .build();
                            //send to SN by sn socket, empty bad node chunk set after getting response
                            try {
                                Socket snSocket = new Socket(source.getIpaddress(),source.getPort());
                                msgWrapper.writeDelimitedTo(snSocket.getOutputStream());
                                System.out.println("Send recovery command to SN "+source.getIpaddress());
                                recoverReplicaRspFromSN
                                        res = recoverReplicaRspFromSN.parseDelimitedFrom(snSocket.getInputStream());
                                if(!res.getReplicaSuccess()){
                                    System.out.println("Failed to recover replica.");
                                }
                                System.out.println("Recovered replica for file "+filename+"'s chunk "+chunkid);

                                snSocket.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        //after recovery, empty the chunk set of the dead node in SNTochunksMap
                        //avoid ConcurrentModificationException while reading and modifying
                        SNToChunkMap.get(inactiveNode).clear();
                    }
                }
                try {
                    Thread.sleep(NODE_INACTIVE_THRESHOLD_MS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void handleHeartBeat(SNHeartBeat heartbeatMsg){
        List<SimplechunkInfo> snci = heartbeatMsg.getChunksList();
        StoreNodeInfo currentNode = StoreNodeInfo.newBuilder()
                .setIpaddress(heartbeatMsg.getIpaddress())
                .setPort(heartbeatMsg.getPort())
                .build();
        if (!activeNodesTsMap.containsKey(currentNode)) {
            activeNodes.add(currentNode); // new node discovered
            System.out.println("New active node detected at ip: " + currentNode.getIpaddress() + " port: " + currentNode.getPort());
        }
        // update the latest active timestamp to the storage node
        activeNodesTsMap.put(currentNode, System.currentTimeMillis());
        //update latest space records in this storage node
        activeNodesSpaceMap.put(currentNode,heartbeatMsg.getSpace());
        HashSet<SimplechunkInfo> newChunkSet = new HashSet<SimplechunkInfo>(snci);
        if (!SNToChunkMap.containsKey(currentNode)) {
            SNToChunkMap.put(currentNode, newChunkSet);
        } else {
            // merge the newly added chunks with existing chunks
            SNToChunkMap.get(currentNode).addAll(newChunkSet);
        }
        if(newChunkSet.size()!=0){
            System.out.println("In node "+heartbeatMsg.getIpaddress()+" add new chunks:");
            for(SimplechunkInfo c : newChunkSet){
                System.out.println(c.getFileName() + "_" + c.getChunkId() + ";");
            }
        }

        String ipaddr_SN = heartbeatMsg.getIpaddress(); //ip address from SN heartbeat
        int port_SN = heartbeatMsg.getPort(); //port number from SN heartbeat
        //update filechunks metadata with heartbeat msg
        for(SimplechunkInfo i : snci){
            String fileName = i.getFileName();
            int chunkId = i.getChunkId();
            StoreNodeInfo snInfo = StoreNodeInfo.newBuilder()
                    .setIpaddress(ipaddr_SN)
                    .setPort(port_SN)
                    .build();
            if (!fileChunks.containsKey(fileName)) {
                fileChunks.put(fileName, new ConcurrentHashMap<Integer, ChunkMetaData>());
            }
            ConcurrentHashMap<Integer, ChunkMetaData> chunkMap = fileChunks.get(fileName);
            List<StoreNodeInfo> updatedNodes = new ArrayList<StoreNodeInfo>();
            updatedNodes.add(snInfo);
            if (chunkMap.containsKey(chunkId)) {
                List<StoreNodeInfo> existingNodes = chunkMap.get(chunkId).getReplicaLocationsList();
                updatedNodes.addAll(existingNodes);
            }
            ChunkMetaData chunkMetadata = ChunkMetaData.newBuilder()
                    .setChunkId(chunkId)
                    .setFileName(fileName)
                    .addAllReplicaLocations(updatedNodes)
                    .build();

            chunkMap.put(chunkId, chunkMetadata);
            fileChunks.put(fileName, chunkMap);
            System.out.println("Update metadata in filechunks map: ");
            System.out.println("For filename="+fileName+",chunkid="+chunkId+
                    ", add storing node "+ipaddr_SN +" updated nodes size: " +
                    chunkMap.get(chunkId).getReplicaLocationsList().size());
            if (files.get(fileName).getNumOfChunks() == chunkMap.size()) {
                System.out.println("Update metadata in files map.");
                FileMetaData fileMetadata = files.get(fileName).toBuilder()
                        .setIsCompleted(true)
                        .build();
                files.put(fileName, fileMetadata);
            }
        }
    }

    private static void handleRetrieveFile(RetrieveRequestToController retrieveFileMsg) {
        try {
            String fileName = retrieveFileMsg.getFileName();
            System.out.println("Retrieving file: " + fileName);
            FileMetaData response;
            if (fileChunks.containsKey(fileName) && files.get(fileName).getIsCompleted()) {
                // only allow to read when file is completed writing and not corrupted
                FileMetaData metadata = files.get(fileName);
                long fileSize = metadata.getFileSize();
                Collection<ChunkMetaData> chunks = fileChunks.get(fileName).values();
                response = FileMetaData.newBuilder()
                        .setFileName(fileName)
                        .setFileSize(fileSize)
                        .setNumOfChunks(chunks.size())
                        .addAllChunkList(chunks)
                        .setIsCompleted(true)
                        .build();
                response.writeDelimitedTo(socket.getOutputStream());
                System.out.println("Returning file metadata for file: " + fileName);
            } else {
                // file doesn't exist
                response = FileMetaData.newBuilder()
                        .setFileName(fileName)
                        .setIsCompleted(false)
                        .build();
                response.writeDelimitedTo(socket.getOutputStream());
                System.out.println("File doesn't exist " + fileName);
            }
        } catch (IOException e) {
            System.out.println("Failed to handle retrieve file request");
            e.printStackTrace();
        }
    }

    private static void handleStoreFile(StoreRequestToController storeFileMsg) {
        try {
            int randIdx = rand.nextInt(activeNodes.size());
            String fileName = storeFileMsg.getFileName();
            int chunkId = storeFileMsg.getChunkId();
            StoreResponseFromController srfc;
            if (fileChunks.containsKey(fileName) &&
                    fileChunks.get(fileName).containsKey(chunkId)) {
                // fileName + chunkId already exist, return empty storeNodeInfo list
                srfc = StoreResponseFromController.newBuilder().build();
                srfc.writeDelimitedTo(socket.getOutputStream());
            } else {
                List<StoreNodeInfo> selectedNodes = new ArrayList<StoreNodeInfo>();
                selectedNodes.add(activeNodes.get(randIdx));
                selectedNodes.add(activeNodes.get((randIdx + 1) % activeNodes.size()));
                selectedNodes.add(activeNodes.get((randIdx + 2) % activeNodes.size()));

                // a new chunk of a known file
                srfc = StoreResponseFromController.newBuilder()
                        .addAllInfo(selectedNodes)
                        .build();
                srfc.writeDelimitedTo(socket.getOutputStream());

                // construct a chunkMetadata to be store in files as truth record
                ChunkMetaData chunkMetaData = ChunkMetaData.newBuilder()
                        .setChunkId(chunkId)
                        .setFileName(fileName)
                        .addAllReplicaLocations(selectedNodes)
                        .build();
                FileMetaData fileMetaData = null;
                if (!files.containsKey(fileName)) { // a new file chunk to be stored
                    fileMetaData = FileMetaData.newBuilder()
                            .setFileName(fileName)
                            .setFileSize(storeFileMsg.getFileSize())
                            .setNumOfChunks(storeFileMsg.getNumOfChunks())
                            .addChunkList(chunkMetaData)
                            .build();

                } else {
                    fileMetaData = files.get(fileName).toBuilder()
                            .addChunkList(chunkMetaData)
                            .build();
                }
                files.put(fileName, fileMetaData);
            }
        } catch (IOException e) {
            System.out.println("Failed to handle store file request");
            e.printStackTrace();
        }
    }

    public static void handleRepCorruption(replicaCorruptFromSN replicacorruptMsg){
        String fileName = replicacorruptMsg.getFileName();
        int chunkId= replicacorruptMsg.getChunkId();
        StoreNodeInfo corruptRepInSN = replicacorruptMsg.getCorruptChunkInSN();
        List<StoreNodeInfo> snilist = fileChunks.get(fileName).get(chunkId).getReplicaLocationsList();
        StoreNodeInfo replicaSource = null;
        for(StoreNodeInfo s : snilist){
            if(!s.getIpaddress().equals(corruptRepInSN.getIpaddress())){
                replicaSource = s;
                break;
            }
        }
        SimplechunkInfo repChunk = SimplechunkInfo.newBuilder()
                .setFileName(fileName)
                .setChunkId(chunkId)
                .build();
        recoverReplicaCmdFromController recRepCmd = recoverReplicaCmdFromController.newBuilder()
                .setTarget(corruptRepInSN)
                .setSource(replicaSource)
                .setReplica(repChunk)
                .build();
        StorageMessageWrapper msgWrapper = StorageMessageWrapper.newBuilder()
                .setRecoverReplicaCmd(recRepCmd)
                .build();
        boolean rrsucsess = true; //read repair sucessful or not
        try {//communicate with replica source, trigger recovery process
            Socket snRecSocket = new Socket(replicaSource.getIpaddress(),replicaSource.getPort());
            msgWrapper.writeDelimitedTo(snRecSocket.getOutputStream());
            System.out.println("Send recovery command to replica source at "+replicaSource.getIpaddress());
            recoverReplicaRspFromSN
                    res = recoverReplicaRspFromSN.parseDelimitedFrom(snRecSocket.getInputStream());
            if(!res.getReplicaSuccess()){
                System.out.println("Failed to repair replica.");
                rrsucsess = false;
            }
            System.out.println("Repaired replica for file "+fileName+"'s chunk "+chunkId);
            snRecSocket.close();
            //communicate with replica target which had the corrupted file

            readRepairFromCtrl repair = readRepairFromCtrl.newBuilder()
                    .setRepairSuccess(rrsucsess)
                    .build();
            StorageMessageWrapper msgWrapper2 = StorageMessageWrapper.newBuilder()
                    .setReadRepairRsp(repair)
                    .build();
            msgWrapper2.writeDelimitedTo(socket.getOutputStream());
            System.out.println("Controller sent repair result to corrupted node");
            //socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void handleFileList(){
        ArrayList<String> fileNames = new ArrayList<>();
        Set<String> fns = fileChunks.keySet();
        for(String fn:fns){
            fileNames.add(fn);
        }
        Set<Map.Entry<StoreNodeInfo,Long>> spaceEntrys = activeNodesSpaceMap.entrySet();
        availableSpace = 0; //re-calculate the available space
        for(Map.Entry<StoreNodeInfo,Long> ns : spaceEntrys){
            availableSpace+=ns.getValue();
        }
        FileListFromController flresponse = FileListFromController.newBuilder()
                .addAllFilenames(fileNames)
                .setSpace(availableSpace)
                .build();
        try {
            flresponse.writeDelimitedTo(socket.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /* deprecated
    private static void handleUpdateChunkReplica(UpdateChunkReplicaToController updateReplicaMsg) {
        String fileName = updateReplicaMsg.getFileName();
        int chunkId = updateReplicaMsg.getChunkId();
        StoreNodeInfo nodeInfo = updateReplicaMsg.getNodeInfo();
        if (!fileChunks.containsKey(fileName)) {
            fileChunks.put(fileName, new ConcurrentHashMap<>());
        }
        ConcurrentHashMap<Integer, ChunkMetaData> chunkMap = fileChunks.get(fileName);
        ChunkMetaData chunkMetadata;
        if (!chunkMap.containsKey(chunkId)) {
            chunkMetadata = ChunkMetaData.newBuilder()
                    .setChunkId(chunkId)
                    .setFileName(fileName)
                    .addReplicaLocations(nodeInfo)
                    .build();
        } else {
            chunkMetadata = chunkMap.get(chunkId);
            chunkMetadata.toBuilder()
                    .addReplicaLocations(nodeInfo)
                    .build();
        }
        chunkMap.put(chunkId, chunkMetadata);
        if (files.get(fileName).getNumOfChunks() == chunkMap.size()) {
            FileMetaData fileMetadata = files.get(fileName).toBuilder()
                    .setIsCompleted(true)
                    .build();
            files.put(fileName, fileMetadata);
        }
        fileChunks.put(fileName, chunkMap);

        // send updateReplica response back
        UpdateChunkReplicaResponseFromController response = UpdateChunkReplicaResponseFromController.newBuilder()
                .setSuccess(true)
                .build();
        try {
            response.writeDelimitedTo(socket.getOutputStream());
        } catch (IOException e) {
            System.out.println("failed to handle update replica request");
            e.printStackTrace();
        }

    }
    */
}
