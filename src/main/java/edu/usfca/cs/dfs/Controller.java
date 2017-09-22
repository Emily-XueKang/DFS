package edu.usfca.cs.dfs;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import edu.usfca.cs.dfs.StorageMessages.*;

public class Controller {
    final public static int CONTROLLER_PORT = 8081;

    private static ArrayList<StoreNodeInfo> activeNodes = new ArrayList<StoreNodeInfo>();
    private static ConcurrentHashMap<String, FileMetaData> files = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, ConcurrentHashMap<Integer, ChunkMetaData>> fileChunks =
            new ConcurrentHashMap<String, ConcurrentHashMap<Integer, ChunkMetaData>>(); //map filename to a map of chunkid--chunkmetadata
    private static Random rand = new Random();
    private static Socket socket;
    public static void main(String[] args) {
        System.out.println("Starting controller...");
        // TODO: Load data structures to memory, active nodes discovery
        StoreNodeInfo testNode = StoreNodeInfo.newBuilder()
                .setIpaddress("localhost")
                .setPort(8082)
                .build();
        activeNodes.add(testNode);
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
                } else if (msgWrapper.hasUpdateReplicaMsg()) {
                    handleUpdateChunkReplica(msgWrapper.getUpdateReplicaMsg());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void handleRetrieveFile(RetrieveRequestToController retrieveFileMsg) {
        try {
            String fileName = retrieveFileMsg.getFileName();
            System.out.println("Retrieving file: " + fileName);
            FileMetaData response;
            if (files.containsKey(fileName) && files.get(fileName).getIsCompleted()) {
                // only allow to read when file is completed writing and not corrupted
                response = files.get(fileName);
                response.writeDelimitedTo(socket.getOutputStream());
                System.out.println("returning file metadata for file: " + fileName);
            } else {
                // file doesn't exist
                response = FileMetaData.newBuilder()
                        .setFileName(fileName)
                        .setIsCompleted(false)
                        .build();
                response.writeDelimitedTo(socket.getOutputStream());
                System.out.println("file doesn't exist " + fileName);
            }
        } catch (IOException e) {
            System.out.println("failed to handle retrieve file request");
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

                // construct a chunkMetada to be store in files as truth record
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
            System.out.println("failed to handle store file request");
            e.printStackTrace();
        }
    }

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
            // TODO: may need to check if the same nodeInfo already exists in the chunkMap
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
}
