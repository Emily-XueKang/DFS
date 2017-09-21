package edu.usfca.cs.dfs;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import edu.usfca.cs.dfs.StorageMessages.*;

public class Controller {
    final public static int CONTROLLER_PORT = 8081;

    private static ArrayList<StoreNodeInfo> activeNodes = new ArrayList<StoreNodeInfo>();
    private static ArrayList<String> files = new ArrayList<String>();
    private static HashMap<String, HashMap<Integer, ChunkMetaData>> fileChunks =
            new HashMap<String, HashMap<Integer, ChunkMetaData>>(); //map filename to a map of chunkid--chunkmetadata
    private static Random rand = new Random();
    private static Socket socket;
    public static void main(String[] args) {
        // TODO: Populate the data structures, active nodes discovery
        ServerSocket serversock = null;
        try {
            serversock = new ServerSocket(CONTROLLER_PORT);
            System.out.println("Starting controller...");
        } catch (IOException e) {
            e.printStackTrace();
        }

        while (true) {
            try {
                socket = serversock.accept();
                ControllerMessageWrapper msgWrapper
                        = ControllerMessageWrapper.parseFrom(
                        socket.getInputStream());
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
            HashMap<Integer, ChunkMetaData> chunks = fileChunks.get(fileName);
            System.out.println("Retrieving file: " + fileName);
            FileMetaData response;
            if (chunks != null) {
                response = FileMetaData.newBuilder()
                        .setFileName(fileName)
                        .addAllChunkList(chunks.values())
                        .setNumOfChunks(chunks.size())
                        .build();
                response.writeDelimitedTo(socket.getOutputStream());
            } else {
                response = FileMetaData.newBuilder()
                        .setFileName(fileName)
                        .build();
                response.writeDelimitedTo(socket.getOutputStream());
            }
        } catch (IOException e) {
            System.out.println("failed to handle retrieve file request");
            e.printStackTrace();
        }
    }

    private static void handleStoreFile(StoreRequestToController storeFileMsg) {
        try {
            int randIdx = rand.nextInt();
            String fileName = storeFileMsg.getFileName();
            int chunkId = storeFileMsg.getChunkId();
            StoreResponseFromController srfc;
            if (fileChunks.containsKey(fileName) &&
                    fileChunks.get(fileName).containsKey(chunkId)) {
                // fileName + chunkId already exist, return empty storeNodeInfo list
                srfc = StoreResponseFromController.newBuilder().build();
                srfc.writeDelimitedTo(socket.getOutputStream());
            } else {
                srfc = StoreResponseFromController.newBuilder()
                        .setInfo(0, activeNodes.get(randIdx))
                        .setInfo(1, activeNodes.get(randIdx + 1))
                        .setInfo(2, activeNodes.get(randIdx + 2))
                        .build();
                System.out.println("Storing file name: " + storeFileMsg.getFileName());
                srfc.writeDelimitedTo(socket.getOutputStream());
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
            fileChunks.put(fileName, new HashMap<Integer, ChunkMetaData>());
        }
        HashMap<Integer, ChunkMetaData> chunkMap = fileChunks.get(fileName);
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
            chunkMetadata.toBuilder() // TODO: correct to use toBuilder(), will that store the same existing data
                    .addReplicaLocations(nodeInfo)
                    .build();
        }
        chunkMap.put(chunkId, chunkMetadata);
        fileChunks.put(fileName, chunkMap);

        // TODO: send updateReplica response back
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
