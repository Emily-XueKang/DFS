package edu.usfca.cs.dfs;
import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import edu.usfca.cs.dfs.StorageMessages.*;
import com.google.protobuf.ByteString;
import org.apache.commons.cli.*;



public class Client {
    final public static int CHUNK_SIZE = 1024 * 1024;
    public static String CONTROLLER_IP = "bass01";
    private static Options options = new Options();



    public static void main(String[] args)
    throws Exception{
        options.addOption("w", "write", true, "write file");
        options.addOption("r", "read", true, "read file");
        options.addOption("l", "list", false, "list files");

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        String filename;
        Client c = new Client();
        try {
            cmd = parser.parse(options, args);
            if (cmd.hasOption("w")) {
                filename = cmd.getOptionValue("w");
                c.writeFile(filename);
            }
            if (cmd.hasOption("r")) {
                filename = cmd.getOptionValue("r");
                c.retrieveFile(filename);
            }
            if (cmd.hasOption("l")) {
                c.listFiles();
            }
        } catch (Exception e) {
            System.out.println("can't parse command line argument");
        }
    }

    public boolean writeFile(String fileName) {
        boolean success = true;
        try {

            long fileSizeInBytes = new File(fileName).length();
            List<StoreChunk> chunkList = splitFile(fileName);
            for (int chunkind = 0;chunkind<chunkList.size();chunkind++) {
                StoreChunk sc = chunkList.get(chunkind);
                //for every chunk in this file, send request to controller,
                // then controller will send back a list of nodes for replica of this chunk
                Socket controllerSock = new Socket(CONTROLLER_IP, Controller.CONTROLLER_PORT);

                StoreRequestToController srtc = StoreRequestToController.newBuilder()
                        .setFileName(sc.getFileName())
                        .setChunkId(sc.getChunkId())
                        .setNumOfChunks(chunkList.size())
                        .setFileSize(fileSizeInBytes)
                        .build();
                ControllerMessageWrapper ControllerMsgWrapper = ControllerMessageWrapper.newBuilder()
                        .setStoreFileMsg(srtc)
                        .build();
                ControllerMsgWrapper.writeDelimitedTo(controllerSock.getOutputStream());
                System.out.println("Sent request to controller..");
                StoreResponseFromController srfc = StoreResponseFromController.parseDelimitedFrom(controllerSock.getInputStream());
                System.out.println("Get response from controller..");
                List<StoreNodeInfo> nodeList = srfc.getInfoList();
                if (nodeList.isEmpty()) {
                    throw new Exception("No available nodes to store file: " + fileName);
                }
                // write to first StoreNode and pass the remain of the list
                System.out.println("Get" + nodeList.size() + " nodes to store file: ");
                for(StoreNodeInfo sn : nodeList){
                    System.out.print(sn);
                }
                StoreNodeInfo targetNode = nodeList.get(0);
                List<StoreNodeInfo> remainNodes = new ArrayList<>(nodeList.size()-1);
                for (int idx = 1; idx < nodeList.size(); idx++) {
                    remainNodes.add(nodeList.get(idx));
                }
                System.out.print("One node selected as first: " + targetNode);
                // setup a new socket to write to storageNode
                Socket storageSock = new Socket(targetNode.getIpaddress(), targetNode.getPort());
                StoreChunk chunk = StoreChunk.newBuilder()
                        .setFileName(fileName)
                        .setChunkId(sc.getChunkId())
                        .setData(sc.getData())
                        .addAllReplicaToStore(remainNodes)
                        .build();
                StorageMessageWrapper storageMsgWrapper = StorageMessageWrapper.newBuilder()
                        .setStoreChunkMsg(chunk)
                        .build();
                storageMsgWrapper.writeDelimitedTo(storageSock.getOutputStream());

                // wait response from SN
                StoreResponseFromStorage storeResp = StoreResponseFromStorage.parseDelimitedFrom(storageSock.getInputStream());
                success = storeResp.getSuccess();
                System.out.println("Store chunk success: " + success);

                storageSock.close();
                controllerSock.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            success = false;
        }
        return success;
    }

    private List<StoreChunk> splitFile(String filename){
        //where do the files in clients come from?
        int count = 0;
        byte[] buffer = new byte[CHUNK_SIZE];
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
                System.out.println("Spliting " + bytesAmount + " bytes of data into one chunk...");
            }
            System.out.println("File spilitted to " + count + " chunks");

        }catch (IOException e){
            System.out.println("Failed spliting file into chunks");
            e.printStackTrace();
        }
        return chunks;
    }

    private boolean retrieveFile(String fileName) {
        FileMetaData fileMetadata;
        try {
            Socket controllerSock = new Socket("bass01", Controller.CONTROLLER_PORT);
            RetrieveRequestToController rrtc = RetrieveRequestToController.newBuilder()
                    .setFileName(fileName)
                    .build();
            ControllerMessageWrapper request = ControllerMessageWrapper.newBuilder()
                    .setRetrieveFileMsg(rrtc)
                    .build();
            request.writeDelimitedTo(controllerSock.getOutputStream());
            fileMetadata = FileMetaData.parseDelimitedFrom(controllerSock.getInputStream());
            controllerSock.close();

        } catch (IOException e) {
            System.out.println("Fail to query controller Node for fileMetaData ");
            e.printStackTrace();
            return false;
        }
        if (fileMetadata == null || !fileMetadata.getIsCompleted()) {
            System.out.println("File" + fileName + "is corrupted or still being written");
            return false;
        }
        ChunksRetriever chunksRetriever = new ChunksRetriever(fileMetadata);
        chunksRetriever.processChunks();
        chunksRetriever.waitUntilFinished();
        chunksRetriever.shutdown();
        return true;
    }

    public void listFiles(){
        try {
            Socket controllerSock = new Socket("bass01", Controller.CONTROLLER_PORT);
            listFilesFromClient lf = listFilesFromClient.newBuilder()
                    .build();
            ControllerMessageWrapper flrequest = ControllerMessageWrapper.newBuilder()
                    .setListfileMsg(lf)
                    .build();
            flrequest.writeDelimitedTo(controllerSock.getOutputStream());
            FileListFromController flresponse = FileListFromController.parseDelimitedFrom(controllerSock.getInputStream());
            List<String> filenames = flresponse.getFilenamesList();
            long availableSpace = flresponse.getSpace();
            controllerSock.close();
            System.out.println("Files in DFS:");
            for(String fn:filenames){
                System.out.println(fn);
            }
            System.out.println("Space in DFS: "+availableSpace);
        } catch (IOException e) {
            System.out.println("Fail to query controller Node for file list ");
            e.printStackTrace();
        }
    }
}
