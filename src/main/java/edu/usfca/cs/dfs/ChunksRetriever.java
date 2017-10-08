package edu.usfca.cs.dfs;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.StorageMessages.*;

/**
 * Created by xuekang on 9/19/17.
 */
public class ChunksRetriever {
    private final WorkQueue queue = new WorkQueue();
    private volatile int numTasks; // how many runnable tasks are pending
    private FileMetaData fileMetaData;
    private ConcurrentHashMap<Integer, ByteString> dataMap = new ConcurrentHashMap<Integer, ByteString>();
    /**
     * Initializes the total bytes found to 0.
     */
    public ChunksRetriever(FileMetaData fileData) {
        this.numTasks = 0;
        fileMetaData = fileData;
    }

    /**
     *  Wait for all pending work to finish
     */
    public synchronized void waitUntilFinished() {
        while (numTasks > 0) {
            try {
                wait();
            } catch (InterruptedException e) {
                System.out.println("Got interrupted while waiting for pending work to finish, " + e.getStackTrace());
            }
        }
    }

    /** Increment the number of tasks */
    public synchronized void incrementTasks() {
        numTasks++;
    }

    /** Decrement the number of tasks.
     * Call notifyAll() if no pending work left.
     */
    public synchronized void decrementTasks() {
        numTasks--;
        if (numTasks <=0 )
            notifyAll();
    }

    /** Wait until there is no pending work, then shutdown the queue */
    public synchronized void shutdown() {
        waitUntilFinished();
        queue.shutdown();
        combineChunks();
    }

    private void combineChunks() {
        FileOutputStream fs = null;
        String outputFileName =  fileMetaData.getFileName() + "_received";
        int numOfChunks = fileMetaData.getNumOfChunks();
        try {
            for (int i = 0; i < numOfChunks - 1; i++) {
                ByteString data = dataMap.get(i);
                fs = new FileOutputStream(outputFileName, true);
                data.writeTo(fs);
            }
            // handle last chunk specially since the chunk is likely not full of useful data
            int lastChunkSize = (int)(fileMetaData.getFileSize() - Client.CHUNK_SIZE * (numOfChunks - 1));
            ByteString data = dataMap.get(numOfChunks - 1);
            byte[] lastBytes = new byte[lastChunkSize];
            data.copyTo(lastBytes, 0, 0, lastChunkSize);
            System.out.println("last chunk size: " + lastChunkSize);
            fs = new FileOutputStream(outputFileName, true);
            fs.write(lastBytes);
            System.out.println("Chunks combined to file successfully");
        } catch (IOException e) {
            System.out.println("fail to combine chunks");
        } finally {
            try {fs.close();} catch (Exception e){}
        }
    }
    /**
     * Processes all the chunks: transfer from storage node and store locally
     *
     */
    public void processChunks() {
        for (ChunkMetaData chunkMetaData : fileMetaData.getChunkListList()) {
            queue.execute(new ChunkWorker(chunkMetaData));
        }
    }

    /**
     * ChunkWorker
     * An inner class that represents a piece of Runnable work.
     * In the run() method, it call/transfer the Storage Node for a specific chunkId
     * and store to a local file
     */
    public class ChunkWorker implements Runnable {
        private String fileName;
        private int chunkId;
        private String ipSR;
        private int portSR;

        ChunkWorker(ChunkMetaData chunkMeta) {
            fileName = chunkMeta.getFileName();
            chunkId = chunkMeta.getChunkId();
            ipSR = chunkMeta.getReplicaLocations(0).getIpaddress();
            portSR = chunkMeta.getReplicaLocations(0).getPort();
            incrementTasks();
        }

        @Override
        public void run() {
            String chunkFileName = fileName + "_" + chunkId;

            try {
                Socket storageSock = new Socket(ipSR, portSR);
                RetrieveRequestToStorage request = RetrieveRequestToStorage.newBuilder()
                        .setChunkId(chunkId)
                        .setFileName(fileName)
                        .build();
                StorageMessageWrapper msgWrapper = StorageMessageWrapper.newBuilder()
                        .setRetrieveChunkMsg(request)
                        .build();
                msgWrapper.writeDelimitedTo(storageSock.getOutputStream());
                RetrieveResponseFromStorage resp =
                        RetrieveResponseFromStorage.parseDelimitedFrom(storageSock.getInputStream());

                ByteString data = resp.getData();
                dataMap.put(chunkId, data); //retrieve to client memory, then combine from memory to disk
                System.out.println("Retrieved chunk: " + chunkFileName);
            } catch (IOException e) {
               System.out.println("Unable to process chunk: " + chunkFileName);
               System.out.println(e.getStackTrace());
            }
            finally {
                decrementTasks(); // done with this task
            }
        }
    }
}
