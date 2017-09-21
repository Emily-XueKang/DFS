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
        try {
            for (int i = 0; i < fileMetaData.getNumOfChunks(); i++) {
                ByteString data = dataMap.get(i);
                fs = new FileOutputStream(fileMetaData.getFileName(), true);
                data.writeTo(fs);
            }
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
     * In the run() method, it call/transfer the Storage Node for a specify chunkId
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
//            FileOutputStream fs = null;
            String chunkFileName = fileName + "_" + chunkId;

            try {
                Socket storageSock = new Socket(ipSR, portSR);
                RetrieveRequestToStorage request = RetrieveRequestToStorage.newBuilder()
                        .setChunkId(chunkId)
                        .setFileName(fileName)
                        .build();
                request.writeDelimitedTo(storageSock.getOutputStream());
                RetrieveResponseFromStorage resp =
                        RetrieveResponseFromStorage.parseDelimitedFrom(storageSock.getInputStream());

                ByteString data = resp.getData();
//                fs = new FileOutputStream(chunkFileName);
//                data.writeTo(fs);
                dataMap.put(chunkId, data); //retrieve to client memory, then combine from memory to disk
            } catch (IOException e) {
               System.out.println("Unable to process chunk: {}" + chunkFileName);
               System.out.println(e.getStackTrace());
            }
            finally {
                decrementTasks(); // done with this task
//                try {fs.close();} catch (Exception ex) {/*ignore*/}
            }

        }
    }
}
