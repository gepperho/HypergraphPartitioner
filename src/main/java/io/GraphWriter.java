package io;

import model.Partition;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class GraphWriter {

    /**
     * writes the given partitions to the hdd
     * @param outputDirectory output directory
     * @param fileName file name of the partitions (id will be added)
     */
    public static void write(Path outputDirectory, String fileName, List<Partition> partitions) {

        List<Thread> threadPool = new ArrayList<>(partitions.size());

        int counter = 0;
        for (Partition partition: partitions) {
            // start thread...
            PartitionWriter writer = new PartitionWriter(outputDirectory, fileName, counter, partition, PartitionWriter.OutputFormats.vList);
            threadPool.add(new Thread(writer));

            counter++;
        }

        // start writing to disc
        threadPool.forEach(Thread::start);

        // wait until writing to disc is finished
        for (Thread writerThread: threadPool) {
            try {
                writerThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
