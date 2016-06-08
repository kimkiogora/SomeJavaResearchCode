/**
 * Author: Kim Kiogora <kimkiogora@gmail.com>
 * Usage : Read Large Files
*/
package com.kiogora.memorymap;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 *
 * @author kim
 */
public class MemoryMap {

    /**
     * Constructor.
     */
    public MemoryMap() {
    }

    /**
     * update the file.
     *
     * @param logging
     * @param file
     * @param data
     * @throws java.io.FileNotFoundException
     */
    public static void updateFile(final Logging logging, final String file,
            final String data) throws FileNotFoundException, IOException {
        File myFile;
        myFile = new File(file);
        FileChannel fc = new RandomAccessFile(myFile, "rw").getChannel();
        MappedByteBuffer out 
                = fc.map(FileChannel.MapMode.READ_WRITE, 0, data.length());
        byte[] b = data.getBytes();
        out.put(b);
    }

    /**
     * Read from memory mapped file.
     *
     * @param logging
     * @param file
     * @return
     * @throws java.io.IOException
     */
    public static String getMemoryContents(final Logging logging,
            final String file) throws IOException {
        StringBuilder builder = new StringBuilder();
        File mfile = new File(file);
        FileChannel fileChannel = new RandomAccessFile(mfile, "r").getChannel();
        MappedByteBuffer buffer;
        buffer = fileChannel.map(
                FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
        // print contents of ByteBuffer //
        while (buffer.hasRemaining()) {
            builder.append((char) buffer.get());
        }
        return builder.toString();
    }
}
