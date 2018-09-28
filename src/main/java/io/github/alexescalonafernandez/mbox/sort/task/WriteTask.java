package io.github.alexescalonafernandez.mbox.sort.task;

import java.io.*;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Created by alexander.escalona on 27/09/2018.
 */
public class WriteTask implements Runnable{
    private final File mboxFile;
    private final Consumer<Integer> progressNotifier;
    private final List<File> sortedFiles;
    private PrintStream writer;
    public WriteTask(File mboxFile, Consumer<Integer> progressNotifier, List<File> sortedFiles) {
        this.mboxFile = mboxFile;
        this.progressNotifier = progressNotifier;
        this.sortedFiles = sortedFiles;
        this.writer = null;
    }

    @Override
    public void run() {
        final byte[] chunk = new byte[1024];
        final Consumer<Integer> byteReadsConsumer = byteReads -> {
            if(!Optional.ofNullable(writer).isPresent()) {
                try {
                    writer = new PrintStream(new FileOutputStream(mboxFile), true);
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            }
            writer.write(chunk, 0, byteReads);
            progressNotifier.accept(byteReads);
        };
        Optional.ofNullable(sortedFiles)
                .ifPresent(files -> files.stream().forEach(file -> {
                    RandomAccessFile reader = null;
                    try {
                        reader = new RandomAccessFile(file, "r");
                        while (reader.getFilePointer() < reader.length()) {
                            byteReadsConsumer.accept(reader.read(chunk));
                        }
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    finally {
                        try {
                            if(reader != null)
                                reader.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }));
        Optional.ofNullable(writer).ifPresent(pw -> pw.close());
    }
}
