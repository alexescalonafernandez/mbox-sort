package io.github.alexescalonafernandez.mbox.sort.task;

import java.io.*;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Created by alexander.escalona on 27/09/2018.
 */
public class WriteTask implements Runnable{
    private final File mboxFile;
    private final Consumer<Integer> progressNotifier;
    private final Supplier<List<File>> sortedFilesSupplier;
    private PrintStream writer;
    public WriteTask(File mboxFile, Consumer<Integer> progressNotifier, Supplier<List<File>> sortedFilesSupplier) {
        this.mboxFile = mboxFile;
        this.progressNotifier = progressNotifier;
        this.sortedFilesSupplier = sortedFilesSupplier;
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
        Optional.ofNullable(sortedFilesSupplier)
                .map(listSupplier -> listSupplier.get()).ifPresent(
                        files -> files.stream().forEach(file -> {
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
                        })
        );
        Optional.ofNullable(writer).ifPresent(pw -> pw.close());
    }
}
