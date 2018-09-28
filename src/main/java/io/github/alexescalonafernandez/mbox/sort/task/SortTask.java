package io.github.alexescalonafernandez.mbox.sort.task;

import java.io.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by alexander.escalona on 27/09/2018.
 */
public class SortTask implements Runnable{
    private final File mboxFile;
    private final Consumer<Integer> progressNotifier;
    private final BiConsumer<Long, File> sortFileNotifier;
    private PrintWriter writer;
    public SortTask(File mboxFile, Consumer<Integer> progressNotifier, BiConsumer<Long, File> sortFileNotifier) {
        this.mboxFile = mboxFile;
        this.progressNotifier = progressNotifier;
        this.sortFileNotifier = sortFileNotifier;
        this.writer = null;
    }

    public void run() {
        RandomAccessFile raf = null;
        Pattern pattern = Pattern.compile("[^\\n]*\\n", Pattern.MULTILINE);
        Matcher matcher;
        byte[] chunk = new byte[1024];
        int byteReads, end = 0;
        StringBuilder buffer = new StringBuilder();
        try {
            raf = new RandomAccessFile(mboxFile, "r");
            while (raf.getFilePointer() < raf.length()) {
                byteReads = raf.read(chunk);
                buffer.append(new String(chunk, 0, byteReads));
                matcher = pattern.matcher(buffer.toString());
                while (matcher.find()) {
                    String line = matcher.group();
                    processLine(line);
                    end = matcher.end();
                }
                buffer.delete(0, end);
                progressNotifier.accept(byteReads);
            }
            if(buffer.length() > 0) {
                processLine(buffer.toString());
            }
            buffer.delete(0, buffer.length());
            tryCloseWriter();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                raf.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void tryCloseWriter() {
        Optional.ofNullable(writer).ifPresent(pw -> pw.close());
    }

    private void processLine(final String line) throws IOException {
        Pattern fromPattern = Pattern.compile("^From\\s\\d");
        Pattern datePattern = Pattern.compile("(\\w{3} \\w{3} \\d{2} \\d{2}:\\d{2}:\\d{2} .0000 \\d{4})$");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);
        Matcher matcher;
        long serialize;
        if(fromPattern.matcher(line).find()) {
            tryCloseWriter();
            matcher = datePattern.matcher(line);
            if(matcher.find()) {
                LocalDateTime dateTime = LocalDateTime.parse(matcher.group(1), formatter);
                serialize = dateTime.toEpochSecond(ZoneOffset.UTC);
                File file = File.createTempFile(String.valueOf(serialize), ".tmp");
                file.deleteOnExit();
                sortFileNotifier.accept(serialize, file);
                writer = new PrintWriter(new FileOutputStream(file, true), true);
            }
        }
        Optional.ofNullable(writer).ifPresent(pw -> pw.print(line));
    }
}
