package io.github.alexescalonafernandez.mbox.sort.task;

import java.io.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
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
        Pattern fromPattern = Pattern.compile("^From\\s+[^\\s]+\\s+(.+)$");
        Matcher matcher = fromPattern.matcher(line);
        long serialize;
        if(matcher.find()) {
            try {
                LocalDateTime dateTime = tryGetDate(matcher.group(1));
                tryCloseWriter();
                serialize = dateTime.toEpochSecond(ZoneOffset.UTC);
                File file = File.createTempFile(String.valueOf(serialize), ".tmp");
                file.deleteOnExit();
                sortFileNotifier.accept(serialize, file);
                writer = new PrintWriter(new FileOutputStream(file, true), true);
            } catch (Exception ex) {
                System.out.println(line);
            }
        }
        Optional.ofNullable(writer).ifPresent(pw -> pw.print(line));
    }

    private LocalDateTime tryGetDate(String toMatch) {
        final StringBuilder builder = new StringBuilder(toMatch);
        final HashMap<String, String> fields = new HashMap<>();

        final Function<Matcher, String> mapper = m -> {
            String value = m.group();
            builder.replace(m.start(), m.end(), "");
            return value;
        };

        fields.put("zone", Optional.ofNullable(Pattern.compile("(?:\\B)[\\+-]\\d{4}"))
                .map(pattern -> pattern.matcher(builder.toString()))
                .filter(m -> m.find())
                .map(mapper)
                .orElse("+0000")
        );

        Optional.ofNullable(Pattern.compile("\\b(\\d{2}):(\\d{2}):?(\\d{2})?\\b"))
                .map(pattern -> pattern.matcher(builder.toString()))
                .filter(m -> m.find())
                .map(m -> {
                    String value = m.group();
                    fields.put("hour", m.group(1));
                    fields.put("minute", m.group(2));
                    fields.put("second", Optional.ofNullable(m.group(3)).orElse("00"));
                    builder.replace(m.start(), m.end(), "");

                    return value;
                })
                .orElse(null);

        fields.put("year", Optional.ofNullable(Pattern.compile("\\b\\d{4}\\b"))
                .map(pattern -> pattern.matcher(builder.toString()))
                .filter(m -> m.find())
                .map(mapper)
                .orElse(null)
        );

        fields.put("day", Optional.ofNullable(Pattern.compile("\\b\\d{2}\\b"))
                .map(pattern -> pattern.matcher(builder.toString()))
                .filter(m -> m.find())
                .map(mapper)
                .orElse(null)
        );

        fields.put("monthName", Optional.ofNullable(Pattern.compile("\\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\\b"))
                .map(pattern -> pattern.matcher(builder.toString()))
                .filter(m -> m.find())
                .map(mapper)
                .orElse(null)
        );

        fields.put("monthNumber", Optional.ofNullable(Pattern.compile("\\b\\d{2}\\b"))
                .map(pattern -> pattern.matcher(builder.toString()))
                .filter(m -> m.find())
                .map(mapper)
                .orElse(null)
        );

        Optional.ofNullable(fields.get("monthName"))
                .ifPresent(monthName -> {
                    int value = Arrays.asList("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
                            .indexOf(monthName) + 1;
                    String str = String.valueOf(value);
                    if(value < 10)
                        str = "0" + value;
                    fields.put("monthNumber", str);
                });
        builder.delete(0, builder.length());

        builder.append(fields.get("day")).append(" ")
                .append(fields.get("monthNumber")).append(" ")
                .append(fields.get("year")).append(" ")
                .append(fields.get("hour")).append(":")
                .append(fields.get("minute")).append(":")
                .append(fields.get("second")).append(" ")
                .append(fields.get("zone"));

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd MM yyyy HH:mm:ss Z", Locale.ENGLISH);
        return LocalDateTime.parse(builder.toString(), formatter);
    }
}
