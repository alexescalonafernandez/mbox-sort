package io.github.alexescalonafernandez.mbox.sort.task;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Comparator;
import java.util.TreeMap;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Created by alexander.escalona on 27/09/2018.
 */
public class TaskExecutor implements Runnable {
    private final File mboxFile;
    private final TaskNotification taskNotification;
    private long fileLength;
    public TaskExecutor(File mboxFile, TaskNotification taskNotification) throws IOException {
        this.mboxFile = mboxFile;
        this.taskNotification = taskNotification;
        RandomAccessFile raf = new RandomAccessFile(mboxFile, "r");
        fileLength = raf.length();
        raf.close();
    }

    @Override
    public void run() {
        final ExecutorService executorService = Executors.newFixedThreadPool(1);
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        final BlockingQueue<Integer> integerBlockingQueue = new LinkedBlockingDeque<>();
        final TreeMap<Long, File> sortedFiles = new TreeMap<>(Comparator.reverseOrder());
        CountDownLatch scheduleCountDownLatch = new CountDownLatch(1);
        taskNotification.initProgressViewer();
        try {
            scheduler.scheduleAtFixedRate(
                    new ProgressTask(
                            fileLength * 2,
                            scheduleCountDownLatch,
                            () -> integerBlockingQueue, taskNotification.getProgressViewerNotifier()
                    ), 100, 30, TimeUnit.MILLISECONDS
            );

            executorService.execute(
                    new SortTask(
                            mboxFile,
                            byteReads -> integerBlockingQueue.add(byteReads),
                            (seconds, file) -> {
                                sortedFiles.put(seconds, file);
                            }
                    )
            );

            executorService.execute(
                    new WriteTask(
                            mboxFile,
                            byteReads -> integerBlockingQueue.add(byteReads),
                            () -> sortedFiles.values().stream().collect(Collectors.toList())
                    )
            );
            scheduleCountDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            executorService.shutdown();
            scheduler.shutdownNow();
        }
    }
}
