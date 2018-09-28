package io.github.alexescalonafernandez.mbox.sort.task;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Created by alexander.escalona on 27/09/2018.
 */
public class ProgressTask implements Runnable{
    private final long length;
    private final CountDownLatch countDownLatch;
    private final Supplier<BlockingQueue<Integer>> blockingQueueSupplier;
    private final Consumer<Double> progressViewerNotifier;
    private long progress;

    public ProgressTask(long length, CountDownLatch countDownLatch,
                        Supplier<BlockingQueue<Integer>> blockingQueueSupplier,
                        Consumer<Double> progressViewerNotifier) {
        this.length = length;
        this.countDownLatch = countDownLatch;
        this.blockingQueueSupplier = blockingQueueSupplier;
        this.progressViewerNotifier = progressViewerNotifier;
    }

    @Override
    public void run() {
        if(blockingQueueSupplier.get().size() > 0) {
            List<Integer> chunkRead = new ArrayList<>();
            blockingQueueSupplier.get().drainTo(chunkRead);
            chunkRead.stream().forEach(integer -> progress += integer);
            double percent = progress * 100.00 / length;
            progressViewerNotifier.accept(percent);
            if(progress == length) {
                this.countDownLatch.countDown();
            }
        }
    }
}
