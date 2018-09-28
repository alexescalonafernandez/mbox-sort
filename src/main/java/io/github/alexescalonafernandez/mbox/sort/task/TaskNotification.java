package io.github.alexescalonafernandez.mbox.sort.task;

import java.util.function.Consumer;

/**
 * Created by alexander.escalona on 27/09/2018.
 */
public interface TaskNotification {
    void initProgressViewer();
    Consumer<Double> getProgressViewerNotifier();
    Consumer<String> getMessageNotifier();
}
