package io.github.alexescalonafernandez.mbox.sort;

import io.github.alexescalonafernandez.mbox.sort.task.TaskExecutor;
import io.github.alexescalonafernandez.mbox.sort.task.TaskNotification;
import org.apache.commons.io.FilenameUtils;

import javax.swing.*;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.swing.filechooser.FileSystemView;
import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Created by alexander.escalona on 27/09/2018.
 */
public class Main {

    private static String getFilePath(String questionText, int selectionMode, Predicate<File> fileChecker) {
        System.out.print(questionText);
        JFileChooser fileChooser = new JFileChooser(FileSystemView.getFileSystemView().getHomeDirectory());
        fileChooser.setDialogTitle(questionText);
        fileChooser.setFileSelectionMode(selectionMode);
        FileNameExtensionFilter mboxFileFilter = new FileNameExtensionFilter("MBOX file", "mbox");
        fileChooser.setFileFilter(mboxFileFilter);
        boolean flag = false;
        int action;
        do {
            action = fileChooser.showOpenDialog(null);
            if(action == JFileChooser.APPROVE_OPTION) {
                if(flag = fileChecker.test(fileChooser.getSelectedFile())){
                    System.out.println(String.format(": %s", fileChooser.getSelectedFile().getAbsolutePath()));
                    return fileChooser.getSelectedFile().getAbsolutePath();
                }
            } else if(action == JFileChooser.CANCEL_OPTION) {
                flag = true;
            }
        } while (!flag);
        return null;
    }

    private static String getFilePath() {
        return getFilePath(
                "Choose the file to split",
                JFileChooser.FILES_ONLY,
                file -> file.exists() && file.isFile());
    }

    private static String getFilePath(String[] args) {
        return Optional.of(args)
                .filter(e -> e.length > 0)
                .map(e -> new File(e[0]))
                .filter(file -> file.exists() && file.isFile())
                .filter(file ->
                        Optional.ofNullable(FilenameUtils.getExtension(file.getAbsolutePath()))
                                .map(ext -> "mbox".equals(ext.toLowerCase()))
                                .orElse(false)
                )
                .map(file -> (Supplier<String>) () -> file.getAbsolutePath())
                .orElse(() -> getFilePath())
                .get();
    }

    public static void main(String[] args) throws IOException {
        String filePath = getFilePath(args);
        if(filePath != null) {
            File mboxFile = new File(filePath);
            TaskExecutor taskExecutor = new TaskExecutor(mboxFile, new TaskNotification() {
                private AtomicInteger store = new AtomicInteger(-1);
                @Override
                public void initProgressViewer() {
                    store.set(-1);
                    System.out.println();
                }

                @Override
                public Consumer<Double> getProgressViewerNotifier() {
                    return percent -> {
                        int value = (int)Math.floor(percent);
                        if(value > store.getAndSet(value)) {
                            printProgressBar(generateProgressBar(value));
                        }
                    };
                }

                @Override
                public Consumer<String> getMessageNotifier() {
                    return message -> System.out.println(message);
                }

                private void printProgressBar(String progressBar) {
                    System.out.printf("\r");
                    System.out.print(progressBar);
                }

                private String generateProgressBar(int percent) {
                    int progressCharsToShow = percent / 2;
                    return String.format("Progress: |%s%s| %d%s",
                            repeat('\u2588', progressCharsToShow),
                            repeat('-', 50 - progressCharsToShow),
                            percent, "%"
                    );
                }

                private String repeat(char c, int times)  {
                    StringBuilder sb = new StringBuilder();
                    while (times-- > 0) {
                        sb.append(c);
                    }
                    return sb.toString();
                }
            });
            taskExecutor.run();
        }
    }
}
