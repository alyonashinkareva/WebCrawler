package info.kgeorgiy.ja.shinkareva.crawler;

import info.kgeorgiy.java.advanced.crawler.*;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;
import java.util.concurrent.*;

import static info.kgeorgiy.java.advanced.crawler.URLUtils.getHost;

public class WebCrawler implements NewCrawler {
    private final Downloader downloader;
    private final ExecutorService downloaders;
    private final ExecutorService extractors;
    private final int perHost;
    private final Map<String, HostSemaphore> hosts;
    private final Set<String> downloaded = new CopyOnWriteArraySet<>();
    private final Map<String, IOException> errors = new ConcurrentHashMap<>();
    private final Set<String> usedLinks = new CopyOnWriteArraySet<>();
    private final Phaser phaser = new Phaser(1);

    public WebCrawler(Downloader downloader, int downloaders, int extractors, int perHost) {
        this.downloader = downloader;
        this.downloaders = Executors.newFixedThreadPool(downloaders);
        this.extractors = Executors.newFixedThreadPool(extractors);
        this.perHost = perHost;
        this.hosts = new ConcurrentHashMap<>();
    }

    @Override
    public Result download(String url, int depth, Set<String> excludes) {
        usedLinks.add(url);
        final Set<String> currUrls = new CopyOnWriteArraySet<>();
        final Set<String> newUrls = new CopyOnWriteArraySet<>();
        currUrls.add(url);
        for (int i = 0; i < depth; i++) {
            for (var prevUrl : currUrls) {
                download(prevUrl, depth - i, newUrls, excludes);
            }
            phaser.arriveAndAwaitAdvance();
            currUrls.clear();
            currUrls.addAll(newUrls);
            newUrls.clear();
        }
        return new Result(new ArrayList<>(downloaded), errors);
    }

    private boolean checkUrl(String url, Set<String> excludes) {
        for (String ex : excludes) {
            if (url.contains(ex)) {
                return false;
            }
        }
        return true;
    }

    private void download(String url, int depth, Set<String> currenUrls, Set<String> excludes) {
        if (checkUrl(url, excludes)) {
            try {
                HostSemaphore hostSemaphore = hosts.computeIfAbsent(getHost(url), s -> new HostSemaphore());
                phaser.register();
                hostSemaphore.add(() -> {
                    try {
                        Document doc = downloader.download(url);
                        downloaded.add(url);
                        if (depth > 1) {
                            phaser.register();
                            extractors.submit(() -> {
                                try {
                                    List<String> extractLinks = doc.extractLinks();
                                    for (String currUrl : extractLinks) {
                                        if (!usedLinks.contains(currUrl)) {
                                            if (checkUrl(currUrl, excludes)) {
                                                usedLinks.add(currUrl);
                                                currenUrls.add(currUrl);
                                            }
                                        }
                                    }
                                } catch (IOException e) {
                                    errors.put(url, e);
                                } finally {
                                    phaser.arriveAndDeregister();
                                }
                            });
                        }
                    } catch (IOException e) {
                        errors.put(url, e);
                    } finally {
                        phaser.arriveAndDeregister();
                        hostSemaphore.runNewTask();
                    }
                });
            } catch (MalformedURLException e) {
                errors.put(url, e);
            }
        }
    }

    @Override
    public void close() {
        downloaders.shutdownNow();
        extractors.shutdownNow();
    }

    private class HostSemaphore {
        private final Queue<Runnable> notWorkingTasks;
        private int countOfLoading = 0;

        private HostSemaphore() {
            this.notWorkingTasks = new ConcurrentLinkedDeque<>();
        }

        private void add(Runnable task) {
            if (countOfLoading < perHost) {
                downloaders.submit(task);
                countOfLoading++;
            } else {
                notWorkingTasks.add(task);
            }
        }

        private void runNewTask() {
            var task = notWorkingTasks.poll();
            if (Objects.nonNull(task)) {
                downloaders.submit(task);
            } else {
                countOfLoading--;
            }
        }
    }

    public static void main(String[] args) {
        if (args == null || args.length != 5 || Arrays.stream(args).anyMatch(Objects::isNull)) {
            System.out.println("Invalid input format. Please follow this format:\nWebCrawler url [depth [downloads [extractors [perHost]]]]");
        } else {
            try {
                WebCrawler webCrawler = new WebCrawler(new CachingDownloader(1), Integer.parseInt(args[2]), Integer.parseInt(args[3]), Integer.parseInt(args[4]));
                webCrawler.download(args[0], Integer.parseInt(args[1]));
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }
}