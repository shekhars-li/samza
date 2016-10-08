package org.apache.samza.executors;

import junit.framework.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestKeyBasedExecutorService {

  @Test
  public void testSubmitOrdered() {
    KeyBasedExecutorService executorService = new KeyBasedExecutorService("test", 2);
    ConcurrentLinkedQueue<Integer> resultFromThread0 = new ConcurrentLinkedQueue<>();
    ConcurrentLinkedQueue<Integer> resultFromThread1 = new ConcurrentLinkedQueue<>();

    final CountDownLatch shutdownLatch = new CountDownLatch(10);

    for (int i = 0; i < 10; i++) {
      final int currentStep = i;
      executorService.submitOrdered(currentStep, new Runnable() {
        @Override
        public void run() {
            String threadName = Thread.currentThread().getName();
            Pattern compiledPattern = Pattern.compile("test-(.+)-0");
            Matcher matcher = compiledPattern.matcher(threadName);
            if (matcher.find()) {
              String threadPoolNumber = matcher.group(1);
              if ("0".equals(threadPoolNumber)) {
                resultFromThread0.add(currentStep);
              } else if ("1".equals(threadPoolNumber)) {
                resultFromThread1.add(currentStep);
              }
              shutdownLatch.countDown();
            }
        }
      });
    }
    try {
      shutdownLatch.await(2, TimeUnit.SECONDS);
      executorService.shutdown();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Assert.assertEquals(5, resultFromThread0.size());
    Assert.assertEquals(5, resultFromThread1.size());

    Iterator<Integer> iterator = resultFromThread0.iterator();
    int i = 0;
    while (iterator.hasNext()) {
      Assert.assertEquals(i, iterator.next().intValue());
      i += 2;
    }
    iterator = resultFromThread1.iterator();
    i = 1;
    while (iterator.hasNext()) {
      Assert.assertEquals(i, iterator.next().intValue());
      i += 2;
    }

  }
}
