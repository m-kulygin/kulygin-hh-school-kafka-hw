package ru.hh.kafkahw.internal;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;

@Component
public class Service {

  private final ConcurrentMap<String, ConcurrentMap<String, AtomicInteger>> counters = new ConcurrentHashMap<>();
  private final Random random = new Random();

  // К каждому исключению (сбою) добавляем информационное сообщение, чтобы можно было
  // распознать тип сбоя извне, и действовать исходя из этого.
  public void handle(String topic, String message) {
    if (random.nextInt(100) < 10) {
      throw new RuntimeException("FAIL"); // тотальный сбой - даже не успели ничего обработать
    }
    counters.computeIfAbsent(topic, key -> new ConcurrentHashMap<>())
        .computeIfAbsent(message, key -> new AtomicInteger(0)).incrementAndGet();
    if (random.nextInt(100) < 2) {
      throw new RuntimeException("SUCCESS"); // средний сбой - в принципе успели обработать сообщение
    }
  }

  public int count(String topic, String message) {
    return counters.getOrDefault(topic, new ConcurrentHashMap<>()).getOrDefault(message, new AtomicInteger(0)).get();
  }
}
