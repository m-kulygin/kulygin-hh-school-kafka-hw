package ru.hh.kafkahw;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.KafkaProducer;

@Component
public class Sender {
  private final KafkaProducer producer;
  private final static int MAX_ATTEMPTS_TO_SEND = 10;

  @Autowired
  public Sender(KafkaProducer producer) {
    this.producer = producer;
  }

  // Можно сказать, что здесь я попытался сделать exactlyOnce именно для отправки сообщений.
  // Механизм аналогичен exactlyOnce в TopicListener.
  // "Долбим" каждое сообщение до победного (ограничиваясь MAX_ATTEMPTS_TO_SEND, чтобы не улететь в цикл).
  // Как только удалось отправить (ориентируемся по message исключения) - останавливаемся.
  public void doSomething(String topic, String message) {
    int currentAttempts = 0;
    while (currentAttempts < MAX_ATTEMPTS_TO_SEND) {
      try {
        producer.send(topic, message);
        break;
      } catch (Exception exception) {
        if (exception.getMessage().equals("FAIL")) {
          currentAttempts++;
        }
        else {
          break;
        }
      }
    }
  }
}
