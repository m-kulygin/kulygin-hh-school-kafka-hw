package ru.hh.kafkahw;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.Service;

@Component
public class TopicListener {
  private final static Logger LOGGER = LoggerFactory.getLogger(TopicListener.class);
  private final Service service;
  private final static int MAX_ATTEMPTS_TO_HANDLE = 10;

  @Autowired
  public TopicListener(Service service) {
    this.service = service;
  }

  // Для удобства отслеживания у всех семантик при логировании выводим еще и счётчик у каждого сообщения.
  @KafkaListener(topics = "topic1", groupId = "group1")
  public void atMostOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    LOGGER.info("Try handle message, topic {}, payload {}, count {}",
            consumerRecord.topic(), consumerRecord.value(), service.count("topic1", consumerRecord.value()));
    // Здесь нам по сути не важно, было ли обработано сообщение или нет.
    // Поэтому возможные сбои при работе handle игнорируем.
    // При этом нам важно, чтобы сообщение не было обработано более 1 раза.
    // Поэтому обрабатываем его только в том случае, когда его count меньше 1.
    if (service.count("topic1", consumerRecord.value()) < 1) {
      service.handle("topic1", consumerRecord.value());
      ack.acknowledge();
    }
  }

  @KafkaListener(topics = "topic2", groupId = "group2")
  public void atLeastOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    LOGGER.info("Try handle message, topic {}, payload {}, count {}",
            consumerRecord.topic(), consumerRecord.value(), service.count("topic2", consumerRecord.value()));
    // Здесь нам важно, чтобы сообщение дошло хотя бы один раз, то есть, имело не нулевой count.
    // В остальном конкретное количество обработок не интересует.
    // Поэтому каждое сообщение долбим до победного (если возник сбой - пытаемся читать еще раз).
    // При этом надо учитывать, что если в работе handle у сообщения сбой возник слишком рано -
    // счетчик этого сообщения никогда не увеличится, и обработка будет бесконечной.
    // Правим эту ситуацию ограничением максимального количества попыток (MAX_ATTEMPTS_TO_HANDLE).
    // Если обработать за такое кол-во попыток не удалось, возможно, что-то упало глобально,
    // и обработать это сообщение мы не сможем в теории, пока всё не починится.
    // Для распознавания того, когда именно в handle произошел сбой (до обработки или после),
    // будем пользоваться message, зашитым в вылетающее из handle исключение.
    // Понятное дело, что все равно нет гарантии обработки, тк у handle могут быть стабильные сбои,
    // что даже в теории не позволит обработать все сообщения, пока сбой не починят.
    int currentAttempts = 0;
    while (currentAttempts < MAX_ATTEMPTS_TO_HANDLE) {
      try {
        service.handle("topic2", consumerRecord.value());
        ack.acknowledge();
        break;
      } catch (Exception exception) {
        if (exception.getMessage().equals("FAIL")) {
          currentAttempts++;
        }
        else {
          ack.acknowledge();
          break;
        }
      }
    }
  }

  @KafkaListener(topics = "topic3", groupId = "group3")
  public void exactlyOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    LOGGER.info("Try handle message, topic {}, payload {}, count {}",
            consumerRecord.topic(), consumerRecord.value(), service.count("topic3", consumerRecord.value()));
    // Здесь аналогично atLeastOnce, но дополнительное условие -
    // мы не обрабатываем сообщение, если оно уже было обработано (то есть, count больше нуля).
    // По сути, это (atMostOnce && atLeastOnce).
    int currentAttempts = 0;
    while ((currentAttempts < MAX_ATTEMPTS_TO_HANDLE) &&
            (service.count("topic3", consumerRecord.value()) < 1)) {
      try {
        service.handle("topic3", consumerRecord.value());
        ack.acknowledge();
        break;
      } catch (Exception exception) {
        if (exception.getMessage().equals("FAIL")) {
          currentAttempts++;
        }
        else {
          ack.acknowledge();
          break;
        }
      }
    }
  }
}
