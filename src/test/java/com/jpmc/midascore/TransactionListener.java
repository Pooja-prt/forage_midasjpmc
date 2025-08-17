package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {

    @KafkaListener(topics = "midas-transactions", groupId = "midas-consumer-group")
    public void listen(Transaction transaction) {
        System.out.println("Transaction Amount: " + transaction.getAmount());
    }
}