package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.repository.TransactionRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class TransactionListener {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @KafkaListener(topics = "midas-transactions", groupId = "midas-consumer-group")
    @Transactional
    public void listen(Transaction transaction) {
        System.out.println("Processing transaction: " + transaction.getAmount());

        // Validate sender exists
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        if (sender == null) {
            System.out.println("Invalid sender ID: " + transaction.getSenderId());
            return;
        }

        // Validate recipient exists
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());
        if (recipient == null) {
            System.out.println("Invalid recipient ID: " + transaction.getRecipientId());
            return;
        }

        // Validate sender has sufficient balance
        if (sender.getBalance() < transaction.getAmount()) {
            System.out.println("Insufficient balance for sender: " + sender.getName() +
                    " (balance: " + sender.getBalance() + ", amount: " + transaction.getAmount() + ")");
            return;
        }

        // Transaction is valid - process it
        System.out.println("Valid transaction: " + transaction.getAmount() +
                " from " + sender.getName() + " to " + recipient.getName());

        // Update balances
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount());

        // Save updated user records
        userRepository.save(sender);
        userRepository.save(recipient);

        // Create and save transaction record
        UserRecord senderRecord = userRepository.findById(transaction.getSenderId());
        UserRecord recipientRecord = userRepository.findById(transaction.getRecipientId());

        TransactionRecord transactionRecord = new TransactionRecord(
                senderRecord,
                recipientRecord,
                transaction.getAmount()
        );

        transactionRepository.save(transactionRecord);

        System.out.println("Transaction processed successfully. Sender balance: " + sender.getBalance() +
                ", Recipient balance: " + recipient.getBalance());
    }
}