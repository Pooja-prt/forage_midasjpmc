package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.repository.TransactionRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

@Component
public class TransactionListener {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private RestTemplate restTemplate;

    private static final String INCENTIVE_API_URL = "http://localhost:8080/incentive";

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

        // Transaction is valid - call incentives API
        System.out.println("Valid transaction: " + transaction.getAmount() +
                " from " + sender.getName() + " to " + recipient.getName());

        // Call incentives API to get incentive amount
        Incentive incentive;
        try {
            incentive = restTemplate.postForObject(INCENTIVE_API_URL, transaction, Incentive.class);
            System.out.println("Received incentive: " + incentive.getAmount());
        } catch (Exception e) {
            System.err.println("Failed to call incentives API: " + e.getMessage());
            incentive = new Incentive(0.0f); // Default to 0 incentive on API failure
        }

        // Update balances - deduct from sender, add transaction amount + incentive to recipient
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentive.getAmount());

        // Save updated user records
        userRepository.save(sender);
        userRepository.save(recipient);

        // Create and save transaction record with incentive
        TransactionRecord transactionRecord = new TransactionRecord(
                sender,
                recipient,
                transaction.getAmount(),
                incentive.getAmount()
        );

        transactionRepository.save(transactionRecord);

        System.out.println("Transaction processed successfully. Sender balance: " + sender.getBalance() +
                ", Recipient balance: " + recipient.getBalance() +
                ", Incentive: " + incentive.getAmount());
    }
}