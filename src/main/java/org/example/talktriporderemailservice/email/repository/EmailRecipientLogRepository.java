package org.example.talktriporderemailservice.email.repository;

import org.example.talktriporderemailservice.email.domain.EmailRecipientLog;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EmailRecipientLogRepository extends JpaRepository<EmailRecipientLog, Long> {
}

