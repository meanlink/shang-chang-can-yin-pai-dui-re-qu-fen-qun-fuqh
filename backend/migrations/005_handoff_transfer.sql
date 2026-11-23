CREATE TABLE IF NOT EXISTS handoff_reconciliations (
    id INT PRIMARY KEY AUTO_INCREMENT,
    handoff_id INT NOT NULL,
    open_digest CHAR(64) NOT NULL,
    open_cases JSON NOT NULL,
    checked_by INT NOT NULL,
    checked_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX (handoff_id,id),
    FOREIGN KEY (handoff_id) REFERENCES floor_handoffs(id),
    FOREIGN KEY (checked_by) REFERENCES users(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS recovery_handoffs (
    case_id INT PRIMARY KEY,
    handoff_id INT NOT NULL,
    owner_id INT NOT NULL,
    transferred_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (case_id) REFERENCES tenant_recoveries(id),
    FOREIGN KEY (handoff_id) REFERENCES floor_handoffs(id),
    FOREIGN KEY (owner_id) REFERENCES users(id)
) ENGINE=InnoDB;
