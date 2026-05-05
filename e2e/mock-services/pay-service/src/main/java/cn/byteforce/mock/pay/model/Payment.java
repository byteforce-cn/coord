package cn.byteforce.mock.pay.model;

import java.time.Instant;

public class Payment {
    public enum Status { PENDING, COMPLETED, FAILED, REFUNDED }

    private String paymentId;
    private String orderId;
    private double amount;
    private String encryptedCardToken;
    private Status status;
    private String failReason;
    private Instant createdAt;

    public Payment() { this.createdAt = Instant.now(); }

    public String getPaymentId() { return paymentId; }
    public void setPaymentId(String paymentId) { this.paymentId = paymentId; }
    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }
    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }
    public String getEncryptedCardToken() { return encryptedCardToken; }
    public void setEncryptedCardToken(String v) { this.encryptedCardToken = v; }
    public Status getStatus() { return status; }
    public void setStatus(Status status) { this.status = status; }
    public String getFailReason() { return failReason; }
    public void setFailReason(String v) { this.failReason = v; }
    public Instant getCreatedAt() { return createdAt; }
}
