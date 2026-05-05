package cn.byteforce.mock.order.model;

import java.time.Instant;

public class Order {
    public enum Status {
        CREATED, INVENTORY_DEDUCTED, PAID, CONFIRMED,
        PAY_FAILED, INVENTORY_INSUFFICIENT, CANCELLED, PROCESSING
    }

    private String orderId;
    private String userId;
    private String productId;
    private int quantity;
    private double unitPrice;
    private double totalAmount;
    private Status status;
    private String encryptedPhone;
    private String encryptedAddress;
    private String paymentId;
    private String workflowId;
    private Instant createdAt;
    private Instant updatedAt;

    public Order() {
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    // ── Getters & Setters ────────────────────────────────────
    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getProductId() { return productId; }
    public void setProductId(String productId) { this.productId = productId; }

    public int getQuantity() { return quantity; }
    public void setQuantity(int quantity) { this.quantity = quantity; }

    public double getUnitPrice() { return unitPrice; }
    public void setUnitPrice(double unitPrice) { this.unitPrice = unitPrice; }

    public double getTotalAmount() { return totalAmount; }
    public void setTotalAmount(double totalAmount) { this.totalAmount = totalAmount; }

    public Status getStatus() { return status; }
    public void setStatus(Status status) {
        this.status = status;
        this.updatedAt = Instant.now();
    }

    public String getEncryptedPhone() { return encryptedPhone; }
    public void setEncryptedPhone(String encryptedPhone) { this.encryptedPhone = encryptedPhone; }

    public String getEncryptedAddress() { return encryptedAddress; }
    public void setEncryptedAddress(String encryptedAddress) { this.encryptedAddress = encryptedAddress; }

    public String getPaymentId() { return paymentId; }
    public void setPaymentId(String paymentId) { this.paymentId = paymentId; }

    public String getWorkflowId() { return workflowId; }
    public void setWorkflowId(String workflowId) { this.workflowId = workflowId; }

    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
}
