package com.umitunal.examples;

import com.umitunal.qlite.config.StorageConfig;
import com.umitunal.qlite.core.Job;
import com.umitunal.qlite.core.JobQueue;
import com.umitunal.qlite.serialization.JsonCodec;
import com.umitunal.qlite.serialization.KryoCodec;
import com.umitunal.qlite.storage.RocksJobQueue;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Demonstrates high-performance object serialization using Kryo.
 * Kryo is 5-10x faster than JSON and produces 2-3x smaller payloads.
 */
public class KryoExample {

    /**
     * Example domain object with nested structures.
     */
    public static class OrderEvent implements Serializable {
        private String orderId;
        private String customerId;
        private List<OrderItem> items;
        private Map<String, String> metadata;
        private long timestamp;

        public OrderEvent() {}

        public OrderEvent(String orderId, String customerId, List<OrderItem> items,
                         Map<String, String> metadata, long timestamp) {
            this.orderId = orderId;
            this.customerId = customerId;
            this.items = items;
            this.metadata = metadata;
            this.timestamp = timestamp;
        }

        // Getters and setters
        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }

        public String getCustomerId() { return customerId; }
        public void setCustomerId(String customerId) { this.customerId = customerId; }

        public List<OrderItem> getItems() { return items; }
        public void setItems(List<OrderItem> items) { this.items = items; }

        public Map<String, String> getMetadata() { return metadata; }
        public void setMetadata(Map<String, String> metadata) { this.metadata = metadata; }

        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

        @Override
        public String toString() {
            return "OrderEvent{" +
                    "orderId='" + orderId + '\'' +
                    ", customerId='" + customerId + '\'' +
                    ", items=" + items +
                    ", metadata=" + metadata +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }

    public static class OrderItem implements Serializable {
        private String productId;
        private int quantity;
        private double price;

        public OrderItem() {}

        public OrderItem(String productId, int quantity, double price) {
            this.productId = productId;
            this.quantity = quantity;
            this.price = price;
        }

        public String getProductId() { return productId; }
        public void setProductId(String productId) { this.productId = productId; }

        public int getQuantity() { return quantity; }
        public void setQuantity(int quantity) { this.quantity = quantity; }

        public double getPrice() { return price; }
        public void setPrice(double price) { this.price = price; }

        @Override
        public String toString() {
            return "OrderItem{" +
                    "productId='" + productId + '\'' +
                    ", quantity=" + quantity +
                    ", price=" + price +
                    '}';
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== Kryo Serialization Example ===\n");

        // Create a complex order object
        OrderEvent order = new OrderEvent(
                "ORD-12345",
                "CUST-67890",
                List.of(
                        new OrderItem("PROD-001", 2, 29.99),
                        new OrderItem("PROD-002", 1, 149.99),
                        new OrderItem("PROD-003", 5, 9.99)
                ),
                Map.of(
                        "source", "mobile-app",
                        "campaign", "summer-sale",
                        "shipping", "express"
                ),
                System.currentTimeMillis()
        );

        System.out.println("Sample Order: " + order + "\n");

        // Example 1: Basic Kryo usage
        basicKryoExample(order);

        // Example 2: Performance comparison with JSON
        performanceComparison(order);

        // Example 3: Custom Kryo configuration
        customKryoConfiguration(order);
    }

    private static void basicKryoExample(OrderEvent order) throws Exception {
        System.out.println("--- Example 1: Basic Kryo Serialization ---");

        StorageConfig config = StorageConfig.newBuilder("/tmp/kryo-queue-basic")
                .withDurableWrites(false)
                .build();

        try (JobQueue<OrderEvent> queue = new RocksJobQueue<>(config, new KryoCodec<>(OrderEvent.class))) {
            long now = System.currentTimeMillis();

            // Enqueue the order
            queue.enqueue("order-1", order, now, 3);
            System.out.println("✓ Enqueued order with Kryo serialization");

            // Process the order
            Job<OrderEvent> job = queue.acquire("worker-1", 30000);
            if (job != null) {
                OrderEvent payload = job.getPayload();
                System.out.println("✓ Acquired order: " + payload.getOrderId());
                System.out.println("  Items: " + payload.getItems().size());
                System.out.println("  Metadata: " + payload.getMetadata());
                queue.acknowledge(job);
            }
        }

        System.out.println();
    }

    private static void performanceComparison(OrderEvent order) throws Exception {
        System.out.println("--- Example 2: Performance Comparison ---");

        // Kryo codec
        KryoCodec<OrderEvent> kryoCodec = new KryoCodec<>(OrderEvent.class);

        // JSON codec
        JsonCodec<OrderEvent> jsonCodec = new JsonCodec<>(OrderEvent.class);

        // Measure Kryo
        long kryoStart = System.nanoTime();
        byte[] kryoBytes = kryoCodec.encode(order);
        OrderEvent kryoDecoded = kryoCodec.decode(kryoBytes);
        long kryoTime = System.nanoTime() - kryoStart;

        // Measure JSON
        long jsonStart = System.nanoTime();
        byte[] jsonBytes = jsonCodec.encode(order);
        OrderEvent jsonDecoded = jsonCodec.decode(jsonBytes);
        long jsonTime = System.nanoTime() - jsonStart;

        System.out.println("Kryo:");
        System.out.println("  Size: " + kryoBytes.length + " bytes");
        System.out.println("  Time: " + (kryoTime / 1_000) + " μs");

        System.out.println("\nJSON:");
        System.out.println("  Size: " + jsonBytes.length + " bytes");
        System.out.println("  Time: " + (jsonTime / 1_000) + " μs");

        System.out.println("\nKryo is " + String.format("%.1fx", (double) jsonBytes.length / kryoBytes.length) +
                          " smaller and " + String.format("%.1fx", (double) jsonTime / kryoTime) + " faster");

        System.out.println();
    }

    private static void customKryoConfiguration(OrderEvent order) throws Exception {
        System.out.println("--- Example 3: Custom Kryo Configuration ---");

        StorageConfig config = StorageConfig.newBuilder("/tmp/kryo-queue-custom")
                .withDurableWrites(false)
                .build();

        // Use performance factory (requires class registration but faster)
        KryoCodec<OrderEvent> codec = new KryoCodec<>(OrderEvent.class, () -> {
            var kryo = new com.esotericsoftware.kryo.Kryo();

            // Register classes for better performance
            kryo.register(OrderEvent.class);
            kryo.register(OrderItem.class);
            kryo.register(java.util.ArrayList.class);
            kryo.register(java.util.HashMap.class);

            // Performance settings
            kryo.setRegistrationRequired(false); // Allow unregistered classes as fallback
            kryo.setReferences(false); // Disable references for simpler objects

            return kryo;
        });

        try (JobQueue<OrderEvent> queue = new RocksJobQueue<>(config, codec)) {
            long now = System.currentTimeMillis();

            // Enqueue with custom Kryo config
            queue.enqueue("order-custom", order, now, 3);
            System.out.println("✓ Enqueued with optimized Kryo configuration");

            Job<OrderEvent> job = queue.acquire("worker-1", 30000);
            if (job != null) {
                System.out.println("✓ Processed order: " + job.getPayload().getOrderId());
                queue.acknowledge(job);
            }
        }

        System.out.println();
    }
}
