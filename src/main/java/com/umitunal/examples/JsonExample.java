package com.umitunal.examples;

import com.umitunal.qlite.config.StorageConfig;
import com.umitunal.qlite.core.Job;
import com.umitunal.qlite.core.JobQueue;
import com.umitunal.qlite.serialization.JsonCodec;
import com.umitunal.qlite.storage.RocksJobQueue;

import java.util.UUID;

/**
 * Example demonstrating JSON serialization with complex objects.
 */
public class JsonExample {

    public static void main(String[] args) {
        System.out.println("=== JSON Codec Example ===\n");

        StorageConfig config = StorageConfig.newBuilder("/tmp/shardqueue-json")
                .withDurableWrites(false)
                .build();

        // Use JsonCodec for custom objects
        JsonCodec<EmailTask> codec = new JsonCodec<>(EmailTask.class);

        try (JobQueue<EmailTask> queue = new RocksJobQueue<>(config, codec)) {

            // Enqueue complex objects
            long now = System.currentTimeMillis();

            EmailTask welcomeEmail = new EmailTask(
                    "user@example.com",
                    "Welcome!",
                    "Welcome to our platform",
                    "welcome-template"
            );

            EmailTask resetEmail = new EmailTask(
                    "user@example.com",
                    "Password Reset",
                    "Click here to reset your password",
                    "reset-template"
            );

            EmailTask promotionEmail = new EmailTask(
                    "user@example.com",
                    "Special Offer",
                    "Get 50% off today!",
                    "promo-template"
            );

            queue.enqueue(UUID.randomUUID().toString(), welcomeEmail, now, 3);
            queue.enqueue(UUID.randomUUID().toString(), resetEmail, now, 3);
            queue.enqueue(UUID.randomUUID().toString(), promotionEmail, now + 5000, 3);

            System.out.println("Enqueued 3 email tasks");
            System.out.println(queue.getMetrics());

            // Process emails
            String workerId = "email-worker";
            System.out.println("\nProcessing emails:");

            Job<EmailTask> job;
            while ((job = queue.acquire(workerId, 5000)) != null) {
                EmailTask email = job.getPayload();

                System.out.println("\n  Sending email:");
                System.out.println("    To: " + email.getRecipient());
                System.out.println("    Subject: " + email.getSubject());
                System.out.println("    Body: " + email.getBody());
                System.out.println("    Template: " + email.getTemplate());

                // Simulate email sending
                Thread.sleep(200);

                queue.acknowledge(job);
                System.out.println("    ✓ Sent successfully");
            }

            System.out.println("\n" + queue.getMetrics());

        } catch (Exception e) {
            e.printStackTrace();
        }

        // Example with more complex nested objects
        nestedObjectExample();
    }

    private static void nestedObjectExample() {
        System.out.println("\n--- Nested Object Example ---\n");

        StorageConfig config = StorageConfig.newBuilder("/tmp/shardqueue-nested")
                .build();

        JsonCodec<OrderTask> codec = new JsonCodec<>(OrderTask.class);

        try (JobQueue<OrderTask> queue = new RocksJobQueue<>(config, codec)) {

            long now = System.currentTimeMillis();

            // Create complex nested object
            OrderTask order = new OrderTask();
            order.setOrderId("ORD-12345");
            order.setCustomerId("CUST-999");
            order.setTotalAmount(99.99);
            order.setStatus("PENDING");

            OrderTask.OrderItem item1 = new OrderTask.OrderItem(
                    "PROD-001", "Laptop", 2, 799.99
            );

            OrderTask.OrderItem item2 = new OrderTask.OrderItem(
                    "PROD-002", "Mouse", 1, 29.99
            );

            order.setItems(new OrderTask.OrderItem[]{item1, item2});

            queue.enqueue("order-process-1", order, now, 3);

            System.out.println("Enqueued order task");
            System.out.println(queue.getMetrics());

            // Process order
            String workerId = "order-processor";
            Job<OrderTask> job = queue.acquire(workerId, 5000);

            if (job != null) {
                OrderTask processedOrder = job.getPayload();
                System.out.println("\nProcessing order:");
                System.out.println("  Order ID: " + processedOrder.getOrderId());
                System.out.println("  Customer ID: " + processedOrder.getCustomerId());
                System.out.println("  Total: $" + processedOrder.getTotalAmount());
                System.out.println("  Items:");

                for (OrderTask.OrderItem item : processedOrder.getItems()) {
                    System.out.println("    - " + item.getName() +
                            " (x" + item.getQuantity() + ") $" + item.getPrice());
                }

                queue.acknowledge(job);
                System.out.println("\n  ✓ Order processed");
            }

            System.out.println("\n" + queue.getMetrics());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Example POJO for email tasks.
     */
    public static class EmailTask {
        private String recipient;
        private String subject;
        private String body;
        private String template;

        // Required for Jackson
        public EmailTask() {}

        public EmailTask(String recipient, String subject, String body, String template) {
            this.recipient = recipient;
            this.subject = subject;
            this.body = body;
            this.template = template;
        }

        public String getRecipient() { return recipient; }
        public void setRecipient(String recipient) { this.recipient = recipient; }

        public String getSubject() { return subject; }
        public void setSubject(String subject) { this.subject = subject; }

        public String getBody() { return body; }
        public void setBody(String body) { this.body = body; }

        public String getTemplate() { return template; }
        public void setTemplate(String template) { this.template = template; }
    }

    /**
     * Example POJO with nested objects.
     */
    public static class OrderTask {
        private String orderId;
        private String customerId;
        private double totalAmount;
        private String status;
        private OrderItem[] items;

        public OrderTask() {}

        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }

        public String getCustomerId() { return customerId; }
        public void setCustomerId(String customerId) { this.customerId = customerId; }

        public double getTotalAmount() { return totalAmount; }
        public void setTotalAmount(double totalAmount) { this.totalAmount = totalAmount; }

        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }

        public OrderItem[] getItems() { return items; }
        public void setItems(OrderItem[] items) { this.items = items; }

        public static class OrderItem {
            private String productId;
            private String name;
            private int quantity;
            private double price;

            public OrderItem() {}

            public OrderItem(String productId, String name, int quantity, double price) {
                this.productId = productId;
                this.name = name;
                this.quantity = quantity;
                this.price = price;
            }

            public String getProductId() { return productId; }
            public void setProductId(String productId) { this.productId = productId; }

            public String getName() { return name; }
            public void setName(String name) { this.name = name; }

            public int getQuantity() { return quantity; }
            public void setQuantity(int quantity) { this.quantity = quantity; }

            public double getPrice() { return price; }
            public void setPrice(double price) { this.price = price; }
        }
    }
}
