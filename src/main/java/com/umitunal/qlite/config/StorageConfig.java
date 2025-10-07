package com.umitunal.qlite.config;

/**
 * Configuration for the underlying storage engine.
 */
public class StorageConfig {
    private final String dataDirectory;
    private final boolean durableWrites;
    private final int memoryBufferSizeMB;
    private final int maxMemoryBuffers;
    private final int backgroundThreads;

    private StorageConfig(Builder builder) {
        this.dataDirectory = builder.dataDirectory;
        this.durableWrites = builder.durableWrites;
        this.memoryBufferSizeMB = builder.memoryBufferSizeMB;
        this.maxMemoryBuffers = builder.maxMemoryBuffers;
        this.backgroundThreads = builder.backgroundThreads;
    }

    public String getDataDirectory() { return dataDirectory; }
    public boolean isDurableWrites() { return durableWrites; }
    public int getMemoryBufferSizeMB() { return memoryBufferSizeMB; }
    public int getMaxMemoryBuffers() { return maxMemoryBuffers; }
    public int getBackgroundThreads() { return backgroundThreads; }

    public static Builder newBuilder(String dataDirectory) {
        return new Builder(dataDirectory);
    }

    public static class Builder {
        private final String dataDirectory;
        private boolean durableWrites = false;
        private int memoryBufferSizeMB = 64;
        private int maxMemoryBuffers = 3;
        private int backgroundThreads = 4;

        private Builder(String dataDirectory) {
            this.dataDirectory = dataDirectory;
        }

        /**
         * Enable durable writes (fsync on every write).
         * Slower but guarantees durability.
         * Default: false
         */
        public Builder withDurableWrites(boolean enable) {
            this.durableWrites = enable;
            return this;
        }

        /**
         * Set memory buffer size in MB.
         * Default: 64 MB
         */
        public Builder withMemoryBufferSize(int sizeMB) {
            this.memoryBufferSizeMB = sizeMB;
            return this;
        }

        /**
         * Set maximum number of memory buffers.
         * Default: 3
         */
        public Builder withMaxMemoryBuffers(int count) {
            this.maxMemoryBuffers = count;
            return this;
        }

        /**
         * Set number of background compaction threads.
         * Default: 4
         */
        public Builder withBackgroundThreads(int count) {
            this.backgroundThreads = count;
            return this;
        }

        public StorageConfig build() {
            return new StorageConfig(this);
        }
    }
}
