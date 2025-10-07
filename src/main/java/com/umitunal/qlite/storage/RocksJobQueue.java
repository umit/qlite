package com.umitunal.qlite.storage;

import com.umitunal.qlite.config.StorageConfig;
import com.umitunal.qlite.core.Job;
import com.umitunal.qlite.core.JobQueue;
import com.umitunal.qlite.core.QueueMetrics;
import com.umitunal.qlite.model.WorkUnit;
import com.umitunal.qlite.serialization.PayloadCodec;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RocksDB-backed implementation of JobQueue.
 * Provides high-performance persistent storage with time-based ordering.
 *
 * @param <T> the type of job payload
 */
public class RocksJobQueue<T> implements JobQueue<T> {
    private final OptimisticTransactionDB transactionDB;
    private final PayloadCodec<T> codec;
    private final WriteOptions writeOpts;
    private final OptimisticTransactionOptions txnOpts;
    private final ReadOptions scanReadOpts;
    private final Options dbOptions;
    private final BlockBasedTableConfig tableConfig;
    private final Cache blockCache;
    private final Filter bloomFilter;
    private final long defaultLeaseDuration;
    private final AtomicLong txnRetryCount = new AtomicLong(0);

    public RocksJobQueue(StorageConfig config, PayloadCodec<T> codec) throws RocksDBException {
        this(config, codec, 30000); // 30 second default lease
    }

    public RocksJobQueue(StorageConfig config, PayloadCodec<T> codec, long defaultLeaseDuration)
            throws RocksDBException {
        this.codec = codec;
        this.defaultLeaseDuration = defaultLeaseDuration;

        RocksDB.loadLibrary();

        // Create block cache and bloom filter
        this.blockCache = new LRUCache(128 * 1024 * 1024); // 128MB cache
        this.bloomFilter = new BloomFilter(10, false); // 10 bits per key

        // Configure table format with performance optimizations
        this.tableConfig = new BlockBasedTableConfig()
                .setBlockCache(blockCache)
                .setFilterPolicy(bloomFilter)
                .setCacheIndexAndFilterBlocks(true)
                .setPinL0FilterAndIndexBlocksInCache(true);

        this.dbOptions = new Options()
                .setCreateIfMissing(true)
                .setCompressionType(CompressionType.LZ4_COMPRESSION)
                .setWriteBufferSize(config.getMemoryBufferSizeMB() * 1024 * 1024)
                .setMaxWriteBufferNumber(config.getMaxMemoryBuffers())
                .setTargetFileSizeBase(64 * 1024 * 1024)
                .setMaxBackgroundJobs(config.getBackgroundThreads())
                .setIncreaseParallelism(Runtime.getRuntime().availableProcessors())
                .setAllowConcurrentMemtableWrite(true)
                .setEnableWriteThreadAdaptiveYield(true)
                .setTableFormatConfig(tableConfig)
                .setMaxOpenFiles(-1);

        // Open as OptimisticTransactionDB for atomic operations
        this.transactionDB = OptimisticTransactionDB.open(dbOptions, config.getDataDirectory());

        this.writeOpts = new WriteOptions()
                .setSync(config.isDurableWrites())
                .setDisableWAL(!config.isDurableWrites());

        this.txnOpts = new OptimisticTransactionOptions()
                .setSetSnapshot(true);

        // ReadOptions for scans - don't pollute cache with full table scans
        this.scanReadOpts = new ReadOptions()
                .setFillCache(false);
    }

    @Override
    public void enqueue(String jobId, T payload, long scheduledTime, int maxAttempts) throws RocksDBException {
        WorkUnit<T> unit = new WorkUnit<>(jobId, payload, scheduledTime, maxAttempts);
        byte[] key = WorkUnit.createStorageKey(scheduledTime, jobId);
        byte[] value = unit.serialize(codec);
        transactionDB.put(writeOpts, key, value);
    }

    /**
     * Acquire a job using default lease duration.
     */
    public Job<T> acquire(String workerId) throws RocksDBException {
        return acquire(workerId, defaultLeaseDuration);
    }

    @Override
    public Job<T> acquire(String workerId, long leaseDuration) throws RocksDBException {
        long now = System.currentTimeMillis();

        try (final RocksIterator iter = transactionDB.newIterator(scanReadOpts)) {
            iter.seekToFirst();

            while (iter.isValid()) {
                byte[] key = iter.key();
                byte[] value = iter.value();

                WorkUnit<T> unit = WorkUnit.deserialize(value, codec);

                // Early termination: skip jobs scheduled too far in the future
                if (unit.getScheduledTime() > now + 60000) { // 1 minute buffer
                    break;
                }

                if (!unit.isReady()) {
                    iter.next();
                    continue;
                }

                if (isAcquirable(unit)) {
                    if (!unit.canRetry()) {
                        unit.markFailed("Maximum attempts exceeded");
                        transactionDB.put(writeOpts, key, unit.serialize(codec));
                        iter.next();
                        continue;
                    }

                    // Atomic acquisition using OptimisticTransaction
                    Job<T> acquired = tryAtomicAcquire(key, unit, workerId, leaseDuration);
                    if (acquired != null) {
                        return acquired;
                    }
                    // Transaction failed (conflict), continue to next job
                }

                iter.next();
            }
        }

        return null;
    }

    @Override
    public List<Job<T>> acquireBatch(String workerId, int limit, long leaseDuration) throws RocksDBException {
        List<Job<T>> acquired = new ArrayList<>();

        try (final RocksIterator iter = transactionDB.newIterator(scanReadOpts)) {
            iter.seekToFirst();

            while (iter.isValid() && acquired.size() < limit) {
                byte[] key = iter.key();
                byte[] value = iter.value();

                WorkUnit<T> unit = WorkUnit.deserialize(value, codec);

                if (!unit.isReady()) {
                    iter.next();
                    continue;
                }

                if (isAcquirable(unit)) {
                    if (!unit.canRetry()) {
                        unit.markFailed("Maximum attempts exceeded");
                        transactionDB.put(writeOpts, key, unit.serialize(codec));
                        iter.next();
                        continue;
                    }

                    // Use atomic acquire for each job in batch
                    Job<T> job = tryAtomicAcquire(key, unit, workerId, leaseDuration);
                    if (job != null) {
                        acquired.add(job);
                    }
                }

                iter.next();
            }
        }

        return acquired;
    }

    @Override
    public void acknowledge(Job<T> job) throws RocksDBException {
        byte[] key = WorkUnit.createStorageKey(job.getScheduledTime(), job.getId());
        // Remove completed jobs to keep storage lean
        transactionDB.delete(writeOpts, key);
    }

    @Override
    public void reject(Job<T> job, String reason) throws RocksDBException {
        byte[] key = WorkUnit.createStorageKey(job.getScheduledTime(), job.getId());

        try (Transaction txn = transactionDB.beginTransaction(writeOpts, txnOpts)) {
            byte[] value = txn.getForUpdate(new ReadOptions(), key, true);

            if (value == null) {
                throw new IllegalStateException("Job not found: " + job.getId());
            }

            WorkUnit<T> unit = WorkUnit.deserialize(value, codec);

            if (unit.canRetry()) {
                unit.resetForRetry();
            } else {
                unit.markFailed(reason);
            }

            txn.put(key, unit.serialize(codec));
            txn.commit();
        }
    }

    @Override
    public void renewLease(Job<T> job, long additionalTime) throws RocksDBException {
        byte[] key = WorkUnit.createStorageKey(job.getScheduledTime(), job.getId());

        try (Transaction txn = transactionDB.beginTransaction(writeOpts, txnOpts)) {
            byte[] value = txn.getForUpdate(new ReadOptions(), key, true);

            if (value == null) {
                throw new IllegalStateException("Job not found: " + job.getId());
            }

            WorkUnit<T> unit = WorkUnit.deserialize(value, codec);
            unit.extendLease(additionalTime);

            txn.put(key, unit.serialize(codec));
            txn.commit();
        }
    }

    @Override
    public long recoverAbandoned() throws RocksDBException {
        long recovered = 0;

        try (final RocksIterator iter = transactionDB.newIterator();
             final WriteBatch batch = new WriteBatch()) {

            iter.seekToFirst();

            while (iter.isValid()) {
                byte[] key = iter.key();
                byte[] value = iter.value();

                WorkUnit<T> unit = WorkUnit.deserialize(value, codec);

                if (unit.getState() == Job.ExecutionState.LEASED && unit.isLeaseExpired()) {
                    unit.markAbandoned();
                    batch.put(key, unit.serialize(codec));
                    recovered++;

                    if (recovered % 1000 == 0 && batch.count() > 0) {
                        transactionDB.write(writeOpts, batch);
                        batch.clear();
                    }
                }

                iter.next();
            }

            if (batch.count() > 0) {
                transactionDB.write(writeOpts, batch);
            }
        }

        return recovered;
    }

    @Override
    public QueueMetrics getMetrics() throws RocksDBException {
        long total = 0;
        long queued = 0;
        long leased = 0;
        long success = 0;
        long failed = 0;
        long abandoned = 0;

        try (final RocksIterator iter = transactionDB.newIterator()) {
            iter.seekToFirst();

            while (iter.isValid()) {
                total++;
                byte[] value = iter.value();
                WorkUnit<T> unit = WorkUnit.deserialize(value, codec);

                switch (unit.getState()) {
                    case QUEUED -> queued++;
                    case LEASED -> leased++;
                    case SUCCESS -> success++;
                    case FAILED -> failed++;
                    case ABANDONED -> abandoned++;
                }

                iter.next();
            }
        }

        return new QueueMetrics(total, queued, leased, success, failed, abandoned);
    }

    @Override
    public void close() {
        if (scanReadOpts != null) {
            scanReadOpts.close();
        }
        if (txnOpts != null) {
            txnOpts.close();
        }
        if (writeOpts != null) {
            writeOpts.close();
        }
        if (transactionDB != null) {
            transactionDB.close();
        }
        if (dbOptions != null) {
            dbOptions.close();
        }
        // Note: BlockBasedTableConfig doesn't have close() method
        // It's closed automatically when Options is closed
        if (blockCache != null) {
            blockCache.close();
        }
        if (bloomFilter != null) {
            bloomFilter.close();
        }
    }

    private boolean isAcquirable(WorkUnit<T> unit) {
        return unit.getState() == Job.ExecutionState.QUEUED ||
               (unit.getState() == Job.ExecutionState.LEASED && unit.isLeaseExpired()) ||
               unit.getState() == Job.ExecutionState.ABANDONED;
    }

    /**
     * Atomically acquire a job using OptimisticTransaction.
     */
    private Job<T> tryAtomicAcquire(byte[] key, WorkUnit<T> unit, String workerId, long leaseDuration) {
        try (Transaction txn = transactionDB.beginTransaction(writeOpts, txnOpts)) {
            // Read with exclusive lock
            byte[] currentValue = txn.getForUpdate(new ReadOptions(), key, true);

            if (currentValue == null) {
                return null; // Job was deleted
            }

            WorkUnit<T> currentUnit = WorkUnit.deserialize(currentValue, codec);

            // Verify job is still acquirable and unchanged
            if (!isAcquirable(currentUnit) || currentUnit.getVersion() != unit.getVersion()) {
                return null; // State changed, skip
            }

            // Apply lease and commit
            currentUnit.lease(workerId, leaseDuration);
            txn.put(key, currentUnit.serialize(codec));
            txn.commit();

            return currentUnit;

        } catch (RocksDBException e) {
            // Conflict - another worker got it first
            txnRetryCount.incrementAndGet();
            return null;
        }
    }

    /**
     * Get the number of transaction retries that occurred.
     * Useful for monitoring contention.
     */
    public long getTransactionRetryCount() {
        return txnRetryCount.get();
    }

    /**
     * Clean up failed jobs from storage.
     */
    public long purgeFailedJobs() throws RocksDBException {
        long purged = 0;

        try (final RocksIterator iter = transactionDB.newIterator();
             final WriteBatch batch = new WriteBatch()) {

            iter.seekToFirst();

            while (iter.isValid()) {
                byte[] key = iter.key();
                byte[] value = iter.value();

                WorkUnit<T> unit = WorkUnit.deserialize(value, codec);

                if (unit.getState() == Job.ExecutionState.FAILED) {
                    batch.delete(key);
                    purged++;

                    if (purged % 1000 == 0 && batch.count() > 0) {
                        transactionDB.write(writeOpts, batch);
                        batch.clear();
                    }
                }

                iter.next();
            }

            if (batch.count() > 0) {
                transactionDB.write(writeOpts, batch);
            }
        }

        return purged;
    }
}
