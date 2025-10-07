package com.umitunal.qlite.storage;

import com.umitunal.qlite.config.StorageConfig;
import com.umitunal.qlite.core.PersistentQueue;
import com.umitunal.qlite.core.QueueMetrics;
import com.umitunal.qlite.model.WorkUnit;
import com.umitunal.qlite.serialization.PayloadCodec;
import org.rocksdb.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * RocksDB-backed implementation of PersistentQueue.
 * Provides a standard Java Queue interface with persistence and scheduling.
 *
 * @param <E> the type of elements held in this queue
 */
public class RocksPersistentQueue<E> implements PersistentQueue<E> {
    private final RocksDB database;
    private final PayloadCodec<E> codec;
    private final WriteOptions writeOpts;
    private final ReadOptions scanReadOpts;
    private final Options dbOptions;
    private final BlockBasedTableConfig tableConfig;
    private final Cache blockCache;
    private final Filter bloomFilter;
    private final String workerId;

    public RocksPersistentQueue(StorageConfig config, PayloadCodec<E> codec, String workerId)
            throws RocksDBException {
        this.codec = codec;
        this.workerId = workerId;

        RocksDB.loadLibrary();

        // Performance optimizations
        this.blockCache = new LRUCache(128 * 1024 * 1024);
        this.bloomFilter = new BloomFilter(10, false);

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
                .setMaxOpenFiles(5000);

        this.database = RocksDB.open(dbOptions, config.getDataDirectory());

        this.writeOpts = new WriteOptions()
                .setSync(config.isDurableWrites())
                .setDisableWAL(!config.isDurableWrites());

        this.scanReadOpts = new ReadOptions()
                .setFillCache(false);
    }

    // ========== Standard Queue Interface ==========

    @Override
    public boolean add(E element) {
        return offer(element);
    }

    @Override
    public boolean offer(E element) {
        try {
            return schedule(element, System.currentTimeMillis());
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public E remove() {
        E element = poll();
        if (element == null) {
            throw new NoSuchElementException("Queue is empty");
        }
        return element;
    }

    @Override
    public E poll() {
        try {
            return pollReady();
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public E element() {
        E element = peek();
        if (element == null) {
            throw new NoSuchElementException("Queue is empty");
        }
        return element;
    }

    @Override
    public E peek() {
        return peekReady();
    }

    @Override
    public int size() {
        try {
            long size = approximateSize();
            return (int) Math.min(size, Integer.MAX_VALUE);
        } catch (Exception e) {
            return 0;
        }
    }

    @Override
    public boolean isEmpty() {
        try (final RocksIterator iter = database.newIterator(scanReadOpts)) {
            iter.seekToFirst();
            return !iter.isValid();
        }
    }

    @Override
    public boolean contains(Object o) {
        // Linear scan - expensive operation
        try (final RocksIterator iter = database.newIterator(scanReadOpts)) {
            iter.seekToFirst();

            while (iter.isValid()) {
                byte[] value = iter.value();
                WorkUnit<E> unit = WorkUnit.deserialize(value, codec);
                if (Objects.equals(unit.getPayload(), o)) {
                    return true;
                }
                iter.next();
            }
        }
        return false;
    }

    @Override
    public Iterator<E> iterator() {
        return new QueueIterator();
    }

    @Override
    public Object[] toArray() {
        List<E> list = new ArrayList<>();
        try (final RocksIterator iter = database.newIterator(scanReadOpts)) {
            iter.seekToFirst();
            while (iter.isValid()) {
                byte[] value = iter.value();
                WorkUnit<E> unit = WorkUnit.deserialize(value, codec);
                list.add(unit.getPayload());
                iter.next();
            }
        }
        return list.toArray();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a) {
        List<E> list = new ArrayList<>();
        try (final RocksIterator iter = database.newIterator(scanReadOpts)) {
            iter.seekToFirst();
            while (iter.isValid()) {
                byte[] value = iter.value();
                WorkUnit<E> unit = WorkUnit.deserialize(value, codec);
                list.add(unit.getPayload());
                iter.next();
            }
        }
        return list.toArray(a);
    }

    @Override
    public boolean remove(Object o) {
        // Find and remove first occurrence
        try (final RocksIterator iter = database.newIterator(scanReadOpts)) {
            iter.seekToFirst();

            while (iter.isValid()) {
                byte[] key = iter.key();
                byte[] value = iter.value();
                WorkUnit<E> unit = WorkUnit.deserialize(value, codec);

                if (Objects.equals(unit.getPayload(), o)) {
                    database.delete(writeOpts, key);
                    return true;
                }
                iter.next();
            }
        } catch (RocksDBException e) {
            return false;
        }
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object element : c) {
            if (!contains(element)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        boolean modified = false;
        for (E element : c) {
            if (add(element)) {
                modified = true;
            }
        }
        return modified;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean modified = false;
        for (Object element : c) {
            if (remove(element)) {
                modified = true;
            }
        }
        return modified;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException("retainAll not supported for persistent queue");
    }

    @Override
    public void clear() {
        try (final RocksIterator iter = database.newIterator(scanReadOpts);
             final WriteBatch batch = new WriteBatch()) {

            iter.seekToFirst();
            while (iter.isValid()) {
                batch.delete(iter.key());
                iter.next();
            }

            if (batch.count() > 0) {
                database.write(writeOpts, batch);
            }
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to clear queue", e);
        }
    }

    // ========== Extended PersistentQueue Interface ==========

    @Override
    public boolean schedule(E element, long scheduledTime) {
        try {
            String jobId = UUID.randomUUID().toString();
            WorkUnit<E> unit = new WorkUnit<>(jobId, element, scheduledTime, 1);
            byte[] key = WorkUnit.createStorageKey(scheduledTime, jobId);
            byte[] value = unit.serialize(codec);
            database.put(writeOpts, key, value);
            return true;
        } catch (RocksDBException e) {
            return false;
        }
    }

    @Override
    public E take() throws InterruptedException {
        while (true) {
            E element = poll();
            if (element != null) {
                return element;
            }
            Thread.sleep(100); // Poll every 100ms
        }
    }

    @Override
    public E peekReady() {
        long now = System.currentTimeMillis();

        try (final RocksIterator iter = database.newIterator(scanReadOpts)) {
            iter.seekToFirst();

            while (iter.isValid()) {
                byte[] value = iter.value();
                WorkUnit<E> unit = WorkUnit.deserialize(value, codec);

                if (unit.getScheduledTime() > now) {
                    break; // Not ready yet
                }

                return unit.getPayload();
            }
        }

        return null;
    }

    @Override
    public long approximateSize() {
        try {
            String numKeysEstimate = database.getProperty("rocksdb.estimate-num-keys");
            return Long.parseLong(numKeysEstimate);
        } catch (RocksDBException e) {
            return 0;
        }
    }

    @Override
    public QueueMetrics getMetrics() throws RocksDBException {
        long total = 0;
        long queued = 0;

        try (final RocksIterator iter = database.newIterator(scanReadOpts)) {
            iter.seekToFirst();

            while (iter.isValid()) {
                total++;
                queued++;
                iter.next();
            }
        }

        return new QueueMetrics(total, queued, 0, 0, 0, 0);
    }

    // ========== Helper Methods ==========

    private E pollReady() throws RocksDBException {
        long now = System.currentTimeMillis();

        try (final RocksIterator iter = database.newIterator(scanReadOpts)) {
            iter.seekToFirst();

            while (iter.isValid()) {
                byte[] key = iter.key();
                byte[] value = iter.value();

                WorkUnit<E> unit = WorkUnit.deserialize(value, codec);

                if (unit.getScheduledTime() > now) {
                    break; // Not ready yet
                }

                // Remove and return
                database.delete(writeOpts, key);
                return unit.getPayload();
            }
        }

        return null;
    }

    @Override
    public void close() {
        if (scanReadOpts != null) scanReadOpts.close();
        if (writeOpts != null) writeOpts.close();
        if (database != null) database.close();
        if (dbOptions != null) dbOptions.close();
        // Note: BlockBasedTableConfig doesn't have close() method
        // It's closed automatically when Options is closed
        if (blockCache != null) blockCache.close();
        if (bloomFilter != null) bloomFilter.close();
    }

    // ========== Inner Classes ==========

    private class QueueIterator implements Iterator<E> {
        private final RocksIterator iter;
        private E nextElement;

        QueueIterator() {
            this.iter = database.newIterator(scanReadOpts);
            iter.seekToFirst();
            advance();
        }

        private void advance() {
            if (iter.isValid()) {
                byte[] value = iter.value();
                WorkUnit<E> unit = WorkUnit.deserialize(value, codec);
                nextElement = unit.getPayload();
                iter.next();
            } else {
                nextElement = null;
                iter.close();
            }
        }

        @Override
        public boolean hasNext() {
            return nextElement != null;
        }

        @Override
        public E next() {
            if (nextElement == null) {
                throw new NoSuchElementException();
            }
            E current = nextElement;
            advance();
            return current;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove not supported");
        }
    }
}
