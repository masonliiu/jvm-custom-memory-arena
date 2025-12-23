public class HashTableStore {
    private final MemoryArena arena;
    private static final int BUCKET_COUNT_OFFSET = 0;
    private static final int BUCKET_ARRAY_OFFSET = 4;
    private static final int ENTRY_KEY_OFFSET = 0;
    private static final int ENTRY_VALUE_OFFSET = 4;
    private static final int ENTRY_NEXT_OFFSET = 8;
    private static final int ENTRY_SIZE = 12;
    private static final int DEFAULT_BUCKET_COUNT = 16;

    public HashTableStore(MemoryArena arena) {
        this.arena = arena;
    }

    public int createHashTable(int bucketCount) {
        if (bucketCount <= 0) {
            bucketCount = DEFAULT_BUCKET_COUNT;
        }
        
        int headerSize = BUCKET_ARRAY_OFFSET + (bucketCount * 4);
        int tableAddr = arena.alloc(headerSize);
        
        arena.putInt(tableAddr + BUCKET_COUNT_OFFSET, bucketCount);
        
        for (int i = 0; i < bucketCount; i++) {
            int bucketPtrAddr = tableAddr + BUCKET_ARRAY_OFFSET + (i * 4);
            arena.putInt(bucketPtrAddr, -1);
        }
        
        return tableAddr;
    }

    public int getBucketCount(int tableAddr) {
        checkTablePtr(tableAddr);
        return arena.getInt(tableAddr + BUCKET_COUNT_OFFSET);
    }

    public void put(int tableAddr, int key, int value) {
        checkTablePtr(tableAddr);
        int bucketCount = getBucketCount(tableAddr);
        int bucketIndex = hash(key, bucketCount);
        
        int bucketPtrAddr = tableAddr + BUCKET_ARRAY_OFFSET + (bucketIndex * 4);
        int headAddr = arena.getInt(bucketPtrAddr);
        
        int entryAddr = findEntry(headAddr, key);
        
        if (entryAddr != -1) {
            arena.putInt(entryAddr + ENTRY_VALUE_OFFSET, value);
        } else {
            int newEntryAddr = createEntry(key, value);
            arena.putInt(newEntryAddr + ENTRY_NEXT_OFFSET, headAddr);
            arena.putInt(bucketPtrAddr, newEntryAddr);
        }
    }

    public Integer get(int tableAddr, int key) {
        checkTablePtr(tableAddr);
        int bucketCount = getBucketCount(tableAddr);
        int bucketIndex = hash(key, bucketCount);
        
        int bucketPtrAddr = tableAddr + BUCKET_ARRAY_OFFSET + (bucketIndex * 4);
        int headAddr = arena.getInt(bucketPtrAddr);
        
        int entryAddr = findEntry(headAddr, key);
        if (entryAddr == -1) {
            return null;
        }
        
        return arena.getInt(entryAddr + ENTRY_VALUE_OFFSET);
    }

    public boolean contains(int tableAddr, int key) {
        return get(tableAddr, key) != null;
    }

    public void remove(int tableAddr, int key) {
        checkTablePtr(tableAddr);
        int bucketCount = getBucketCount(tableAddr);
        int bucketIndex = hash(key, bucketCount);
        
        int bucketPtrAddr = tableAddr + BUCKET_ARRAY_OFFSET + (bucketIndex * 4);
        int headAddr = arena.getInt(bucketPtrAddr);
        
        if (headAddr == -1) {
            return;
        }
        
        if (arena.getInt(headAddr + ENTRY_KEY_OFFSET) == key) {
            int nextAddr = arena.getInt(headAddr + ENTRY_NEXT_OFFSET);
            arena.putInt(bucketPtrAddr, nextAddr);
            return;
        }
        
        int current = headAddr;
        while (current != -1) {
            int nextAddr = arena.getInt(current + ENTRY_NEXT_OFFSET);
            if (nextAddr == -1) {
                break;
            }
            
            if (arena.getInt(nextAddr + ENTRY_KEY_OFFSET) == key) {
                int nextNextAddr = arena.getInt(nextAddr + ENTRY_NEXT_OFFSET);
                arena.putInt(current + ENTRY_NEXT_OFFSET, nextNextAddr);
                return;
            }
            
            current = nextAddr;
        }
    }

    public void printHashTable(int tableAddr) {
        checkTablePtr(tableAddr);
        int bucketCount = getBucketCount(tableAddr);
        
        System.out.println("HashTable (buckets: " + bucketCount + "):");
        for (int i = 0; i < bucketCount; i++) {
            int bucketPtrAddr = tableAddr + BUCKET_ARRAY_OFFSET + (i * 4);
            int headAddr = arena.getInt(bucketPtrAddr);
            
            if (headAddr != -1) {
                System.out.print("  Bucket " + i + ": ");
                printBucket(headAddr);
                System.out.println();
            }
        }
    }

    private int hash(int key, int bucketCount) {
        return Math.abs(key) % bucketCount;
    }

    private int createEntry(int key, int value) {
        int entryAddr = arena.alloc(ENTRY_SIZE);
        arena.putInt(entryAddr + ENTRY_KEY_OFFSET, key);
        arena.putInt(entryAddr + ENTRY_VALUE_OFFSET, value);
        arena.putInt(entryAddr + ENTRY_NEXT_OFFSET, -1);
        return entryAddr;
    }

    private int findEntry(int headAddr, int key) {
        int current = headAddr;
        while (current != -1) {
            int currentKey = arena.getInt(current + ENTRY_KEY_OFFSET);
            if (currentKey == key) {
                return current;
            }
            current = arena.getInt(current + ENTRY_NEXT_OFFSET);
        }
        return -1;
    }

    private void printBucket(int headAddr) {
        int current = headAddr;
        boolean first = true;
        while (current != -1) {
            if (!first) {
                System.out.print(" -> ");
            }
            int key = arena.getInt(current + ENTRY_KEY_OFFSET);
            int value = arena.getInt(current + ENTRY_VALUE_OFFSET);
            System.out.print("(" + key + ":" + value + ")");
            current = arena.getInt(current + ENTRY_NEXT_OFFSET);
            first = false;
        }
    }

    private void checkTablePtr(int ptr) {
        if (ptr < 0) {
            throw new InvalidPointerException(ptr, BUCKET_ARRAY_OFFSET, arena.used(), arena.capacity());
        }
        int bucketCount = ptr >= 0 && ptr + BUCKET_ARRAY_OFFSET <= arena.used() 
            ? arena.getInt(ptr + BUCKET_COUNT_OFFSET) 
            : 0;
        int headerSize = BUCKET_ARRAY_OFFSET + (bucketCount * 4);
        if (ptr + headerSize > arena.used()) {
            throw new InvalidPointerException(ptr, headerSize, arena.used(), arena.capacity());
        }
    }
}

