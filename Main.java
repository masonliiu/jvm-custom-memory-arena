public class Main {
    public static void main(String[] args) {
        
        testBasicAllocation();
        testAlignmentFeatures();
        testNodeStore();
        testErrorMessages();
        testBigEndianEncoding();
        testLongSupport();
        testShortSupport();
        testCharSupport();
        testBooleanSupport();
        testArrayStore();
        testVectorStore();
        testStringStore();
        testHashTableStore();
    }

    static void testBasicAllocation() {
        System.out.println("Test 1: Basic Allocation");
        MemoryArena arena = new MemoryArena(128);
        
        int a = arena.alloc(4);
        int b = arena.alloc(4);
        int c = arena.alloc(4);
        
        System.out.println("Allocated 3 blocks of 4 bytes each:");
        System.out.println("  Address of a: " + a);
        System.out.println("  Address of b: " + b);
        System.out.println("  Address of c: " + c);
        System.out.println("  Total capacity: " + arena.capacity());
        System.out.println("  Used: " + arena.used());
        System.out.println("  Remaining: " + arena.remaining());
        
        arena.reset();
        int d = arena.alloc(4);
        System.out.println("\nAfter reset, allocated d at address: " + d);
        System.out.println("  Used after reset: " + arena.used());
        System.out.println();
    }

    static void testAlignmentFeatures() {
        System.out.println("Test 2: Memory Alignment");
        MemoryArena arena = new MemoryArena(128);
        
        System.out.println("Allocating 3 bytes (unaligned):");
        int addr1 = arena.alloc(3);
        System.out.println("  Address: " + addr1 + " (offset: " + addr1 + ")");
        System.out.println("  Alignment waste so far: " + arena.getAlignmentWaste());
        
        System.out.println("\nAllocating 4 bytes with 4-byte alignment:");
        int addr2 = arena.allocAligned(4, 4);
        System.out.println("  Address: " + addr2 + " (aligned to 4-byte boundary)");
        System.out.println("  Alignment waste: " + arena.getAlignmentWaste() + " bytes");
        System.out.println("  Used: " + arena.used());
        
        System.out.println("\nAllocating another 4 bytes with 4-byte alignment:");
        int addr3 = arena.allocAligned(4, 4);
        System.out.println("  Address: " + addr3 + " (already aligned, no waste)");
        System.out.println("  Alignment waste: " + arena.getAlignmentWaste() + " bytes");
        System.out.println("  Used: " + arena.used());
        
        System.out.println("\nTesting align() helper:");
        System.out.println("  align(0, 4) = " + arena.align(0, 4));
        System.out.println("  align(1, 4) = " + arena.align(1, 4));
        System.out.println("  align(3, 4) = " + arena.align(3, 4));
        System.out.println("  align(4, 4) = " + arena.align(4, 4));
        System.out.println("  align(5, 4) = " + arena.align(5, 4));
        System.out.println("  align(7, 4) = " + arena.align(7, 4));
        System.out.println("  align(8, 4) = " + arena.align(8, 4));
        System.out.println();
    }

    static void testNodeStore() {
        System.out.println("Test 3: NodeStore (Separated Node Logic)");
        MemoryArena arena = new MemoryArena(128);
        NodeStore nodeStore = new NodeStore(arena);
        
        System.out.println("Creating 3 nodes:");
        int node1 = nodeStore.createNode(10);
        int node2 = nodeStore.createNode(20);
        int node3 = nodeStore.createNode(30);
        
        System.out.println("  Node 1 address: " + node1 + ", value: " + nodeStore.getValue(node1));
        System.out.println("  Node 2 address: " + node2 + ", value: " + nodeStore.getValue(node2));
        System.out.println("  Node 3 address: " + node3 + ", value: " + nodeStore.getValue(node3));
        System.out.println("  Node size: " + nodeStore.getNodeSize() + " bytes");
        
        System.out.println("\nForming linked list:");
        nodeStore.setNext(node1, node2);
        nodeStore.setNext(node2, node3);
        System.out.println("  Node 1 -> Node 2 -> Node 3");
        System.out.println("  Node 1's next pointer: " + nodeStore.getNext(node1));
        System.out.println("  Node 2's next pointer: " + nodeStore.getNext(node2));
        System.out.println("  Node 3's next pointer: " + nodeStore.getNext(node3));
        
        System.out.println("\nTraversing list:");
        System.out.print("  List contents: ");
        nodeStore.printList(node1);
        System.out.println();
        
        System.out.println("\nTesting aligned node creation:");
        arena.reset();
        NodeStore alignedNodeStore = new NodeStore(arena);
        arena.alloc(3);
        int alignedNode = alignedNodeStore.createNodeAligned(42, 4);
        System.out.println("  After allocating 3 bytes, created aligned node at: " + alignedNode);
        System.out.println("  Alignment waste: " + arena.getAlignmentWaste() + " bytes");
        System.out.println();
    }

    static void testErrorMessages() {
        System.out.println("Test 4: Enhanced Error Messages");
        
        System.out.println("\nTest 4.1: Out of Memory Exception");
        try {
            MemoryArena arena = new MemoryArena(10);
            arena.alloc(5);
            arena.alloc(6);
        } catch (OutOfMemoryException e) {
            System.out.println("  Caught: " + e.getMessage());
            System.out.println("  Requested: " + e.getRequestedSize() + " bytes");
            System.out.println("  Available: " + e.getAvailable() + " bytes");
            System.out.println("  Capacity: " + e.getCapacity() + " bytes");
            System.out.println("  Current offset: " + e.getCurrentOffset());
        }
        
        System.out.println("\nTest 4.2: Invalid Address Exception");
        try {
            MemoryArena arena = new MemoryArena(20);
            arena.alloc(10);
            arena.getInt(15);
        } catch (InvalidAddressException e) {
            System.out.println("  Caught: " + e.getMessage());
            System.out.println("  Address attempted: " + e.getAddress());
            System.out.println("  Bytes needed: " + e.getBytesNeeded());
            System.out.println("  Allocated boundary: " + e.getAllocatedBoundary());
        }
        
        System.out.println("\nTest 4.3: Invalid Pointer Exception");
        try {
            MemoryArena arena = new MemoryArena(20);
            NodeStore nodeStore = new NodeStore(arena);
            int node = nodeStore.createNode(10);
            nodeStore.setNext(node, 99999);
        } catch (InvalidPointerException e) {
            System.out.println("  Caught: " + e.getMessage());
            System.out.println("  Invalid pointer: " + e.getPointer());
            System.out.println("  Node size: " + e.getNodeSize());
        }
        
        System.out.println("\nTest 4.4: Null pointer handling (should work)");
        try {
            MemoryArena arena = new MemoryArena(20);
            NodeStore nodeStore = new NodeStore(arena);
            int node = nodeStore.createNode(10);
            nodeStore.setNext(node, -1);
            System.out.println("  Successfully set next pointer to -1 (null)");
            System.out.println("  Node's next: " + nodeStore.getNext(node));
        } catch (Exception e) {
            System.out.println("  Unexpected error: " + e.getMessage());
        }
        System.out.println();
    }

    static void testBigEndianEncoding() {
        System.out.println("Test 5: Big-Endian Integer Encoding");
        MemoryArena arena = new MemoryArena(128);
        
        int addr = arena.alloc(4);
        int testValue = 0x12345678;
        arena.putInt(addr, testValue);
        
        System.out.println("Storing value: 0x" + Integer.toHexString(testValue) + " at address " + addr);
        System.out.println("Byte representation (big-endian):");
        System.out.println("  memory[" + addr + "] = 0x" + Integer.toHexString(arena.memory[addr] & 0xFF));
        System.out.println("  memory[" + (addr + 1) + "] = 0x" + Integer.toHexString(arena.memory[addr + 1] & 0xFF));
        System.out.println("  memory[" + (addr + 2) + "] = 0x" + Integer.toHexString(arena.memory[addr + 2] & 0xFF));
        System.out.println("  memory[" + (addr + 3) + "] = 0x" + Integer.toHexString(arena.memory[addr + 3] & 0xFF));
        
        int reconstructed = arena.getInt(addr);
        System.out.println("Reconstructed value: 0x" + Integer.toHexString(reconstructed));
        System.out.println("Match: " + (testValue == reconstructed));
        System.out.println();
    
    }

    static void testLongSupport() {
        System.out.println("Test 6: Long Support (8 bytes)");
        MemoryArena arena = new MemoryArena(128);
        
        int addr = arena.alloc(8);
        long testValue = 0x0123456789ABCDEFL;
        arena.putLong(addr, testValue);
        
        System.out.println("Storing long value: 0x" + Long.toHexString(testValue) + " at address " + addr);
        System.out.println("Byte representation (big-endian, 8 bytes):");
        for (int i = 0; i < 8; i++) {
            System.out.println("  memory[" + (addr + i) + "] = 0x" + Integer.toHexString(arena.memory[addr + i] & 0xFF));
        }
        
        long reconstructed = arena.getLong(addr);
        System.out.println("Reconstructed value: 0x" + Long.toHexString(reconstructed));
        System.out.println("Match: " + (testValue == reconstructed));
        
        System.out.println("\nTesting edge cases:");
        long maxLong = Long.MAX_VALUE;
        int addr2 = arena.alloc(8);
        arena.putLong(addr2, maxLong);
        System.out.println("  Max long (" + maxLong + ") stored and retrieved: " + (maxLong == arena.getLong(addr2)));
        
        long minLong = Long.MIN_VALUE;
        int addr3 = arena.alloc(8);
        arena.putLong(addr3, minLong);
        System.out.println("  Min long (" + minLong + ") stored and retrieved: " + (minLong == arena.getLong(addr3)));
        
        long zero = 0L;
        int addr4 = arena.alloc(8);
        arena.putLong(addr4, zero);
        System.out.println("  Zero stored and retrieved: " + (zero == arena.getLong(addr4)));
        System.out.println();
    }

    static void testShortSupport() {
        System.out.println("Test 7: Short Support (2 bytes)");
        MemoryArena arena = new MemoryArena(128);
        
        int addr = arena.alloc(2);
        short testValue = (short)0xABCD;
        arena.putShort(addr, testValue);
        
        System.out.println("Storing short value: 0x" + Integer.toHexString(testValue & 0xFFFF) + " at address " + addr);
        System.out.println("Byte representation (big-endian, 2 bytes):");
        System.out.println("  memory[" + addr + "] = 0x" + Integer.toHexString(arena.memory[addr] & 0xFF));
        System.out.println("  memory[" + (addr + 1) + "] = 0x" + Integer.toHexString(arena.memory[addr + 1] & 0xFF));
        
        short reconstructed = arena.getShort(addr);
        System.out.println("Reconstructed value: 0x" + Integer.toHexString(reconstructed & 0xFFFF));
        System.out.println("Match: " + (testValue == reconstructed));
        
        System.out.println("\nTesting edge cases:");
        short maxShort = Short.MAX_VALUE;
        int addr2 = arena.alloc(2);
        arena.putShort(addr2, maxShort);
        System.out.println("  Max short (" + maxShort + ") stored and retrieved: " + (maxShort == arena.getShort(addr2)));
        
        short minShort = Short.MIN_VALUE;
        int addr3 = arena.alloc(2);
        arena.putShort(addr3, minShort);
        System.out.println("  Min short (" + minShort + ") stored and retrieved: " + (minShort == arena.getShort(addr3)));
        
        short zero = 0;
        int addr4 = arena.alloc(2);
        arena.putShort(addr4, zero);
        System.out.println("  Zero stored and retrieved: " + (zero == arena.getShort(addr4)));
        System.out.println();
    }

    static void testCharSupport() {
        System.out.println("Test 8: Char Support (2 bytes, UTF-16)");
        MemoryArena arena = new MemoryArena(128);
        
        int addr = arena.alloc(2);
        char testValue = 'A';
        arena.putChar(addr, testValue);
        
        System.out.println("Storing char value: '" + testValue + "' (Unicode: U+" + Integer.toHexString(testValue).toUpperCase() + ") at address " + addr);
        System.out.println("Byte representation (big-endian, 2 bytes):");
        System.out.println("  memory[" + addr + "] = 0x" + Integer.toHexString(arena.memory[addr] & 0xFF));
        System.out.println("  memory[" + (addr + 1) + "] = 0x" + Integer.toHexString(arena.memory[addr + 1] & 0xFF));
        
        char reconstructed = arena.getChar(addr);
        System.out.println("Reconstructed value: '" + reconstructed + "'");
        System.out.println("Match: " + (testValue == reconstructed));
        
        System.out.println("\nTesting various characters:");
        char[] testChars = {'A', 'Z', '0', '9', ' ', '!', '中', '\u03A9'};
        for (char c : testChars) {
            int charAddr = arena.alloc(2);
            arena.putChar(charAddr, c);
            char retrieved = arena.getChar(charAddr);
            boolean match = (c == retrieved);
            System.out.println("  '" + c + "' (U+" + Integer.toHexString(c).toUpperCase() + ") - " + (match ? "PASS" : "FAIL"));
        }
        System.out.println();
    }

    static void testBooleanSupport() {
        System.out.println("Test 9: Boolean Support (1 byte)");
        MemoryArena arena = new MemoryArena(128);
        
        int addr1 = arena.alloc(1);
        boolean testValue1 = true;
        arena.putBoolean(addr1, testValue1);
        
        System.out.println("Storing boolean value: " + testValue1 + " at address " + addr1);
        System.out.println("Byte representation (1 byte):");
        System.out.println("  memory[" + addr1 + "] = " + (arena.memory[addr1] & 0xFF));
        
        boolean reconstructed1 = arena.getBoolean(addr1);
        System.out.println("Reconstructed value: " + reconstructed1);
        System.out.println("Match: " + (testValue1 == reconstructed1));
        
        int addr2 = arena.alloc(1);
        boolean testValue2 = false;
        arena.putBoolean(addr2, testValue2);
        
        System.out.println("\nStoring boolean value: " + testValue2 + " at address " + addr2);
        System.out.println("Byte representation (1 byte):");
        System.out.println("  memory[" + addr2 + "] = " + (arena.memory[addr2] & 0xFF));
        
        boolean reconstructed2 = arena.getBoolean(addr2);
        System.out.println("Reconstructed value: " + reconstructed2);
        System.out.println("Match: " + (testValue2 == reconstructed2));
        
        System.out.println("\nTesting representation:");
        System.out.println("  true stored as: " + (arena.memory[addr1] & 0xFF));
        System.out.println("  false stored as: " + (arena.memory[addr2] & 0xFF));
        System.out.println();
    }

    static void testArrayStore() {
        System.out.println("Test 10: Fixed-Size Arrays");
        MemoryArena arena = new MemoryArena(256);
        ArrayStore arrayStore = new ArrayStore(arena);
        
        System.out.println("Creating int array of length 5:");
        int arrayAddr = arrayStore.createArray(5, 4);
        System.out.println("  Array address: " + arrayAddr);
        System.out.println("  Array length: " + arrayStore.getLength(arrayAddr));
        System.out.println("  Element size: " + arrayStore.getElementSize(arrayAddr) + " bytes");
        
        System.out.println("\nSetting array elements:");
        for (int i = 0; i < 5; i++) {
            arrayStore.setInt(arrayAddr, i, (i + 1) * 10);
            System.out.println("  array[" + i + "] = " + arrayStore.getInt(arrayAddr, i));
        }
        
        System.out.println("\nArray contents:");
        arrayStore.printArray(arrayAddr);
        
        System.out.println("\nTesting bounds checking:");
        try {
            arrayStore.getInt(arrayAddr, -1);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("  Caught: " + e.getMessage());
        }
        
        try {
            arrayStore.getInt(arrayAddr, 10);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("  Caught: " + e.getMessage());
        }
        
        System.out.println("\nMemory layout:");
        System.out.println("  Header (length): address " + arrayAddr + " (4 bytes)");
        System.out.println("  Data start: address " + (arrayAddr + 4) + " (20 bytes for 5 ints)");
        System.out.println("  Total size: " + (4 + 5 * 4) + " bytes");
        System.out.println();
    }

    static void testVectorStore() {
        System.out.println("Test 11: Dynamic Arrays (Vector)");
        MemoryArena arena = new MemoryArena(512);
        VectorStore vectorStore = new VectorStore(arena);
        
        System.out.println("Creating vector with initial capacity 2:");
        int vectorAddr = vectorStore.createVector(2);
        System.out.println("  Vector address: " + vectorAddr);
        System.out.println("  Initial length: " + vectorStore.getLength(vectorAddr));
        System.out.println("  Initial capacity: " + vectorStore.getCapacity(vectorAddr));
        System.out.println("  Data pointer: " + vectorStore.getDataPtr(vectorAddr));
        
        System.out.println("\nAppending elements:");
        for (int i = 1; i <= 5; i++) {
            vectorStore.append(vectorAddr, i * 10);
            System.out.println("  After appending " + (i * 10) + ":");
            System.out.println("    Length: " + vectorStore.getLength(vectorAddr));
            System.out.println("    Capacity: " + vectorStore.getCapacity(vectorAddr));
        }
        
        System.out.println("\nVector contents:");
        vectorStore.printVector(vectorAddr);
        
        System.out.println("\nAccessing elements by index:");
        for (int i = 0; i < vectorStore.getLength(vectorAddr); i++) {
            System.out.println("  vector[" + i + "] = " + vectorStore.get(vectorAddr, i));
        }
        
        System.out.println("\nModifying element at index 2:");
        vectorStore.set(vectorAddr, 2, 999);
        System.out.println("  vector[2] = " + vectorStore.get(vectorAddr, 2));
        vectorStore.printVector(vectorAddr);
        
        System.out.println("\nTesting growth behavior:");
        System.out.println("  Vector grows when length >= capacity");
        System.out.println("  Growth factor: 1.5x");
        System.out.println("  Old data is copied to new location");
        System.out.println("  Old space remains allocated (bump allocator limitation)");
        System.out.println();
    }

    static void testStringStore() {
        System.out.println("Test 12: String Storage (UTF-16)");
        MemoryArena arena = new MemoryArena(512);
        StringStore stringStore = new StringStore(arena);
        
        System.out.println("Creating strings:");
        String test1 = "Hello";
        int str1Addr = stringStore.createString(test1);
        System.out.println("  Created: \"" + test1 + "\" at address " + str1Addr);
        System.out.println("  Length: " + stringStore.getStringLength(str1Addr));
        
        String test2 = "World!";
        int str2Addr = stringStore.createString(test2);
        System.out.println("  Created: \"" + test2 + "\" at address " + str2Addr);
        System.out.println("  Length: " + stringStore.getStringLength(str2Addr));
        
        String test3 = "你好";
        int str3Addr = stringStore.createString(test3);
        System.out.println("  Created: \"" + test3 + "\" at address " + str3Addr);
        System.out.println("  Length: " + stringStore.getStringLength(str3Addr));
        
        String test4 = "";
        int str4Addr = stringStore.createString(test4);
        System.out.println("  Created: \"" + test4 + "\" (empty) at address " + str4Addr);
        System.out.println("  Length: " + stringStore.getStringLength(str4Addr));
        
        System.out.println("\nRetrieving strings:");
        String retrieved1 = stringStore.getString(str1Addr);
        System.out.println("  Retrieved: \"" + retrieved1 + "\"");
        System.out.println("  Match: " + test1.equals(retrieved1));
        
        String retrieved2 = stringStore.getString(str2Addr);
        System.out.println("  Retrieved: \"" + retrieved2 + "\"");
        System.out.println("  Match: " + test2.equals(retrieved2));
        
        String retrieved3 = stringStore.getString(str3Addr);
        System.out.println("  Retrieved: \"" + retrieved3 + "\"");
        System.out.println("  Match: " + test3.equals(retrieved3));
        
        System.out.println("\nAccessing individual characters:");
        System.out.println("  str1[0] = '" + stringStore.getCharAt(str1Addr, 0) + "'");
        System.out.println("  str1[4] = '" + stringStore.getCharAt(str1Addr, 4) + "'");
        System.out.println("  str3[0] = '" + stringStore.getCharAt(str3Addr, 0) + "'");
        System.out.println("  str3[1] = '" + stringStore.getCharAt(str3Addr, 1) + "'");
        
        System.out.println("\nModifying character:");
        stringStore.setCharAt(str1Addr, 0, 'h');
        System.out.println("  Modified str1[0] to 'h'");
        System.out.println("  New string: \"" + stringStore.getString(str1Addr) + "\"");
        
        System.out.println("\nMemory layout for \"Hello\":");
        System.out.println("  Header (length): address " + str1Addr + " (4 bytes)");
        System.out.println("  Char data start: address " + (str1Addr + 4) + " (10 bytes for 5 chars)");
        System.out.println("  Total size: " + (4 + 5 * 2) + " bytes");
        System.out.println("  UTF-16 encoding: 2 bytes per character");
        System.out.println();
    }

    static void testHashTableStore() {
        System.out.println("Test 13: Hash Table");
        MemoryArena arena = new MemoryArena(1024);
        HashTableStore hashTable = new HashTableStore(arena);
        
        System.out.println("Creating hash table with 8 buckets:");
        int tableAddr = hashTable.createHashTable(8);
        System.out.println("  Table address: " + tableAddr);
        System.out.println("  Bucket count: " + hashTable.getBucketCount(tableAddr));
        
        System.out.println("\nInserting key-value pairs:");
        hashTable.put(tableAddr, 10, 100);
        System.out.println("  put(10, 100)");
        hashTable.put(tableAddr, 20, 200);
        System.out.println("  put(20, 200)");
        hashTable.put(tableAddr, 30, 300);
        System.out.println("  put(30, 300)");
        hashTable.put(tableAddr, 18, 180);
        System.out.println("  put(18, 180) - potential collision with 10");
        hashTable.put(tableAddr, 26, 260);
        System.out.println("  put(26, 260) - potential collision with 18");
        
        System.out.println("\nHash table structure:");
        hashTable.printHashTable(tableAddr);
        
        System.out.println("\nRetrieving values:");
        System.out.println("  get(10) = " + hashTable.get(tableAddr, 10));
        System.out.println("  get(20) = " + hashTable.get(tableAddr, 20));
        System.out.println("  get(30) = " + hashTable.get(tableAddr, 30));
        System.out.println("  get(18) = " + hashTable.get(tableAddr, 18));
        System.out.println("  get(26) = " + hashTable.get(tableAddr, 26));
        System.out.println("  get(99) = " + hashTable.get(tableAddr, 99));
        
        System.out.println("\nChecking contains:");
        System.out.println("  contains(10) = " + hashTable.contains(tableAddr, 10));
        System.out.println("  contains(99) = " + hashTable.contains(tableAddr, 99));
        
        System.out.println("\nUpdating existing key:");
        hashTable.put(tableAddr, 10, 1000);
        System.out.println("  put(10, 1000) - update");
        System.out.println("  get(10) = " + hashTable.get(tableAddr, 10));
        
        System.out.println("\nRemoving key:");
        hashTable.remove(tableAddr, 20);
        System.out.println("  remove(20)");
        System.out.println("  get(20) = " + hashTable.get(tableAddr, 20));
        System.out.println("  contains(20) = " + hashTable.contains(tableAddr, 20));
        
        System.out.println("\nHash table after removal:");
        hashTable.printHashTable(tableAddr);
        
        System.out.println("\nHash table details:");
        System.out.println("  Layout: [bucketCount][bucket array pointers]");
        System.out.println("  Entry layout: [key:4B][value:4B][next:4B]");
        System.out.println("  Collision resolution: Chaining (linked lists)");
        System.out.println("  Hash function: key % bucketCount");
        System.out.println();
    }
}
