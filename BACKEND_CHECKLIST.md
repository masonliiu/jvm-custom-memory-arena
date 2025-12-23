# Backend Development Roadmap

## Phase 1: Foundation & Code Quality 

### 1.1 Extract NodeStore from MemoryArena 
- [x] Create NodeStore class with MemoryArena reference
- [x] Move NODE_SIZE, VALUE_OFFSET, NEXT_OFFSET constants
- [x] Move createNode(), getValue(), setNext(), getNext(), checkNodePtr() methods
- [x] Move printList() method
- [x] Update MemoryArena to remove node-specific code
- [x] Update Main.java to use NodeStore

### 1.2 Improve Error Messages & Diagnostics 
- [x] Create MemoryException base class
- [x] Create OutOfMemoryException
- [x] Create InvalidPointerException
- [x] Create InvalidAddressException
- [x] Add context to error messages (address, size, offset, capacity)
- [x] Update all throw statements to use new exceptions

### 1.3 Add Alignment Support 
- [x] Create align() helper method
- [x] Add allocAligned() method
- [x] Track alignment waste
- [x] Update NodeStore to use aligned allocation

## Phase 2: Additional Primitive Types

### 2.1 Add long support (8 bytes)
- [x] Implement putLong(int addr, long x)
- [x] Implement getLong(int addr)
- [x] Test big-endian encoding/decoding

### 2.2 Add short support (2 bytes)
- [x] Implement putShort(int addr, short x)
- [x] Implement getShort(int addr)
- [x] Test 2-byte big-endian encoding

### 2.3 Add char support (2 bytes, UTF-16)
- [x] Implement putChar(int addr, char c)
- [x] Implement getChar(int addr)
- [x] Test character encoding

### 2.4 Add boolean support (1 byte)
- [x] Implement putBoolean(int addr, boolean b)
- [x] Implement getBoolean(int addr)
- [x] Test boolean representation

## Phase 3: Structured Data Types

### 3.1 Implement Fixed-Size Arrays
- [x] Create ArrayStore.java
- [x] Define array layout: [length][data]
- [x] Implement createArray(int length, int elementSize)
- [x] Implement getElement() and setElement()
- [x] Add bounds checking

### 3.2 Implement Dynamic Arrays (Vector-like)
- [x] Define vector layout: [length][capacity][data pointer]
- [x] Implement createVector(int initialCapacity)
- [x] Implement append() method
- [x] Implement grow() method
- [x] Handle reallocation

### 3.3 Implement String Storage
- [x] Define string layout: [length][char data]
- [x] Implement createString(String s)
- [x] Implement getString(int addr)
- [x] Implement getStringLength()
- [x] Handle UTF-16 encoding

### 3.4 Implement Hash Table (Advanced)
- [x] Define hash table layout
- [x] Implement hash function
- [x] Implement bucket array
- [x] Implement collision resolution
- [x] Add get/put operations

## Phase 4: Advanced Memory Management

### 4.1 Add Memory Regions/Segments
- [ ] Create MemoryRegion class
- [ ] Track multiple regions
- [ ] Validate addresses against regions

### 4.2 Implement Free List Allocator (Optional)
- [ ] Track freed blocks
- [ ] Implement free() method
- [ ] Modify alloc() to check free list
- [ ] Handle fragmentation

### 4.3 Add Memory Statistics & Visualization
- [ ] Track allocation statistics
- [ ] Implement getStats() method
- [ ] Implement visualize() method
- [ ] Print memory layout diagrams

## Phase 5: Data Structure Operations

### 5.1 Linked List Operations
- [ ] Implement insertAfter(int nodeAddr, int val)
- [ ] Implement deleteAfter(int nodeAddr)
- [ ] Implement find(int headAddr, int val)
- [ ] Implement reverse(int headAddr)

### 5.2 Stack Implementation
- [ ] Define stack layout: [top pointer][capacity]
- [ ] Implement push(), pop(), peek()
- [ ] Implement isEmpty()

### 5.3 Queue Implementation
- [ ] Define queue layout: [head pointer][tail pointer]
- [ ] Implement enqueue(), dequeue()
- [ ] Handle empty queue case

## Phase 6: Testing & Documentation

### 6.1 Unit Tests for All Operations
- [ ] Test success cases
- [ ] Test failure cases
- [ ] Test edge cases
- [ ] Test bounds violations

### 6.2 Memory Layout Diagrams Generator
- [ ] Implement printMemoryLayout()
- [ ] Implement printStructureLayout()
- [ ] Color-code allocated vs free

### 6.3 Comprehensive Examples
- [ ] Create Examples.java
- [ ] Basic allocation examples
- [ ] Linked list operations
- [ ] Array operations
- [ ] Error case demonstrations

