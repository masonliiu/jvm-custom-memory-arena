public class MemoryArenaBehaviorTest {
    public static void main(String[] args) {
        testIntegerRoundTripAndEndianOrder();
        testPrimitiveRoundTrips();
        testBoundsAreEnforced();
        System.out.println("MemoryArenaBehaviorTest passed");
    }

    private static void testIntegerRoundTripAndEndianOrder() {
        MemoryArena arena = new MemoryArena(16);
        int address = arena.alloc(4);
        arena.putInt(address, 0x12345678);
        assert arena.getInt(address) == 0x12345678;
        assert (arena.memory[address] & 0xff) == 0x12;
        assert (arena.memory[address + 1] & 0xff) == 0x34;
        assert (arena.memory[address + 2] & 0xff) == 0x56;
        assert (arena.memory[address + 3] & 0xff) == 0x78;
    }

    private static void testPrimitiveRoundTrips() {
        MemoryArena arena = new MemoryArena(32);
        int longAddress = arena.alloc(8);
        int shortAddress = arena.alloc(2);
        int charAddress = arena.alloc(2);
        int booleanAddress = arena.alloc(1);
        arena.putLong(longAddress, Long.MIN_VALUE);
        arena.putShort(shortAddress, Short.MIN_VALUE);
        arena.putChar(charAddress, '\uffff');
        arena.putBoolean(booleanAddress, true);
        assert arena.getLong(longAddress) == Long.MIN_VALUE;
        assert arena.getShort(shortAddress) == Short.MIN_VALUE;
        assert arena.getChar(charAddress) == '\uffff';
        assert arena.getBoolean(booleanAddress);
    }

    private static void testBoundsAreEnforced() {
        MemoryArena arena = new MemoryArena(8);
        arena.alloc(4);
        boolean rejected = false;
        try {
            arena.putLong(0, 1L);
        } catch (InvalidAddressException expected) {
            rejected = true;
        }
        assert rejected : "an eight-byte write outside the allocated range must fail";
    }
}
