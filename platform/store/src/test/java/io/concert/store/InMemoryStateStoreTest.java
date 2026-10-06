package io.concert.store;

class InMemoryStateStoreTest extends StateStoreContract {
    private final InMemoryStateStore store = new InMemoryStateStore();

    @Override
    protected StateStore store() {
        return store;
    }
}
