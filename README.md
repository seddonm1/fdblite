# fdblite

_EXPERIMENTAL_ FoundationDB-like [Transactions](https://apple.github.io/foundationdb/transaction-manifesto.html) implemented for [etcd](https://etcd.io/) on top of the Rust [etcd-client](https://docs.rs/etcd-client/latest/etcd_client/) crate.

Ths project is inspired by the [go fdb bindings](https://pkg.go.dev/github.com/apple/foundationdb/bindings/go/src/fdb) to implement the automatic [transaction retry loops](https://apple.github.io/foundationdb/developer-guide.html#transaction-retry-loops) functionality of the [Database.Transact](https://pkg.go.dev/github.com/apple/foundationdb/bindings/go/src/fdb#Database.Transact) method which presents a beautifully simple API to the user but hides a lot of complexity.

## WARNING

This project was a byproduct of experimenting with highly-available storage provided by [etcd](https://etcd.io/) and very likely has bugs. Use at your own risk.

## How does it work

A `fdblite` `Transaction` is a state-machine that manages a set of `conflict` keys to provide serializability using optimistic concurrency. Upon execution `fdblite` generates the `if/then/else` style [etcd transactions](https://etcd.io/docs/v3.5/learning/api/#transaction) so the user does not have to manage this state-machine themselves. This means the user is presented with a simple API but provides the benefits of [abstractions](https://apple.github.io/foundationdb/transaction-manifesto.html#transactions-enable-abstraction) and [flexibility](https://apple.github.io/foundationdb/transaction-manifesto.html#transactions-enable-flexibility) spoken about by FoundationDB.

The `fdblite` `Transaction` also implements [transaction retry loops](https://apple.github.io/foundationdb/developer-guide.html#transaction-retry-loops). This means that all transactions need to be [idempotent](https://apple.github.io/foundationdb/developer-guide.html#transactions-with-unknown-results).

## Operations

- `get` returns the value associated with the specified key.
- `set` sets associated the given key and value.
- `clear` removes the specified key.
- `get_range` performs a range read.
- `clear_range` removes all keys k such that begin <= k < end.
- `add_read_conflict_key` adds a key to the transactions read conflict range as if you had read the key.
- `options` configures the transaction retry behavior.

## Example

Here is a basic transaction implementation from the FoundationDB [documentation](https://apple.github.io/foundationdb/developer-guide.html#transaction-basics) in Python:

```python
@fdb.transactional
def example(tr):
    # Read two values from the database
    a = tr.get('a')
    b = tr.get('b')
    # Write two key-value pairs to the database
    tr.set('c', a+b)
    tr.set('d', a+b)

example(db)
```

The same thing implemented with `fdblite`:

```rust
client
    .transact(|tr| async move {
        // Read two values from the database
        let a = tr.get("a").await?.unwrap();
        let b = tr.get("b").await?.unwrap();
        // Write two key-value pairs to the database
        tr.set("c", [a.value(), b.value()].concat()).await;
        tr.set("d", [a.value(), b.value()].concat()).await;
        Ok(())
    })
    .await?;
```

## Why etcd?

[etcd](https://etcd.io/) is a [Jepsen](https://jepsen.io/analyses/etcd-3.4.3) proven RAFT-based reliable orderered-key-value store similar to FoundationDB that is easy to deploy (or it may already be deplyed in your K8s cluster). It may be perfect for smaller datasets which do not need the scaling of FoundationDB.

## etcd configuration

Because of the way the `Transaction` manages state the number of operations per transaction sent to `etcd` may be higher than the default `128`. It can be increased like:

- `max-txn-ops=4096`
