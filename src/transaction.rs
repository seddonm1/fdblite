use async_trait::async_trait;
use etcd_client::proto::PbKeyValue;
use etcd_client::{
    Client, Compare, CompareOp, DeleteOptions, GetOptions, KeyRange, KeyValue, SortOrder,
    SortTarget, Txn, TxnOp, TxnResponse,
};
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::ops::Bound::Included;
use tokio::sync::oneshot;
use tokio::sync::{mpsc, mpsc::Receiver, mpsc::Sender};
use tokio::time::{sleep, Duration};

pub type Result<T> = std::result::Result<T, TransactionError>;

#[derive(Debug)]
pub enum TransactionError {
    Cancelled,
    ClientError(etcd_client::Error),
    CompareFailed,
    KeyDeleted,
    KeyNotFound(Key),
    KeyFound(Key),
    ValueNotFound,
    TooManyRetries,
    External(Box<dyn std::error::Error + Send + Sync>),
}

impl Error for TransactionError {}

impl Display for TransactionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &*self {
            TransactionError::Cancelled => write!(f, "Transaction Cancelled"),
            TransactionError::ClientError(ref desc) => write!(f, "{}", desc),
            TransactionError::CompareFailed => write!(f, "Transaction CompareFailed"),
            TransactionError::KeyDeleted => write!(f, "Transaction KeyDeleted"),
            TransactionError::TooManyRetries => write!(f, "Transaction TooManyRetries"),
            TransactionError::KeyNotFound(key) => {
                write!(f, "Transaction KeyNotFound {:?}", key)
            }
            TransactionError::ValueNotFound => {
                write!(f, "Transaction ValueNotFound")
            }
            TransactionError::KeyFound(key) => {
                write!(f, "Transaction KeyFound {:?}", key)
            }
            TransactionError::External(_) => write!(f, "Transaction External"),
        }
    }
}

#[derive(Debug)]
pub struct RangeOptions {
    limit: Option<i64>,
    reverse: Option<bool>,
}

impl From<etcd_client::Error> for TransactionError {
    fn from(err: etcd_client::Error) -> TransactionError {
        TransactionError::ClientError(err)
    }
}

pub type Key = Vec<u8>;

pub trait Range {
    fn key_range(&self) -> KeyRange;
}

#[derive(Clone, Debug)]
pub struct PrefixRange {
    prefix: Key,
}

impl PrefixRange {
    #[allow(dead_code)]
    pub fn new(prefix: impl Into<Key>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }
}

impl Range for PrefixRange {
    fn key_range(&self) -> KeyRange {
        let mut key_range = KeyRange::new();
        key_range.with_key(self.prefix.clone());
        key_range.with_prefix();
        key_range
    }
}

pub trait ExactRange {
    fn key_range(&self) -> KeyRange;
}

#[derive(Clone, Debug)]
pub struct CompleteRange {}

impl Range for CompleteRange {
    fn key_range(&self) -> KeyRange {
        let mut key_range = KeyRange::new();
        key_range.with_all_keys();
        key_range
    }
}

impl ExactRange for CompleteRange {
    fn key_range(&self) -> KeyRange {
        let mut key_range = KeyRange::new();
        key_range.with_all_keys();
        key_range
    }
}

#[derive(Debug)]
pub enum Command {
    Cancel {},
    Clear {
        key: Key,
    },
    ClearRange {
        key_range: KeyRange,
    },
    Commit {
        resp: oneshot::Sender<Result<TxnResponse>>,
    },
    Get {
        key: Key,
        resp: oneshot::Sender<Result<Option<KeyValue>>>,
    },
    GetOptions {
        resp: oneshot::Sender<TransactionOptions>,
    },
    GetRange {
        key_range: KeyRange,
        options: Option<RangeOptions>,
        resp: oneshot::Sender<Result<Vec<KeyValue>>>,
    },
    Reset {},
    Set {
        key: Key,
        value: Vec<u8>,
    },
    SetOptions {
        options: TransactionOptions,
    },
}

#[async_trait]
pub trait Transactor {
    async fn transact<F, Fut, T>(&self, closure: F) -> Result<(T, etcd_client::ResponseHeader)>
    where
        F: Fn(Transaction) -> Fut + Send + Sync,
        Fut: Future<Output = Result<T>> + Send,
        T: Send;
}

#[async_trait]
impl Transactor for Client {
    async fn transact<F, Fut, T>(&self, closure: F) -> Result<(T, etcd_client::ResponseHeader)>
    where
        F: Fn(Transaction) -> Fut + Send + Sync,
        Fut: Future<Output = Result<T>> + Send,
        T: Send,
    {
        let mut attempt = 0;

        let mut options = TransactionOptions {
            min_retry_delay: None,
            max_retry_delay: None,
            retry_limit: None,
        };

        loop {
            let (tx, rx) = mpsc::channel(4);
            let client = self.clone();

            let _ = tokio::spawn(async move {
                // Establish a connection to the server
                let mut tr = TransactionManager::new(client, rx, options, attempt);

                // Start receiving messages
                while let Some(cmd) = tr.rx.recv().await {
                    use Command::*;

                    match cmd {
                        Cancel {} => {
                            tr.cancel().await;
                        }
                        Clear { key } => {
                            tr.clear(key).await;
                        }
                        ClearRange { key_range } => {
                            tr.clear_range(key_range).await;
                        }
                        Commit { resp } => {
                            let _ = resp.send(tr.commit().await);
                            break;
                        }
                        Get { key, resp } => {
                            let _ = resp.send(tr.get(key).await);
                        }
                        GetOptions { resp } => {
                            let _ = resp.send(tr.options.clone());
                        }
                        GetRange {
                            key_range,
                            options,
                            resp,
                        } => {
                            let _ = resp.send(tr.get_range(key_range, options).await);
                        }
                        SetOptions { options } => tr.options = options,
                        Reset {} => {
                            tr.reset().await;
                        }
                        Set { key, value } => {
                            tr.set(key, value).await;
                        }
                    }
                }
            });

            let tr = Transaction { tx, attempt };

            match closure(tr.clone()).await {
                Ok(result) => {
                    let (resp_tx, resp_rx) = oneshot::channel();
                    let cmd = Command::GetOptions { resp: resp_tx };
                    tr.tx.send(cmd).await.unwrap();
                    options = resp_rx.await.unwrap();

                    let (resp_tx, resp_rx) = oneshot::channel();
                    let cmd = Command::Commit { resp: resp_tx };
                    tr.tx.send(cmd).await.unwrap();

                    match resp_rx.await.unwrap() {
                        Ok(txn_response) => {
                            break Ok((result, txn_response.header().unwrap().to_owned()))
                        }
                        Err(err) => match err {
                            // retry
                            TransactionError::CompareFailed => (),
                            // fatal
                            _ => {
                                break Err(err);
                            }
                        },
                    }
                }
                Err(err) => match err {
                    // retry
                    TransactionError::CompareFailed => {
                        let (resp_tx, resp_rx) = oneshot::channel();
                        let cmd = Command::GetOptions { resp: resp_tx };
                        tr.tx.send(cmd).await.unwrap();
                        options = resp_rx.await.unwrap();
                    }
                    // fatal
                    _ => {
                        break Err(err);
                    }
                },
            };

            attempt += 1;
            if let Some(retry_limit) = options.retry_limit {
                if attempt > retry_limit {
                    break Err(TransactionError::TooManyRetries);
                }
            }

            let mut backoff = (u64::pow(attempt as u64, 2) - 1) / 2;
            if let Some(min_retry_delay) = options.min_retry_delay {
                backoff = u64::max(backoff, min_retry_delay as u64);
            }
            if let Some(max_retry_delay) = options.max_retry_delay {
                backoff = u64::min(backoff, max_retry_delay as u64);
            }

            sleep(Duration::from_millis(backoff)).await;
        }
    }
}

#[derive(Clone)]
#[allow(dead_code)]
pub struct Transaction {
    tx: Sender<Command>,
    attempt: usize,
}

impl Transaction {
    #[allow(dead_code)]
    pub async fn cancel(&self) {
        let cmd = Command::Cancel {};

        self.tx.send(cmd).await.unwrap();
    }

    #[allow(dead_code)]
    pub async fn clear(&self, key: impl Into<Key>) {
        let cmd = Command::Clear { key: key.into() };

        self.tx.send(cmd).await.unwrap();
    }

    #[allow(dead_code)]
    pub async fn clear_range(&self, range: &(dyn ExactRange + Send + Sync)) {
        let cmd = Command::ClearRange {
            key_range: range.key_range(),
        };

        self.tx.send(cmd).await.unwrap()
    }

    #[allow(dead_code)]
    pub async fn get(&self, key: impl Into<Key>) -> Result<Option<KeyValue>> {
        let (resp_tx, resp_rx) = oneshot::channel();

        let cmd = Command::Get {
            key: key.into(),
            resp: resp_tx,
        };

        self.tx.send(cmd).await.unwrap();
        resp_rx.await.unwrap()
    }

    #[allow(dead_code)]
    pub async fn get_range(
        &self,
        range: Box<dyn Range + Send + Sync>,
        options: Option<RangeOptions>,
    ) -> Result<Vec<KeyValue>> {
        let (resp_tx, resp_rx) = oneshot::channel();

        let cmd = Command::GetRange {
            key_range: range.key_range(),
            options,
            resp: resp_tx,
        };

        self.tx.send(cmd).await.unwrap();
        resp_rx.await.unwrap()
    }

    #[allow(dead_code)]
    pub async fn options(&self, options: TransactionOptions) {
        let cmd = Command::SetOptions { options };

        self.tx.send(cmd).await.unwrap();
    }

    #[allow(dead_code)]
    pub async fn reset(&self) {
        let cmd = Command::Reset {};

        self.tx.send(cmd).await.unwrap();
    }

    #[allow(dead_code)]
    pub async fn set(&self, key: impl Into<Key>, value: impl Into<Vec<u8>>) {
        let cmd = Command::Set {
            key: key.into(),
            value: value.into(),
        };

        self.tx.send(cmd).await.unwrap();
    }

    #[allow(dead_code)]
    pub async fn add_read_conflict_key(&self, key: impl Into<Key>) {
        self.get(key).await.unwrap();
    }
}

#[derive(Debug, Clone)]
pub struct TransactionOptions {
    min_retry_delay: Option<usize>,
    max_retry_delay: Option<usize>,
    retry_limit: Option<usize>,
}

struct TransactionManager {
    client: Client,
    rx: Receiver<Command>,
    options: TransactionOptions,
    cancelled: bool,
    attempt: usize,
    whens: Vec<Compare>,
    gets: HashMap<Vec<u8>, KeyValue>,
    sets: BTreeMap<Vec<u8>, TxnOp>,
    clears: HashMap<Vec<u8>, TxnOp>,
}

impl TransactionManager {
    #[allow(dead_code)]
    pub fn new(
        client: Client,
        rx: Receiver<Command>,
        options: TransactionOptions,
        attempt: usize,
    ) -> Self {
        TransactionManager {
            client,
            rx,
            options,
            cancelled: false,
            attempt,
            whens: vec![],
            gets: HashMap::new(),
            sets: BTreeMap::new(),
            clears: HashMap::new(),
        }
    }

    async fn cancel(&mut self) {
        self.cancelled = true;
    }

    async fn clear(&mut self, key: Key) {
        let delete_op = TxnOp::delete(key, None);

        self.clears.insert(delete_op.key(), delete_op.clone());
        self.sets.remove(&delete_op.key());
    }

    async fn clear_range(&mut self, key_range: KeyRange) {
        let delete_op = TxnOp::delete(
            key_range.clone().key,
            Some(DeleteOptions::new().with_key_range(key_range.clone())),
        );

        self.clears.insert(delete_op.key(), delete_op);
        for (key, _) in self.sets.clone().range((
            Included(key_range.clone().key),
            Included(key_range.range_end),
        )) {
            self.sets.remove(key);
        }
    }

    async fn commit(&mut self) -> Result<TxnResponse> {
        if self.cancelled {
            Err(TransactionError::Cancelled)
        } else {
            let txn_response = self
                .client
                .clone()
                .txn(
                    Txn::new().when(self.whens.clone()).and_then(
                        [
                            self.clears.values().cloned().collect::<Vec<_>>(),
                            self.sets.values().cloned().collect::<Vec<_>>(),
                        ]
                        .concat(),
                    ),
                )
                .await?;

            if txn_response.succeeded() {
                Ok(txn_response)
            } else {
                self.attempt += 1;
                Err(TransactionError::CompareFailed)
            }
        }
    }

    async fn get(&mut self, key: Key) -> Result<Option<KeyValue>> {
        let get_op = TxnOp::get(key, None);

        if self.clears.get(&get_op.key()).is_some() {
            return Err(TransactionError::KeyDeleted);
        }

        if let Some(put_op) = self.sets.get(&get_op.key()) {
            match self.gets.get(&get_op.key()) {
                Some(get) => {
                    return Ok(Some(KeyValue(PbKeyValue {
                        key: put_op.key(),
                        value: put_op.value(),
                        lease: get.lease(),
                        create_revision: get.create_revision(),
                        mod_revision: get.mod_revision() + 1,
                        version: get.version() + 1,
                    })))
                }
                None => {
                    return Ok(Some(KeyValue(PbKeyValue {
                        key: put_op.key(),
                        value: put_op.value(),
                        lease: 0,
                        create_revision: 0,
                        mod_revision: 0,
                        version: -1,
                    })))
                }
            }
        }

        if let Some(get) = self.gets.get(&get_op.key()) {
            return Ok(Some(get.clone()));
        }

        let get_response = &self.client.get(get_op.key(), None).await?;

        Ok(match get_response.kvs().first() {
            Some(kv) => {
                // add get version to transaction whens
                self.whens.push(Compare::version(
                    get_op.key(),
                    CompareOp::Equal,
                    kv.version(),
                ));

                // add get kv to transaction cache
                self.gets.insert(get_op.key(), kv.clone());

                Some(kv.clone())
            }
            None => {
                // add not exists compare to transaction whens
                self.whens
                    .push(Compare::version(get_op.key(), CompareOp::Equal, 0));

                None
            }
        })
    }

    async fn get_range(
        &mut self,
        key_range: KeyRange,
        options: Option<RangeOptions>,
    ) -> Result<Vec<KeyValue>> {
        let get_options = match options {
            Some(options) => GetOptions::new()
                .with_key_range(key_range.clone())
                .with_limit(options.limit.unwrap_or(0))
                .with_sort(
                    SortTarget::Key,
                    options
                        .reverse
                        .map(|reverse| {
                            if reverse {
                                SortOrder::Descend
                            } else {
                                SortOrder::Ascend
                            }
                        })
                        .unwrap_or(SortOrder::Ascend),
                ),
            None => GetOptions::new().with_key_range(key_range.clone()),
        };

        let get_response = &self
            .client
            .get(key_range.clone().key, Some(get_options))
            .await?;

        get_response
            .kvs()
            .iter()
            .map(|kv| {
                // add get kv to transaction cache
                match self.gets.get(kv.key()) {
                    Some(get_kv) => {
                        if get_kv.version() != kv.version() {
                            return Err(TransactionError::CompareFailed);
                        }
                    }
                    None => {
                        self.whens
                            .push(Compare::version(kv.key(), CompareOp::Equal, kv.version()));

                        self.gets.insert(kv.key().to_vec(), kv.clone());
                    }
                }

                Ok(kv.clone())
            })
            .collect::<Result<Vec<_>>>()
    }

    async fn reset(&mut self) {
        self.cancelled = false;
        self.whens = vec![];
        self.gets = HashMap::new();
        self.sets = BTreeMap::new();
        self.clears = HashMap::new();
    }

    async fn set(&mut self, key: Key, value: Vec<u8>) {
        let put_op = TxnOp::put(key, value, None);

        self.sets
            .entry(put_op.key())
            .and_modify(|e| *e = put_op.clone())
            .or_insert_with(|| put_op.clone());
        self.clears.remove(&put_op.key());
    }
}

#[cfg(test)]
mod tests {
    use crate::transaction::{
        CompleteRange, PrefixRange, RangeOptions, TransactionError, Transactor,
    };
    use etcd_client::Client;
    use futures::future::join_all;
    use serial_test::serial;

    use super::TransactionOptions;

    #[tokio::test]
    #[serial]
    async fn test_put_conflict() {
        let client = Client::connect(["localhost:2379"], None).await.unwrap();
        client
            .transact(|tr| async move {
                let range = CompleteRange {};
                tr.clear_range(&range).await;

                tr.set("abc", 0i64.to_le_bytes()).await;
                Ok(())
            })
            .await
            .unwrap();

        let n: i64 = 50;

        let clients = (0..n)
            .map(|i: i64| {
                tokio::spawn(async move {
                    let client = Client::connect(["localhost:2379"], None).await.unwrap();
                    client
                        .transact(|tr| async move {
                            println!("execute {}: {:?}", i, tr.attempt);

                            tr.options(TransactionOptions {
                                min_retry_delay: Some(10),
                                max_retry_delay: Some(1000),
                                retry_limit: None,
                            })
                            .await;

                            let kv = tr.get("abc").await.unwrap().unwrap();
                            let count = i64::from_le_bytes(kv.value().try_into().unwrap());
                            tr.set(kv.key(), (count + 1).to_le_bytes()).await;
                            Ok(tr.get("abc").await.unwrap())
                        })
                        .await
                        .unwrap()
                })
            })
            .collect::<Vec<_>>();

        let results = join_all(clients).await;
        assert_eq!(
            results
                .iter()
                .map(|result| match result {
                    Ok(result) => match result {
                        (Some(kv), _) => i64::from_le_bytes(kv.value().try_into().unwrap()),
                        _ => {
                            panic!();
                        }
                    },
                    Err(_) => {
                        panic!();
                    }
                })
                .collect::<Vec<_>>()
                .iter()
                .max()
                .cloned()
                .unwrap(),
            n
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_put_conflict_options() {
        let client = Client::connect(["localhost:2379"], None).await.unwrap();
        client
            .transact(|tr| async move {
                let range = CompleteRange {};
                tr.clear_range(&range).await;

                tr.set("abc", "initial").await;
                Ok(())
            })
            .await
            .unwrap();

        let a = tokio::spawn(async {
            let client = Client::connect(["localhost:2379"], None).await.unwrap();
            client
                .transact(|tr| async move {
                    println!("execute a: {:?}", tr.attempt);

                    tr.options(TransactionOptions {
                        min_retry_delay: Some(0),
                        max_retry_delay: Some(0),
                        retry_limit: Some(0),
                    })
                    .await;

                    let kv = tr.get("abc").await.unwrap().unwrap();
                    tr.set(kv.key(), "a").await;
                    Ok(tr.get("abc").await.unwrap())
                })
                .await
        });

        let b = tokio::spawn(async {
            let client = Client::connect(["localhost:2379"], None).await.unwrap();
            client
                .transact(|tr| async move {
                    println!("execute b: {:?}", tr.attempt);

                    tr.options(TransactionOptions {
                        min_retry_delay: Some(0),
                        max_retry_delay: Some(0),
                        retry_limit: Some(0),
                    })
                    .await;

                    let kv = tr.get("abc").await.unwrap().unwrap();
                    tr.set(kv.key(), "b").await;
                    Ok(tr.get("abc").await.unwrap())
                })
                .await
        });

        match tokio::try_join!(a, b) {
            Ok((Ok((Some(a), _)), Ok((Some(b), _)))) => {
                assert_eq!(i64::abs(a.mod_revision() - b.mod_revision()), 1);
                println!("{:?}", a);
                println!("{:?}", b);
            }
            Ok((Err(TransactionError::TooManyRetries), _)) => {}
            Ok((_, Err(TransactionError::TooManyRetries))) => {}
            _ => panic!(),
        }
    }

    #[tokio::test]
    #[serial]
    async fn test_get_range_prefix() {
        let client = Client::connect(["localhost:2379"], None).await.unwrap();
        client
            .transact(|tr| async move {
                let range = CompleteRange {};
                tr.clear_range(&range).await;
                tr.set("alpha", "1").await;
                tr.set("alphabetA", "2").await;
                tr.set("alphabetB", "3").await;
                tr.set("alphabetize", "4").await;
                tr.set("beta", "5").await;
                Ok(())
            })
            .await
            .unwrap();

        let range = client
            .transact(|tr| async move {
                let range = PrefixRange::new("alphabet");
                tr.get_range(Box::new(range), None).await
            })
            .await
            .unwrap();

        assert_eq!(range.0.len(), 3);
    }

    #[tokio::test]
    #[serial]
    async fn test_get_range_complete() {
        let client = Client::connect(["localhost:2379"], None).await.unwrap();
        client
            .transact(|tr| async move {
                let range = CompleteRange {};
                tr.clear_range(&range).await;
                tr.set("alpha", "1").await;
                tr.set("alphabetA", "2").await;
                tr.set("alphabetB", "3").await;
                tr.set("alphabetize", "4").await;
                tr.set("beta", "5").await;
                Ok(())
            })
            .await
            .unwrap();

        let range = client
            .transact(|tr| async move {
                let range = CompleteRange {};
                tr.get_range(Box::new(range), None).await
            })
            .await
            .unwrap();

        assert_eq!(range.0.len(), 5);
    }

    #[tokio::test]
    #[serial]
    async fn test_get_range_complete_options() {
        let client = Client::connect(["localhost:2379"], None).await.unwrap();
        client
            .transact(|tr| async move {
                let range = CompleteRange {};
                tr.clear_range(&range).await;
                tr.set("alpha", "1").await;
                tr.set("alphabetA", "2").await;
                tr.set("alphabetB", "3").await;
                tr.set("alphabetize", "4").await;
                tr.set("beta", "5").await;
                Ok(())
            })
            .await
            .unwrap();

        let range = client
            .transact(|tr| async move {
                let range = CompleteRange {};
                tr.get_range(
                    Box::new(range),
                    Some(RangeOptions {
                        limit: Some(1),
                        reverse: Some(true),
                    }),
                )
                .await
            })
            .await
            .unwrap();

        assert_eq!(range.0.len(), 1);
        assert_eq!(range.0.first().unwrap().key(), "beta".as_bytes());
    }
}
