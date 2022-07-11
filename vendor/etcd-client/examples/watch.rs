//! Watch example

use etcd_client::*;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut client = Client::connect(["localhost:2379"], None).await?;

    let (watcher, mut stream) = client
        .watch(vec![], Some(WatchOptions::new().with_all_keys()))
        .await?;
    println!("create watcher {}", watcher.watch_id());
    println!();

    while let Some(resp) = stream.message().await? {
        println!("receive watch response");

        if resp.canceled() {
            println!("watch canceled!");
            break;
        }

        for event in resp.events() {
            println!("event type: {:?}", event.event_type());
            if let Some(kv) = event.kv() {
                println!("kv: {{{}: {}}}", kv.key_str()?, kv.value_str()?);
            }
        }
    }

    Ok(())
}
