use xho::daemon::session::TargetSession;
use xho::daemon::session::local::LocalSession;

// Mirrors the 2222 proxy bridge data path: client data arrives as Data
// commands while stdout events are relayed. Slow-consumer must not stall the
// writer after sustained traffic.
#[tokio::test(flavor = "multi_thread")]
async fn sustained_stdin_does_not_stall() {
    let sess: Box<dyn TargetSession> =
        Box::new(LocalSession::new("/bin/sh".to_string(), None, None));
    let (writer, mut stream) = sess.split();
    // echo loop: every tick we SEND comes back on stdout => exercises BOTH directions under backpressure.
    writer
        .exec("while read line; do echo \"reply:$line\"; done; echo LOOP-EXITED")
        .await
        .unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<usize>();
    let pump = tokio::spawn(async move {
        for i in 0..600 {
            let line = format!("tick-{i}\n");
            let t0 = std::time::Instant::now();
            if let Err(e) = writer.write_stdin(line.as_bytes()).await {
                panic!("writer closed at tick {i}: {e}");
            }
            if t0.elapsed() > std::time::Duration::from_secs(2) {
                panic!(
                    "write_stdin PARKED {}s at tick {i}",
                    t0.elapsed().as_secs_f32()
                );
            }
            tx.send(i).unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    });

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(40);
    let mut replies = 0usize;
    let mut last_tick = 0usize;
    while replies < 550 {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "STALL: only {replies} replies, last tick {last_tick}"
        );
        match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(xho::daemon::session::SessionEvent::Stdout(d))) => {
                let s = String::from_utf8_lossy(&d);
                if let Some(rest) = s.strip_prefix("reply:tick-") {
                    replies += 1;
                    last_tick = rest.trim().parse().unwrap_or(last_tick);
                }
            }
            Ok(Some(_)) => {}
            _ => break,
        }
    }
    let _ = pump.await;
    // After the input side finishes, drain whatever is buffered: the chain
    // must deliver every reply (nothing may be dropped or wedged).
    let drain_deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while replies < 550 && std::time::Instant::now() < drain_deadline {
        match tokio::time::timeout(std::time::Duration::from_millis(500), stream.next()).await {
            Ok(Some(xho::daemon::session::SessionEvent::Stdout(d))) => {
                let s = String::from_utf8_lossy(&d);
                if s.starts_with("reply:tick-") {
                    replies += 1;
                }
            }
            Ok(Some(_)) => {}
            _ => break,
        }
    }
    assert!(replies >= 550, "replies lost: {replies}/550");
}
