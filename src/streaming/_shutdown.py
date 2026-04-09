"""Shared graceful shutdown loop for streaming jobs."""

import signal


def run_until_shutdown(spark, *queries, job_label="streaming"):
    """Block until SIGINT/SIGTERM, then stop all queries cleanly."""
    _shutdown = False

    def _stop(sig, frame):
        nonlocal _shutdown
        print(f"\nCaught signal {sig}, shutting down {job_label}...")
        _shutdown = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    last_batch = {q.name: -1 for q in queries}
    while any(q.isActive for q in queries):
        if _shutdown:
            for q in queries:
                q.stop()
                print(f"  stopped {q.name}")
            break
        for q in queries:
            progress = q.lastProgress
            if progress and progress.get("batchId", -1) > last_batch[q.name]:
                last_batch[q.name] = progress["batchId"]
                rows = progress.get("numInputRows", 0)
                if rows > 0:
                    print(f"  {q.name}: batch {last_batch[q.name]}, {rows} rows")
        spark.streams.awaitAnyTermination(timeout=1)

    print("done")
