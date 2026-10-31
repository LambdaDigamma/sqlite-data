import ConcurrencyExtras
import SQLiteData
import Testing

@Suite
struct CoalescingStatePersistenceTests {
  @Test func coalescesWhileTheWriterIsBlocked() async {
    let entered = Gate()
    let release = Gate()
    let calls = LockIsolated(0)
    let writes = LockIsolated<[[String: Int]]>([])
    let persistence = CoalescingStatePersistence<String, Int> { takeLatest in
      calls.withValue { $0 += 1 }
      entered.signal()
      await release.wait()
      writes.withValue { $0.append(takeLatest()) }
    }

    persistence.enqueue(0, for: "private")
    await entered.wait()
    for value in 1...1_000 {
      persistence.enqueue(value, for: "private")
    }
    persistence.enqueue(42, for: "shared")

    #expect(calls.withValue { $0 } == 1)
    #expect(persistence.latestValue(for: "private") == 1_000)
    #expect(persistence.latestValue(for: "shared") == 42)
    release.signal()
    await persistence.flush()

    #expect(writes.withValue { $0 } == [["private": 1_000, "shared": 42]])
  }

  @Test func drainsValuesReceivedDuringAWrite() async {
    let entered = Gate()
    let release = Gate()
    let writes = LockIsolated<[[String: Int]]>([])
    let persistence = CoalescingStatePersistence<String, Int> { takeLatest in
      let values = takeLatest()
      let isFirst = writes.withValue { writes in
        writes.append(values)
        return writes.count == 1
      }
      if isFirst {
        entered.signal()
        await release.wait()
      }
    }

    persistence.enqueue(1, for: "private")
    await entered.wait()
    persistence.enqueue(2, for: "private")
    persistence.enqueue(3, for: "shared")
    release.signal()
    await persistence.flush()

    #expect(writes.withValue { $0 } == [["private": 1], ["private": 2, "shared": 3]])
    #expect(persistence.latestValue(for: "private") == 2)
    #expect(persistence.latestValue(for: "shared") == 3)
  }

  @Test func retainsTheLatestCheckpointAfterDraining() async {
    let writes = LockIsolated<[[String: Int]]>([])
    let persistence = CoalescingStatePersistence<String, Int> { takeLatest in
      writes.withValue { $0.append(takeLatest()) }
    }

    persistence.enqueue(7, for: "private")
    await persistence.flush()
    #expect(persistence.latestValue(for: "private") == 7)
    persistence.enqueue(9, for: "private")
    await persistence.flush()

    #expect(persistence.latestValue(for: "private") == 9)
    #expect(writes.withValue { $0 } == [["private": 7], ["private": 9]])
    persistence.invalidate()
    #expect(persistence.latestValue(for: "private") == nil)
  }

  @Test func invalidationDiscardsAQueuedWriteWithoutConsumingNewValues() async {
    let entered = Gate()
    let release = Gate()
    let calls = LockIsolated(0)
    let writes = LockIsolated<[[String: Int]]>([])
    let persistence = CoalescingStatePersistence<String, Int> { takeLatest in
      let isFirst = calls.withValue { calls in
        calls += 1
        return calls == 1
      }
      if isFirst {
        entered.signal()
        await release.wait()
      }
      let values = takeLatest()
      if !values.isEmpty {
        writes.withValue { $0.append(values) }
      }
    }

    persistence.enqueue(1, for: "private")
    await entered.wait()
    persistence.invalidate()
    #expect(persistence.latestValue(for: "private") == nil)
    persistence.enqueue(2, for: "private")
    release.signal()
    await persistence.flush()

    #expect(calls.withValue { $0 } == 2)
    #expect(writes.withValue { $0 } == [["private": 2]])
    #expect(persistence.latestValue(for: "private") == 2)
  }

  @Test func anErrorBeforeTakingDoesNotBlockLaterWrites() async {
    let calls = LockIsolated(0)
    let writes = LockIsolated<[[String: Int]]>([])
    let persistence = CoalescingStatePersistence<String, Int> { takeLatest in
      let isFirst = calls.withValue { calls in
        calls += 1
        return calls == 1
      }
      if isFirst { return }
      writes.withValue { $0.append(takeLatest()) }
    }

    persistence.enqueue(1, for: "private")
    await persistence.flush()
    #expect(writes.withValue { $0 }.isEmpty)
    persistence.enqueue(2, for: "shared")
    await persistence.flush()

    #expect(calls.withValue { $0 } == 2)
    #expect(writes.withValue { $0 } == [["private": 1, "shared": 2]])
  }

  @Test func retriesANewValueReceivedDuringAFailedWrite() async {
    let entered = Gate()
    let release = Gate()
    let calls = LockIsolated(0)
    let writes = LockIsolated<[[String: Int]]>([])
    let persistence = CoalescingStatePersistence<String, Int> { takeLatest in
      let isFirst = calls.withValue { calls in
        calls += 1
        return calls == 1
      }
      if isFirst {
        entered.signal()
        await release.wait()
        return
      }
      writes.withValue { $0.append(takeLatest()) }
    }

    persistence.enqueue(1, for: "private")
    await entered.wait()
    persistence.enqueue(2, for: "private")
    release.signal()
    await persistence.flush()

    #expect(calls.withValue { $0 } == 2)
    #expect(writes.withValue { $0 } == [["private": 2]])
  }

  @Test func anErrorAfterTakingDoesNotBlockLaterWrites() async {
    let calls = LockIsolated(0)
    let writes = LockIsolated<[[String: Int]]>([])
    let persistence = CoalescingStatePersistence<String, Int> { takeLatest in
      let values = takeLatest()
      let isFirst = calls.withValue { calls in
        calls += 1
        return calls == 1
      }
      if isFirst { return }
      writes.withValue { $0.append(values) }
    }

    persistence.enqueue(1, for: "private")
    await persistence.flush()
    #expect(persistence.latestValue(for: "private") == 1)
    persistence.enqueue(2, for: "private")
    await persistence.flush()

    #expect(calls.withValue { $0 } == 2)
    #expect(writes.withValue { $0 } == [["private": 2]])
  }

  private struct Gate: Sendable {
    private let stream: AsyncStream<Void>
    private let continuation: AsyncStream<Void>.Continuation

    init() {
      (stream, continuation) = AsyncStream.makeStream()
    }

    func signal() {
      continuation.yield()
    }

    func wait() async {
      for await _ in stream { break }
    }
  }
}
