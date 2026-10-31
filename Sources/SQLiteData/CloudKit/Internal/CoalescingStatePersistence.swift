import ConcurrencyExtras

/// Keeps the latest complete checkpoint for each key while persistence waits for a writer.
package final class CoalescingStatePersistence<Key: Hashable & Sendable, Value: Sendable>:
  Sendable
{
  private struct State {
    var latest: [Key: Value] = [:]
    var pending: [Key: Value] = [:]
    var generation = 0
    var revision = 0
    var worker: Task<Void, Never>?
  }

  private let state = LockIsolated(State())
  private let persist: @Sendable (@escaping @Sendable () -> [Key: Value]) async -> Void

  /// The persistence operation must take the latest values inside its writer closure.
  package init(
    persist: @escaping @Sendable (@escaping @Sendable () -> [Key: Value]) async -> Void
  ) {
    self.persist = persist
  }

  package func enqueue(_ value: Value, for key: Key) {
    state.withValue { state in
      state.latest[key] = value
      state.pending[key] = value
      state.revision += 1
      if state.worker == nil {
        state.worker = Task { await drain() }
      }
    }
  }

  /// Includes checkpoints that are still waiting to be persisted.
  package func latestValue(for key: Key) -> Value? {
    state.withValue { $0.latest[key] }
  }

  /// Invalidates queued checkpoints before the caller's metadata-reset writer barrier.
  package func invalidate() {
    state.withValue { state in
      state.generation += 1
      state.latest.removeAll()
      state.pending.removeAll()
    }
  }

  /// Waits for queued work. The injected persistence operation reports any write errors.
  package func flush() async {
    while let worker = state.withValue({ $0.worker }) {
      await worker.value
    }
  }

  private func drain() async {
    while true {
      let attempt = state.withValue { state -> (generation: Int, revision: Int)? in
        guard !state.pending.isEmpty else {
          state.worker = nil
          return nil
        }
        return (state.generation, state.revision)
      }
      guard let attempt else { return }
      let didTake = LockIsolated(false)

      await persist { [state] in
        didTake.withValue { $0 = true }
        return state.withValue { state in
          guard state.generation == attempt.generation else { return [:] }
          let values = state.pending
          state.pending.removeAll()
          return values
        }
      }

      if !didTake.withValue({ $0 }) {
        // A write can fail before it enters the writer closure. Keep the values for a
        // later event, but do not retry unchanged work in a loop after an error.
        let shouldContinue = state.withValue { state in
          if state.revision != attempt.revision && !state.pending.isEmpty {
            return true
          }
          state.worker = nil
          return false
        }
        if !shouldContinue { return }
      }
    }
  }
}
