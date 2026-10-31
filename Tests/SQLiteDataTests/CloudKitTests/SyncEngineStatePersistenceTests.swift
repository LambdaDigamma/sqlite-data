#if canImport(CloudKit)
  import CloudKit
  import Foundation
  import SQLiteData
  import Testing

  extension BaseCloudKitTests {
    @MainActor
    final class SyncEngineStatePersistenceTests: BaseCloudKitTests, @unchecked Sendable {
      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func stateCallbacksReturnWhileTheWriterIsBlocked() async throws {
        defer { syncEngine.stop() }
        let checkpoint = try Self.checkpoint()
        let entered = AsyncStream<Void>.makeStream()
        let release = DispatchSemaphore(value: 0)
        let blockedWrite = Task {
          try await userDatabase.write { _ in
            entered.continuation.yield()
            release.wait()
          }
        }
        var iterator = entered.stream.makeAsyncIterator()
        await iterator.next()
        do {
          defer { release.signal() }
          await syncEngine.handleStateUpdate(
            stateSerialization: checkpoint, syncEngine: syncEngine.private)
          await syncEngine.handleStateUpdate(
            stateSerialization: checkpoint, syncEngine: syncEngine.shared)
          #expect(
            try Self.data(syncEngine.statePersistence.latestValue(for: .private))
              == Self.data(checkpoint))
          #expect(
            try Self.data(syncEngine.statePersistence.latestValue(for: .shared))
              == Self.data(checkpoint))
        }
        try await blockedWrite.value
        await syncEngine.statePersistence.flush()
        #expect(try storedData(for: .private) == Self.data(checkpoint))
        #expect(try storedData(for: .shared) == Self.data(checkpoint))
      }

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func stoppedEngineKeepsItsLatestAcceptedCheckpoint() async throws {
        defer { syncEngine.stop() }
        let checkpoint = try Self.checkpoint()
        let source = syncEngine.private
        syncEngine.stop()
        await syncEngine.handleStateUpdate(stateSerialization: checkpoint, syncEngine: source)
        #expect(
          try Self.data(syncEngine.statePersistence.latestValue(for: .private))
            == Self.data(checkpoint))
        try await syncEngine.start()
        #expect(
          try Self.data(syncEngine.statePersistence.latestValue(for: .private))
            == Self.data(checkpoint))
        await syncEngine.statePersistence.flush()
        #expect(try storedData(for: .private) == Self.data(checkpoint))
      }

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func restartRejectsCallbacksFromTheReplacedEngine() async throws {
        defer { syncEngine.stop() }
        let checkpoint = try Self.checkpoint()
        let oldSource = syncEngine.private
        syncEngine.stop()
        try await syncEngine.start()
        await syncEngine.handleStateUpdate(stateSerialization: checkpoint, syncEngine: oldSource)
        #expect(syncEngine.statePersistence.latestValue(for: .private) == nil)
        #expect(try storedData(for: .private) == nil)
        await syncEngine.handleStateUpdate(
          stateSerialization: checkpoint, syncEngine: syncEngine.private)
        await syncEngine.statePersistence.flush()
        #expect(try storedData(for: .private) == Self.data(checkpoint))
      }

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      @Test func accountResetClearsCheckpointAndRejectsOldCallbacks() async throws {
        defer { syncEngine.stop() }
        let checkpoint = try Self.checkpoint()
        let oldSource = syncEngine.private
        await syncEngine.handleStateUpdate(stateSerialization: checkpoint, syncEngine: oldSource)
        await syncEngine.statePersistence.flush()
        #expect(try storedData(for: .private) != nil)
        try await syncEngine.deleteLocalData()
        await syncEngine.handleStateUpdate(stateSerialization: checkpoint, syncEngine: oldSource)
        await syncEngine.statePersistence.flush()
        #expect(syncEngine.statePersistence.latestValue(for: .private) == nil)
        #expect(try storedData(for: .private) == nil)
      }

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      private func storedData(for scope: CKDatabase.Scope) throws -> Data? {
        try syncEngine.metadatabase.read { db in
          try Self.data(StateSerialization.find(#bind(scope)).select(\.data).fetchOne(db))
        }
      }

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      nonisolated private static func data(_ checkpoint: CKSyncEngine.State.Serialization?) throws
        -> Data?
      {
        try checkpoint.map { try JSONEncoder().encode($0) }
      }

      @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
      nonisolated private static func checkpoint() throws -> CKSyncEngine.State.Serialization {
        // A genuine one-record checkpoint captured with automatic network sync disabled.
        // The outer base64 is only a portable encoding of its JSON representation.
        let encoded = """
          eyJkYXRhIjoiWW5Cc2FYTjBNRERVQVFJREJBVUdCd3BZSkhabGNuTnBiMjVaSkdGeVkyaHBkbVZ5VkNSMGIzQllK
          RzlpYW1WamRITVNBQUdHb0Y4UUQwNVRTMlY1WldSQmNtTm9hWFpsY3RFSUNWUnliMjkwZ0FHdkVCb0xERGM3UVVO
          SVNreFFWbHhkWjJocGJYRjFkM2w3ZjRLSGpGVWtiblZzYk44UUZRME9EeEFSRWhNVUZSWVhHQmthR3h3ZEhoOGdJ
          U0lqSkNVbUppZ21LaXNzTFNZbUpqRXlNVFExTlY4UUVuQmxibVJwYm1kYWIyNWxRMmhoYm1kbGMxOFFFWEJsYm1S
          cGJtZEJjM05sZEZONWJtTnpXWHB2Ym1WVGRHRjBaVjhRR25ObGNuWmxja05vWVc1blpWUnZhMlZ1YzBKNVdtOXVa
          VWxFWHhBY2JHRnpkRVpsZEdOb1JHRjBZV0poYzJWRGFHRnVaMlZ6UkdGMFpWOFFISE5sY25abGNrTm9ZVzVuWlZS
          dmEyVnVSbTl5UkdGMFlXSmhjMlZXSkdOc1lYTnpYeEFVYkdGemRGQjFjMmhTWldObGFYWmxaRVJoZEdWZkVCSnBi
          a1pzYVdkb2RFRnpjMlYwVTNsdVkzTmZFQng2YjI1bFNVUnpUbVZsWkdsdVoxUnZSbVYwWTJoRGFHRnVaMlZ6WHhB
          YWNHVnVaR2x1WjFKbFkyOXlaRTF2WkdsbWFXTmhkR2x2Ym5OYWFXUmxiblJwWm1sbGNsdHNZWE4wUVdOamIzVnVk
          RjhRSG1WNGFYTjBhVzVuUkdGMFlXSmhjMlZUZFdKelkzSnBjSFJwYjI1SlJGOFFGV3hoYzNSTGJtOTNibFZ6WlhK
          U1pXTnZjbVJKUkY4UUcyNWxaV1J6Vkc5R1pYUmphRVJoZEdGaVlYTmxRMmhoYm1kbGMxOFFFMmx1Um14cFoyaDBX
          bTl1WlVOb1lXNW5aWE5mRUI5dVpXVmtjMVJ2VTJGMlpVUmhkR0ZpWVhObFUzVmljMk55YVhCMGFXOXVYeEFiYVc1
          R2JHbG5hSFJTWldOdmNtUk5iMlJwWm1sallYUnBiMjV6WHhBYmFHRnpTVzVHYkdsbmFIUlZiblJ5WVdOclpXUkRh
          R0Z1WjJWelh4QWFhR0Z6VUdWdVpHbHVaMVZ1ZEhKaFkydGxaRU5vWVc1blpYT0FCb0FUZ0JXQUY0QUFnQUNBR1lB
          QWdCU0FCSUFJZ0FLQUFJQUFnQUFKZ0FjSmdCSUlDTkk0RXprNlhFNVRMblYxYVdSaWVYUmxjMDhRRUF2bGlOZ3dk
          MDFwa1FRTmJtb3htNkdBQTlJOFBUNFwvV2lSamJHRnpjMjVoYldWWUpHTnNZWE56WlhOV1RsTlZWVWxFb2o1QVdF
          NVRUMkpxWldOMDBSTkNnQVhTUEQxRVJWOFFFMDVUVFhWMFlXSnNaVTl5WkdWeVpXUlRaWFNqUmtkQVh4QVRUbE5O
          ZFhSaFlteGxUM0prWlhKbFpGTmxkRnhPVTA5eVpHVnlaV1JUWlhUUkUwS0FCZEVUUW9BRjBoTk5RazliVGxNdWIy
          SnFaV04wTGpDQUJZQUowMUVUVWxOVVZWUjBlWEJsV0hKbFkyOXlaRWxFRUFHQUVZQUsweE5YV0ZsYVcxcFNaV052
          Y21ST1lXMWxWbHB2Ym1WSlJJQVFnQXVBREY4UUduTjViblJvWlhScFl5MWphR1ZqYTNCdmFXNTBPbTV2ZEdWejFW
          NWZZR0VUWWlaa1pXWmZFQkJrWVhSaFltRnpaVk5qYjNCbFMyVjVYeEFSWVc1dmJubHRiM1Z6UTB0VmMyVnlTVVJa
          YjNkdVpYSk9ZVzFsV0ZwdmJtVk9ZVzFsRUFDQUFJQU9nQTJBRDE4UUUybHpiMnhoZEdWa0xXTm9aV05yY0c5cGJu
          UmZFQkJmWDJSbFptRjFiSFJQZDI1bGNsOWYwanc5YW10ZVEwdFNaV052Y21SYWIyNWxTVVNpYkVCZVEwdFNaV052
          Y21SYWIyNWxTVVRTUEQxdWIxcERTMUpsWTI5eVpFbEVvbkJBV2tOTFVtVmpiM0prU1VUU1BEMXljMThRSTBOTFUz
          bHVZMFZ1WjJsdVpWQmxibVJwYm1kU1pXTnZjbVJhYjI1bFEyaGhibWRsb25SQVh4QWpRMHRUZVc1alJXNW5hVzVs
          VUdWdVpHbHVaMUpsWTI5eVpGcHZibVZEYUdGdVoyWFJFMEtBQmRFVFFvQUYwUk5DZ0FYU2ZCTjlmbHBPVXk1dllt
          cGxZM1J6b0lBVzBqdzlnSUZYVGxOQmNuSmhlYUtBUU5PRGZCT0VoWVpYVGxNdWEyVjVjNkNnZ0JqU1BEMklpVjhR
          RTA1VFRYVjBZV0pzWlVScFkzUnBiMjVoY25tamlvdEFYeEFUVGxOTmRYUmhZbXhsUkdsamRHbHZibUZ5ZVZ4T1Uw
          UnBZM1JwYjI1aGNublNQRDJOamw4UUVVTkxVM2x1WTBWdVoybHVaVk4wWVhSbG9vOUFYeEFSUTB0VGVXNWpSVzVu
          YVc1bFUzUmhkR1VBQ0FBUkFCb0FKQUFwQURJQU53QkpBRXdBVVFCVEFIQUFkZ0NqQUxnQXpBRFdBUE1CRWdFeEFU
          Z0JUd0ZrQVlNQm9BR3JBYmNCMkFId0FnNENKQUpHQW1RQ2dnS2ZBcUVDb3dLbEFxY0NxUUtyQXEwQ3J3S3hBck1D
          dFFLM0Fya0N1d0s5QXI0Q3dBTEJBc01DeEFMRkFzb0Mxd0xxQXV3QzhRTDhBd1VEREFNUEF4Z0RHd01kQXlJRE9B
          TThBMUlEWHdOaUEyUURad05wQTI0RGVnTjhBMzREaFFPS0E1TURsUU9YQTVrRG9BT3JBN0lEdEFPMkE3Z0QxUVBn
          QVwvTUVCd1FSQkJvRUhBUWVCQ0FFSWdRa0JEb0VUUVJTQkdFRVpBUnpCSGdFZ3dTR0JKRUVsZ1M4Qkw4RTVRVG9C
          T29FN1FUdkJQSUU5QVQ1QlFRRkJRVUhCUXdGRkFVWEJSNEZKZ1VuQlNnRktnVXZCVVVGU1FWZkJXd0ZjUVdGQlln
          QUFBQUFBQUFDQVFBQUFBQUFBQUNRQUFBQUFBQUFBQUFBQUFBQUFBQUZuQT09In0=
          """
        let data = try #require(Data(base64Encoded: encoded, options: .ignoreUnknownCharacters))
        return try JSONDecoder().decode(CKSyncEngine.State.Serialization.self, from: data)
      }
    }
  }
#endif
