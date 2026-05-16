import SwiftLevelDBBenchCore

let options = try SwiftLevelDBBenchmark.parse(arguments: Array(CommandLine.arguments.dropFirst()))
let results = try await SwiftLevelDBBenchmark.run(options: options)
print(try SwiftLevelDBBenchmark.render(results, json: options.json))
