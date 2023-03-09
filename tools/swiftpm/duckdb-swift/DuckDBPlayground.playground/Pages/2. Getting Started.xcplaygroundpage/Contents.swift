/*:
 [Home](1.%20Welcome)
 # Getting Started
 
 ## Introduction
 
 In this first playground, we'll show you how you can use DuckDB with SwiftUI
 to view a table of data. The task is simple, but it will demonstrate many of
 the steps you'll need to interact successfully with DuckDB.
 
 > For this playground to work, it's important to access it via the
 > `DuckDB.xcworkspace` file otherwise Xcode Playgrounds will be unable to find
 > the DuckDB Swift module.
 
 You'll see how to:
 - Create an in-memory database
 - Create a connection to that database
 - Query the database through the connection
 
 We'll also be showing how DuckDB can be used to not only query internal tables,
 but also any CSV or JSON files that you may have downloaded to your file system.
 
 So let's get started.
 */

PlaygroundPage.current.needsIndefiniteExecution = true

// First, we need to import the DuckDB modules plus any other modules we'll be
// using within this playground

import DuckDB
import PlaygroundSupport
import SwiftUI

/*:
 ## Step 1: Fetching Data
 
 Before we can query data, we'll need some data to query.
 
 In this example, and for the other playground pages in this series, we're using
 the [Stack Overflow Annual Developer Survey](https://insights.stackoverflow.com/survey).
 In the 2022 survey there were over 70,000 responses from 180 countries and
 each and every response has been collected into a CSV for us to analyze.
 
 Handy. But what's in it?
 
 That seems like a great first task. Let's create a simple query that allows us
 to see all the columns in the table – plus they're types – so we know what
 we're working with. We'll use SwiftUI to visualize our results in a simple
 table.
 */

// Now the real-stuff. we'll define an asynchronous function that performs
// the heavy-lifting in extracting our required data

func fetchData() async throws -> ResultSet {
  
  // Here, we download the CSVs for the Stack Overflow survey. Subsequent calls
  // to this method return a cached version you can view at
  // `~/Documents/Shared Playground Data/org.duckdb/surveys`
  let fileURL = try await SurveyLoader.downloadSurveyCSV(forYear: 2022)
  
  // We'll use an in-memory store for our database, as we don't need to persist
  // anything
  let database = try Database(store: .inMemory)
  
  // Next we create a connection. A connection is used to issue queries to the
  // database. You can have multiple connections per database.
  let connection = try database.connect()
  
  // We're now ready to issue our first query! Notice how we enclose the file
  // path for the CSV in single quotes. We'll limit this to zero as we're only
  // interested in the column layout for now.
  return try connection.query("SELECT * FROM '\(fileURL.path)' LIMIT 0")
}

/*:
 
 ## Step 2: Rendering Data
 
 With our asynchronous data fetching function in hand, we can move on to
 displaying the data.
 
 We'll define a view with a single `rows` property initially set to `nil`. Then,
 when the view loads we'll call the function we defined in Step 1 to load the
 data asynchronously.
 
 SwiftUI's `task(_:)` modifier is perfect for this. The `task(_:)` modifier runs
 in the background when the view is loaded. We'll attach the `task(_:)`
 modifier to our loading spinner so we can see that something is happening.
 
 Finally, we'll process the result into rows for our view table and set the
 `rows` property. This will act as SwiftUI's trigger to re-render the view.
 */

struct TableLayoutView: View {
  
  // We'll define a new type to hold the view data for our table view
  struct TableRow: Identifiable {
    let id: String
    let databaseType: String
  }
  
  // This will hold our view state and signal to the view whether or not it
  // can show our gathered data
  enum ViewState {
    case loading
    case result(ResultSet)
  }
  
  @State var state = ViewState.loading
  
  var body: some View {
    switch state {
    
    // Whilst we're waiting for our data to load we'll display a spinner
    case .loading:
      ProgressView { Text("loading") }
        .task {
          do {
            // Here, we kick-off the function we defined in step 1
            self.state = .result(try await fetchData())
          }
          catch { print("error: \(error)") }
        }
      
    // Now our result set has been loaded we can display its data
    case .result(let result):
      Table(result) {
        TableColumn("Column Name", value: \.name)
        TableColumn("Column Type", value: \.typeName)
      }
    }
  }
}

// Plus a small extension on Column to help us get the column type as a String
fileprivate extension Column {
  var typeName: String {
    String(describing: underlyingDatabaseType)
  }
}

/*:
 
 ## Conclusion
 
 And that's how easy it is to get started with DuckDB.
 
 You've learnt how to create a database, connect to it, and issue queries.
 
 Next-up, we'll see how we can use DuckDB to create incredible visulizations
 using SwiftUI, TabularData and SwiftCharts. See you there.
 
 [Next Playground](@next)
 */

// Kick-off the playground
PlaygroundPage.current.setLiveView(
  TableLayoutView()
    .frame(width: 640, height: 480, alignment: .center)
)
