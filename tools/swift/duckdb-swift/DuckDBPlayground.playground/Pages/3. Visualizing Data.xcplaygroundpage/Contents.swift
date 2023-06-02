/*:
 [Home](1.%20Welcome)
 # Visualizing Data
 
 ## Introduction
 
 In this playground, we build on the skills learnt in
 [Getting Started](2.%20Getting%20Started) and begin to extract some
 meaningful data from our data source.
 
 > For this playground to work, it's important to access it via the
 > `DuckDB.xcworkspace` file otherwise Xcode Playgrounds will be unable to find
 > the DuckDB Swift module.
 
 You'll see how to:
 - Cast DuckDB columns to native Swift types
 - Visualize extracted data using Swift Charts
 */

PlaygroundPage.current.needsIndefiniteExecution = true

// Import the DuckDB modules plus any other modules we'll be using within
// this playground

import DuckDB
import PlaygroundSupport
import SwiftUI
import Charts

/*:
 ## Step 1: Extracting Data
 
 Thanks to our work in the [Getting Started](2.%20Getting%20Started) playground,
 we can start now start to do some meaningful analysis on our data.
 
 In this example we're going to take a look at which countries generated the
 largest response in the
 [Stack Overflow Annual Developer Survey](https://insights.stackoverflow.com/survey)
 and plot the results into a chart.
 
 Let's create a function that extracts the top 10 countries participating in
 the survey and puts it into a format ready for visualization.
 
 ### Casting columns
 
 To get the data from our columns out, we'll use the `cast(to:)` method on
 Column. It's important that you only cast to a column that matches the
 `underlyingDataType` of the Column you are casting from, or you'll get an
 assert in debug builds and nil values in release mode.
 
 Its possible to debug the column layout – and therefore column types – of any
 ResultSet object by simply printing it to the console, or checking the
 `underlyingDataType` member of the Column.
 
 Once you know this, you can then select the appropriate Swift type to cast to.
 For example, a Column with a `underlyingDatabaseType` of `DatabaseType.varchar`
 casts to `String`, and a column with an underlying database type of
 `DatabaseType.bigint` casts to `Int32` – and for convenience `Int`. For DuckDB
 composite types you can even cast to matching types which conform to
 `Decodable`.
 
 You can see which database types convert to which Swift types by checking out
 the docs for `DatabaseType`.
 
 Let's see this in practice.
 */

// We'll use this type as a source for our chart
struct ChartRow {
  let nameOfCountry: String
  let participantCount: Int
}

// It needs to conform to identifiable so that SwiftUI can group it correctly
extension ChartRow: Identifiable {
  var id: String { nameOfCountry }
}

// Here, we'll define an asynchronous function for extracting our data.
func generateSurveyParticipantCountryChartData() async throws -> [ChartRow] {
  
  // We'll connect to the database in the exact same way as the previous
  // playground
  let fileURL = try await SurveyLoader.downloadSurveyCSV(forYear: 2022)
  let database = try Database(store: .inMemory)
  let connection = try database.connect()
  
  // For our query, we'll count and group by the Country column in our CSV
  let result = try connection.query("""
    SELECT Country, COUNT(Country) AS Count
      FROM '\(fileURL.path)'
      GROUP BY Country
      ORDER BY Count DESC
      LIMIT 10
    """)
  
  // We know our Country column has VARCHAR as its underlying data type and we
  // know BIGINT is the underlying data type for the Count column. Let's cast
  // them appropriately.
  let countryColumn = result[0].cast(to: String.self)
  let countColumn = result[1].cast(to: Int.self)
  
  // Now that our columns have been cast, we can pull out the values. As
  // Column conforms to Collection, this is fairly simple. However, we do need
  // to guard against nil values and replace with a suitable default
  let countries = countryColumn.map { $0 ?? "unknown" }
  let counts = countColumn.map { $0 ?? 0 }
  
  // Finally, we can zip up the results and generate our chart rows
  var rows = [ChartRow]()
  for (country, count) in zip(countries, counts) {
    let row = ChartRow(nameOfCountry: country, participantCount: count)
    rows.append(row)
  }
  return rows
}

/*:
 
 ## Step 2: Making Charts
 
 Now that we have our data extraction routine ready to go, we can look into
 visualizing it into something that's great to look at and easy to understand.
 Thankfully, the combination of DuckDB, SwiftUI and Swift Charts make this
 process really easy.
 
 As in the previous playground we'll create a SwiftUI view to display our chart
 that displays a spinner while the data is assembled using our routine from step 1.
 Then, when our data is ready, we'll update our view and use Swift Charts to
 visualize what we've gathered.
 
 Let's take a look.
 */

struct PopularCountriesView: View {
  
  enum ViewState {
    case loading
    case data([ChartRow])
  }
  
  @State private var state = ViewState.loading
  
  var body: some View {
    switch state {
      
    // If we're still loading, we'll display a spinner and kick-off our
    // routine from step 1
    case .loading:
      ProgressView { Text("Fetching Data") }
        .task {
          do {
            let chartData = try await generateSurveyParticipantCountryChartData()
            self.state = .data(chartData)
          }
          catch { print("unexpected error: \(error)") }
        }
      
    // Now we have our chart data we can display the chart
    case .data(let chartRows):
      Chart(chartRows) { row in
        BarMark(
          x: .value("Respondents", row.participantCount),
          y: .value("Country", row.nameOfCountry)
        )
      }
    }
  }
}

/*:
 
 ## Conclusion
 
 In just a few lines of code DuckDB has enabled you to:
 
 - Open a CSV file withing DuckDB
 - Analyze it using SQL
 - Prepare it for display using Swift Charts.
 
 You've also learnt how you to cast from DuckDB's underlying database types and
 into native Swift types using Column's `cast(to:)` methods.
 
 Now you're well on your way to becoming a DuckDB master!
 */

PlaygroundPage.current.setLiveView(
  PopularCountriesView()
    .frame(width: 640, height: 480, alignment: .center)
)
