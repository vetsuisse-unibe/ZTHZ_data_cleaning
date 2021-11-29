# Code assumes working directory is set to where this file is

# Main data structure
library("data.table")

# Load common functions
source("../functions.R")

# Takes a timeline and filters out intervals smaller than 
filterTimeline <- function(data, filterSize) {
  filterHenTimeline <- function(henData) {
    # Make sure the hen data is compact / timeline is sparse, i.e. every event is a proper transition
    henData <- compactHenData(henData)
    # Add durations to all intervals
    henData <- addDurations(henData)
    # Keep first data point for any hen, otherwise filter out any interval with a duration less than the filter size
    filtered <- henData[(Duration >= filterSize) | henData[, .I == 1]]
    # Re-compact the timeline
    filtered <- compactHenData(filtered)
    # Clear out (now wrong) duration data
    filtered[, Duration := NULL]
    return(filtered)
  }
  
  # Apply the above per hen since durations would be wrong otherwise
  return(applyPerHen(filterHenTimeline, data))
}

# Load the preprocessed data
combinedData <- loadTimeline(
  file.path(getwd(), "..", "..", "SampleData", "SamplePreprocessedData", "preprocessed_sample_combined.csv")
)

# Filtering out records shorter than 30 seconds
filterSize <- 30

# Filter the raw timeline
filteredData <- filterTimeline(combinedData, filterSize)
fwrite(filteredData, file=file.path(
  getwd(), "..", "..", "SampleData", "SampleFilteredData", "filtered_30s_sample_combined.csv"
), sep=";")
