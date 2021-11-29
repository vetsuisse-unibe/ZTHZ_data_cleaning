# Code assumes working directory is set to where this file is

# Main data structure
library("data.table")

# Load common functions
source("../functions.R")

# Apply a sliding-window filter to a timeline
# Parameters: timeline and size of the sliding window
# Returns an almost-sparse timeline
slidingWindowTimeline <- function(data, slidingSize) {
  lastAddedTies <- 0
  otherTies <- 0
  
  # Apply a sliding-window filter to a single hen timeline
  # Parameters: timeline and size of the sliding window
  # Returns an almost-sparse timeline
  # 
  # Note: the value is calculating for the center of the sliding window
  # Note: first and last values are extrapolated to populate the window
  #  outside the timeline
  slidingWindowFilter <- function(henData) {
    # Nothing to filter if there aren't at least 2 records
    if (nrow(henData) < 2) {
      return(copy(henData))
    }
    
    # Get first record
    firstRecord <- first(henData)
    
    # Prepare a list of all zones
    zones <- unique(henData[, Zone])
    
    # Store sliding window buffer as a ring buffer:
    #  vector with a wrap-around index pointer
    slidingData <- vector(length = slidingSize)
    # Pre-populate the buffer with first record's zone
    for (i in 1:slidingSize) {
      slidingData[i] <- firstRecord[, Zone]
    }
    # Wrap-around pointer, 0-based index (for easier modulo)
    slidingIndex <- 0
    
    # Keep track of counts of zones in the buffer separately in a key-value list
    # Key: zone name, value: number of occurrences of the zone in the buffer
    slidingSums <- list()
    # Pre-populate sums with first record's zone
    for (zone in zones) {
      slidingSums[zone] <- 0
    }
    slidingSums[firstRecord[, Zone]] <- slidingSize
    
    # Implement adding an element to the ring buffer:
    # 1) Evict element under the pointer
    # 2) Replace it with the new element
    # 3) Advance pointer (wrapping around as needed)
    # 4) Adjust counts accordingly
    pushData <- function(zone) {
      # Record what zone we're replacing
      evicting <- slidingData[slidingIndex + 1]
      # Replace with new zone
      # Note: <<- required since we're changing a non-local variable
      slidingData[slidingIndex + 1] <<- zone
      
      # Increment then wrap around the pointer
      slidingIndex <<- (slidingIndex + 1) %% slidingSize
      
      # Reduce the count of the evicted zone (with sanity check)
      slidingSums[evicting] <<- slidingSums[[evicting]] - 1
      stopifnot(slidingSums[[evicting]] >= 0)
      # Increase the count of the added zone (with sanity check)
      slidingSums[zone] <<- slidingSums[[zone]] + 1
      stopifnot(slidingSums[[zone]] <= slidingSize)
    }
    
    # Get most represented zone in the sliding window buffer
    # Last added zone wins ties
    getMax <- function() {
      maxCount <- -1
      maxCandidates <- list()
      
      for (zone in zones) {
        # New best candidate found
        if (slidingSums[[zone]] > maxCount) {
          # Replace best candidate
          maxCount <- slidingSums[[zone]]
          maxCandidates <- list(zone)
        } else if (slidingSums[[zone]] == maxCount) {
          # Add to best candidates
          maxCandidates <- c(maxCandidates, zone)
        }
      }
      
      stopifnot(length(maxCandidates) > 0)
      
      if (length(maxCandidates) == 1) {
        return(maxCandidates[[1]])
      } else {
        # Resolve ties towards last added element
        for (shift in 1:slidingSize) {
          index <- ((slidingIndex - shift) %% slidingSize) + 1
          candidate <- slidingData[[index]]
          if (is.element(candidate, maxCandidates)) {
            if (shift == 1) {
              lastAddedTies <<- lastAddedTies + 1
            } else {
              otherTies <<- otherTies + 1
            }
            return(candidate)
          }
        }
      }
    }
    
    # Use a copy of the original data as the structure for the new timeline
    #  since the filtered timeline can't have more records
    outData <- copy(henData)
    # First record already corresponds to what we want - don't need to "add" it
    # Keep track of the last added index to outData
    outIndex <- 1
    
    # Set current time (= leading edge of the sliding window) and last added zone
    time <- firstRecord[, Time]
    lastAddedZone <- firstRecord[, Zone]
    
    # Loop through pairs (x, x+1) of timeline records
    for (x in 1:(nrow(henData) - 1)) {
      # We need current timeline record's zone and beginning of next record
      currZone <- henData[x, Zone]
      nextTime <- henData[x+1, Time]
      
      # Continue at most until we reach the next record
      while (time < nextTime) {
        # Push current record's zone into the sliding window buffer
        pushData(currZone)
        # Get most represented zone in the sliding window buffer
        # Last added zone wins ties
        zone <- getMax()
        
        # If zone flipped, record that as a transition in the new timeline
        if (zone != lastAddedZone) {
          # Transition time = center of the sliding window
          # This guarantees as little change to the timeline as possible
          windowCenter <- time - floor((slidingSize - 1)/2)
          # Advance record counter
          outIndex <- outIndex + 1
          # Add a record
          outData[outIndex, `:=`(Time=windowCenter, Zone=zone)]
          # Update last added zone
          lastAddedZone <- zone
        }
        
        # Advance time
        time <- time + 1
        
        # If the current interval has been going for long enough to overwrite
        #  the whole sliding window, skip to the next interval
        if (slidingSums[[zone]] == slidingSize) {
          time <- nextTime
        }
      }
    }
    
    # Last record remains to be processed
    finalZone <- henData[x+1, Zone]
    
    # If the final zone isn't the currently tracked one, it's guaranteed to flip
    #  (at latest at the last timestamp)
    while (finalZone != zone) {
      pushData(finalZone)
      zone <- getMax()
      
      if (zone != lastAddedZone) {
        windowCenter <- time - floor((slidingSize - 1)/2)
        outIndex <- outIndex + 1
        outData[outIndex, `:=`(Time=windowCenter, Zone=zone)]
        lastAddedZone <- zone
      }
      
      time <- time + 1
    }
    
    # At this point, nextTime is the last timestamp of the old timeline
    #  and windowCenter is the last timestamp added to the new timeline
    # Write a "bookend" event if they differ
    if (nextTime != windowCenter) {
      outIndex <- outIndex + 1
      outData[outIndex, `:=`(Time=nextTime, Zone=zone)]
    }
    
    # Truncate unused rows of the result data table
    outData <- outData[1:outIndex]
  }
  
  # Apply the above per hen since durations would be wrong otherwise
  result <- applyPerHen(slidingWindowFilter, data)
  
  cat("Ties stats for size", slidingSize, ": last added ties", lastAddedTies, ", other ties", otherTies, "\n")
  return(result)
}

###

# Load the preprocessed data
combinedData <- loadTimeline(
  file.path(getwd(), "..", "..", "SampleData", "SamplePreprocessedData", "preprocessed_sample_combined.csv")
)

# Sizes for sliding windows
# slidingSizes = c(
#   1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
#   15, 20, 25, 30, 35, 40, 45, 50, 55, 60,
#   65, 70, 75, 80, 85, 90, 95, 100, 105, 110, 115, 120,
#   130, 140, 150, 160, 170, 180, 190, 200, 210, 220,
#   230, 240, 250, 260, 270, 280, 290, 300,
#   600, 1200
# )
slidingSizes = c(30)

start <- proc.time()

for (slidingSize in slidingSizes) {
  filterStart <- proc.time()
  cat("Building merged timeline for filter size", slidingSize, "... ")
  # Build combined timeline for filter size
  filteredData <- slidingWindowTimeline(combinedData, slidingSize)
  
  filename <- paste0("sliding_", slidingSize, "s_sample_combined.csv")
  fwrite(filteredData, file=file.path(
    getwd(), "..", "..", "SampleData", "SampleFilteredData", filename
  ), sep=";")
  
  cat("processed in", timetaken(filterStart), "\n")
}
cat("Total processing time: ", timetaken(start), "\n")

