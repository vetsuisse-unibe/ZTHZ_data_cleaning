# Code assumes working directory is set to where this file is

# Main data structure
library("data.table")

# Load common functions
source("../functions.R")

slidingToBinned <- function(data, binSize) {
  stbFilter <- function(henData) {
    startTime <- first(henData)[, Time]
    endTime <- last(henData)[, Time]
    
    outData <- copy(henData)
    outIndex <- 1
    
    # Middle of first bin
    time <- startTime - as.numeric(startTime, unit="secs") %% binSize + floor(binSize / 2)
    lastZone <- first(henData)[, Zone]
    
    outData[1, `:=`(Time=startTime, Zone=lastZone)]
    outIndex <- 2
    
    for (x in 1:(nrow(henData) - 1)) {
      currZone <- henData[x, Zone]
      nextTime <- henData[x+1, Time]
      
      if (time + binSize >= nextTime) { 
        # cat("Skipped to", as.character(nextTime), "\n")
        next;
      }
      
      if (currZone != lastZone) {
        # cat(
        #   as.character(time + binSize - floor(binSize / 2)),
        #   "sampled at", as.character(time + binSize),
        #   currZone, "\n"
        # )
        outData[outIndex, `:=`(Time=time + binSize - floor(binSize / 2), Zone=currZone)]
        outIndex <- outIndex + 1
        lastZone <- currZone
      }
      
      # Jump forward the maximum number of binSize-length chunks to nextTime, not inclusive
      time <- time + binSize * ((as.numeric(nextTime) - as.numeric(time) - 1) %/% binSize)
      # cat("Jumped to", as.character(time), "\n")
    }
    
    # cat(as.character(endTime), last(sample)[, Zone], "\n")
    outData[outIndex, `:=`(Time=endTime, Zone=last(henData)[, Zone])]
    outData <- outData[1:outIndex]
    
    return(outData)
  }
  
  return(applyPerHen(stbFilter, data))
}

###

findSlidingFile <- function(basepath, binSize, marker="") {
  pattern <- paste0("sliding_", binSize, "s_.*", marker, ".*\\.csv$")
  filenames <- list.files(path = basepath, pattern = pattern, ignore.case=TRUE)
  if (length(filenames)) {
    return(file.path(basepath, filenames[1]))
  } else {
    return(NULL)
  }
}

loadSlidingFile <- function(binSize) {
  binPath <- findSlidingFile(
    file.path(getwd(), "..", "..", "SampleData", "SampleFilteredData"),
  binSize, "combined")
  stopifnot(!is.null(binPath))
  data <- loadTimeline(binPath)
  return(data)
}

# Sizes for sliding windows
# binSizes = c(
#   1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
#   15, 20, 25, 30, 35, 40, 45, 50, 55, 60,
#   65, 70, 75, 80, 85, 90, 95, 100, 105, 110, 115, 120,
#   130, 140, 150, 160, 170, 180, 190, 200, 210, 220,
#   230, 240, 250, 260, 270, 280, 290, 300,
#   600, 1200
# )
binSizes <- c(30)

start <- proc.time()

for (binSize in binSizes) {
  filterStart <- proc.time()
  cat("Building merged timeline for bin size", binSize, "... ")
  
  # Load sliding window data for bin size
  slidingData <- loadSlidingFile(binSize)
  
  # Build combined timeline for bin size
  binnedData <- slidingToBinned(slidingData, binSize)
  
  filename <- paste0("bins_", binSize, "s_sample_combined.csv")
  fwrite(binnedData, file=file.path(getwd(), "..", "..", "SampleData", "SampleFilteredData", filename), sep=";")
  
  cat("processed in", timetaken(filterStart), "\n")
}
cat("Total processing time: ", timetaken(start), "\n")
