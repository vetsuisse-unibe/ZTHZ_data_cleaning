### File-related functions

# Loads hen data file, converting timestamps to POSIX and setting up a key
# Parameter: path to file
# Output: a timeline
loadTimeline <- function(path) {
  # Load data
  data <- fread(path, sep=";", header=TRUE)
  # Sanity check that required columns are present
  stopifnot(all(c("Time", "Hen", "Zone") %in% colnames(data)))
  # Fast-convert string timestamps into POSIX
  data[, Time := lubridate::ymd_hms(Time)]
  # Make sure empty strings are converted to NA
  data[Zone == "", Zone := NA]
  # Set indexing key (also sorts)
  setkeyv(data, c("Time", "Hen"))
}

# Finds bin data given bin size and ID marker in the filename
# Parameters: folder path, bin size, OPTIONAL ID marker present in the filename
# Output: full path to a matching file or NULL if none found
findBinFile <- function(basepath, binSize, marker="") {
  pattern <- paste0("binval", binSize, "sec_.*", marker, ".*\\.csv$")
  filenames <- list.files(path = basepath, pattern = pattern, ignore.case=TRUE)
  if (length(filenames)) {
    return(file.path(basepath, filenames[1]))
  } else {
    return(NULL)
  }
}

### Data-filtering functions

# Filter a timeline to a single hen
# Parameters: a timeline and a hen ID
# Output: a single-hen timeline
filterHen <- function(data, henID) {
  data[Hen == henID]
}

# Extract a specific time period from a timeline
# Parameters: a single-hen timeline
# Output: a filtered timeline between start and end, with added records at start and end exactly
extractInterval <- function(henData, start, end) {
  stopifnot(start < end)
  prevRecord <- henData[findInterval(start, henData[,Time])]
  lastRecord <- henData[findInterval(end, henData[,Time])]
  inside <- henData[between(Time, start, end)]
  if (nrow(inside)) {
    if (nrow(prevRecord)) {
      # We have a record before start time - take it, but truncate to start time
      prevRecord[,Time := start]
    } else {
      # We don't have a record - duplicate first row with start time and NA zone
      prevRecord <- first(inside)
      prevRecord[,Time := start]
      prevRecord[,Zone := NA]
    }
    
    if (lastRecord$Time == end) {
      return(rbind(prevRecord, inside, fill=TRUE))
    } else {
      lastRecord[,Time := end]
      return(rbind(prevRecord, inside, lastRecord, fill=TRUE))
    }
  } else {
    return(inside)
  }
}

### Data-wrangling functions

# Convert a timeline into an list of single-hen timelines
# Parameter: a timeline
# Output: a list of single-hen timelines
splitHenData <- function(data) {
  if (nrow(data)) {
    hens <- unique(data[, Hen])
    splitData <- list()
    for (henID in hens) {
      splitData <- append(splitData, list(filterHen(data, henID)))
    }
  } else { # Data is empty - return a list with one empty dataframe
    splitData <- list(data) 
  }
  return(splitData)
}

# Merge a list of timelines into a single timeline
# Parameter: a list of timelines
# Output: a single timeline
mergeHenData <- function(henDataList) {
  combined <- rbindlist(henDataList)
  setkeyv(combined, c("Time", "Hen"))
  setindex(combined, Hen)
  return(combined)
}

# Apply a function to a timeline hen-wise
# Parameters: a function to transform a single-hen timeline, a timeline
# Output: a single transformed timeline
applyPerHen <- function(f, data) {
  mergeHenData(Map(f, splitHenData(data)))
}

### Transition-related functions

isTransition <- function(vec) {
  # See https://www.rdocumentation.org/packages/data.table/versions/1.14.0/topics/rleid
  # TRUE for first element in groups of repeated elements, FALSE otherwise
  # Also FALSE for the first transition
  return(!duplicated(rleid(vec)) & rleid(vec) != 1)
} 

### Binning-related functions

# Transform time markers for end-of-bin 
# Parameters: binned timeline, and original bin size for the timeline
# Output: timeline with markers shifted from last to the first second of the bin
invertBins <- function(data, binSize) {
  data[, Time := (Time - as.difftime(binSize - 1, units="secs"))]
  return(data)
}

### Sparse-timeline related functions

# Remove consecutive Zone duplicates but also keep the last record
# Does not change original data
# Parameter: single-hen timeline
# Output: a sparse timeline
compactHenData <- function(henData) {
  # See https://www.rdocumentation.org/packages/data.table/versions/1.14.0/topics/rleid
  #   TRUE for first element in groups of repeated elements, FALSE otherwise
  # henData[, .I == .N] is a "is it the last row" vector
  henData[!duplicated(rleid(henData$Zone)) | henData[, .I == .N]]
}

# As compactHenData, but suitable for multiple-hen timelines
# Parameter: a timeline
# Output: a sparse timeline
compactData <- function(data) {
  applyPerHen(compactHenData, data)
}

### Duration-related functions

# Add a Duration column to a timeline
# Calculated for each row as the time difference to next row; 0 for the last row
# Parameter: a single-hen timeline
# Output: a timeline with durations
addDurations <- function(henData) {
  newData <- copy(henData)
  # Calculate duration of entry as difference to next entry
  newData[, Duration := (shift(Time, type="lead") - Time)]
  # Set duration for last entry as 0
  newData[nrow(newData), Duration := as.difftime(0, units="secs")]
  return(data.table(newData))
}

# Return a table of zone / cumulative duration for the zone in a timeline
# Parameters: timeline data, and OPTIONAL list of zones (to display 0 in missing ones)
sumDurations <- function(data, zones) {
  # Add durations
  dataD <- applyPerHen(addDurations, data)
  # Sum durations by zone
  sums <- dataD[, list(TotalDuration=sum(Duration)), by=c("Zone")]
  if (!missing(zones)) {
    zonesDT <- data.table(Zone = zones)
    # Replace zone list with "zones" argument
    sums <- sums[zonesDT, on="Zone"]
    # Fill in NA for missing zones
    sums[is.na(TotalDuration), TotalDuration := as.difftime(0, units="secs")]
  }
  return(sums)
}

### Dual timeline related functions

# Merge two single-hen timelines into one dual timeline
# Parameters: two timelines
# Output: a single timeline with time markers from both
#         Zones for both timelines are present as Zone.A and Zone.B
#         Missing Zone records are filled as locf
dualTimeline <- function(dataA, dataB) {
  # Outer join two timelines on timestamps and hens
  merged <- merge(
    dataA, dataB,
    by = c("Time", "Hen"), all=TRUE, suffixes=c(".A", ".B")
  )
  # Carry zones forward
  merged[, Zone.A := zoo::na.locf(Zone.A, na.rm = FALSE), by=Hen]
  merged[, Zone.B := zoo::na.locf(Zone.B, na.rm = FALSE), by=Hen]
  # Merge PackID if present
  if ("PackID.A" %in% colnames(merged)) {
    merged[, PackID := PackID.A]
    merged[is.na(PackID), PackID := PackID.B]
    merged[, `:=`(PackID.A=NULL, PackID.B=NULL)]
  }
  return(merged)
}

# Loads hen data file, converting timestamps to POSIX and setting up a key
# Parameter: path to file
# Output: a dual timeline
loadDualTimeline <- function(path) {
  # Load data
  data <- fread(path, sep=";", header=TRUE)
  # Sanity check that required columns are present
  stopifnot(all(c("Time", "Hen", "Zone.A", "Zone.B") %in% colnames(data)))
  # Fast-convert string timestamps into POSIX
  data[, Time := lubridate::ymd_hms(Time)]
  # Make sure empty strings are converted to NA
  data[Zone.A == "", Zone.A := NA]
  data[Zone.B == "", Zone.B := NA]
  # Set indexing key (also sorts)
  setkeyv(data, c("Time", "Hen"))
}
