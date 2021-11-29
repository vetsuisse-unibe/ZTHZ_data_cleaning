# Main data structure
library("data.table")

# Load common functions
source("functions.R")

# Text matching
library("stringr")
# Mapping vector values
library("plyr")

# Load the "full tag ID" file
fullTagIDPath <- file.path(getwd(), "..", "SampleData", "SampleMetadata", "full_tagID_long.csv")
fullTagIDData <- fread(fullTagIDPath, sep=";", header=TRUE)

# Convert string dates to timestamps, and reformat PackID and Hen
fullTagIDData[,`:=`(
  StartTs=lubridate::dmy(StartDate),
  EndTs=lubridate::dmy(EndDate),
  PackID=paste0(PenID, "_", BackPackID),
  Hen=paste0("Hen_", HenID)
)]

# Reused some code from Klara's code in (1)
# Convert the start/end date records into a new table with record per day

# Create an empty table with the required column types
fullIDs = data.table(
  HenID = character(),
  Pen = numeric(),
  TagID = character(),
  Date = as.Date(character()),
  BackPackID = character()
)

# Loop through tag data
for (i in 1:nrow(fullTagIDData)) {
  # Create a sequence of days from StartTs to EndTs (inclusive)
  henSeq = seq(fullTagIDData[i, StartTs], fullTagIDData[i, EndTs], by = "day")
  # We want to omit the last date in sequence, as EndTs is the next day after last
  nEntries = length(henSeq) - 1
  # Create a new table with the hen data repeating and the slice of the date sequence except for the last one
  newEntry = data.table(
    HenID = rep(fullTagIDData[i, HenID], nEntries), 
    Pen = rep(fullTagIDData[i, PenID], nEntries), 
    TagID = rep(fullTagIDData[i, TagID], nEntries),
    BackPackID = rep(fullTagIDData[i, BackPackID], nEntries),
    Date = henSeq[1:nEntries]
  )
  # Append the table to the main table
  fullIDs = rbind(fullIDs, newEntry)
}

# Mapping between zone names according to sensors and final zone names
zoneMapping = c(
  "Tier 1" = "Litter",
  "Tier 2" = "Tier_2",
  "Tier 3" = "Ramp_Nestbox",
  "Tier 4" = "Tier_4",
  "Wintergarten" = "Wintergarten"
)

# A function to convert sensor zone names to final zone names in two steps
# Input: a vector of raw zone names (stings)
# Output: a vector of final zone names
mapZones <- function(fullZones) {
  # Extract either "Tier N" or "Wintergarten" from the beginning of the strings
  # str_extract() comes from stringr
  zones <- str_extract(fullZones, "^(Tier \\d|Wintergarten)")
  # Apply zoneMapping to extracted strings
  # revalue() comes from plyr
  return(revalue(zones, zoneMapping))
}

loadSensorCompact <- function(inPath) {
  # Read data
  fullData <- fread(inPath, select=c(1, 3, 4, 7), sep=";")
  
  print(inPath)
  
  # Drop bogus data with NA in last column
  fullData <- fullData[!is.na(V7)]
  
  # Rename columns, drop original columns
  fullData <- fullData[,`:=`(
    Time=lubridate::dmy_hms(V1),
    Date=as.Date(lubridate::dmy_hms(V1)),
    Zone=mapZones(V4),
    TagID=as.character(V3),
    V1=NULL,
    V3=NULL,
    V4=NULL,
    V7=NULL
  )]
  
  # Merge with the fullIDs data based on TagID and date
  fullData <- merge(fullData, fullIDs, by = c('TagID', 'Date'), all.x = TRUE)
  # Drop unmatched values
  fullData <- fullData[!is.na(HenID)]
  
  # Reformat data from fullIDs into final format of Hen and PackID
  fullData <- fullData[, `:=`(
    Hen = paste0("Hen_", HenID),
    PackID = paste0(as.character(Pen), "_", BackPackID),
    Date = NULL,
    TagID = NULL,
    HenID = NULL,
    Pen = NULL,
    BackPackID = NULL
  )]
  
  # Order/index by Time and Hen
  setkeyv(fullData, c("Time", "Hen"))
  
  # Compact the data
  sparseData <- applyPerHen(compactHenData, fullData)
  
  return(sparseData)
}

# Base path to search for raw files
logBasePath <- file.path(getwd(), "..", "SampleData", "SampleRawData")

# List of all files in that path
filenames <- list.files(path = logBasePath, pattern = "\\.csv$", ignore.case=TRUE)

# Load data from each filename into a list of datatables
sensorData <- lapply(filenames, function(filename) {
  loadSensorCompact(file.path(logBasePath, filename))
})

# Merge datatables into a single table
combinedData <- mergeHenData(sensorData)
# Free up memory from individual tables
rm(sensorData)

fwrite(
  combinedData,
  file=file.path(getwd(), "..", "SampleData", "SamplePreprocessedData", "preprocessed_sample_combined.csv"),
  sep=";"
)