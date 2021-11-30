# Hen tracking data cleaning

## Sample Data

In `SampleData`:

* `SampleData/SampleRawData` is a sample of data from the measurement platform.
* `SampleData/SampleMetadata` contains a file describing the mapping of tracking tags to hens.
* `SampleData/SamplePreprocessedData` contains a file that's a result of combining the raw data with the metadata, in the "timeline" format.
* `SampleData/SampleFilteredData` contains the output of runnning the filtering methods on the preprocessed ata, in the "sparse timeline" format.

## Methods

**Note:** All R files assume that the working directory is set to the folder they are ran from.

In `Methods`:

* `Methods/functions.R` contains shared functions that are reused by other code in the project.
* `Methods/Preprocess.R` combines the raw data and the mapping metadata into a single (unfiltered) timeline.
* `Methods/Filtering` contains code that applies the simple filtering by interval length.
* `Methods/SlidingBin` contains code that applies a sliding window algorithm to smooth out rapid changes.
* `Methods/Binning` contains code that applies a binning algorthm to replace the timeline with dominant value in each fixed-size bin.
    * This is actually done by sampling the output of the `SlidingBin` code - so it needs to be ran first to do fixed binning.

## Required R version and libraries

Code has been tested with R 4.0.5

Library dependencies:
* `data.table` (tested with 1.14.0)
* `lubridate` (tested with 1.7.10)
* `stringr` (tested with 1.4.0)
* `plyr` (tested with 1.8.6)
