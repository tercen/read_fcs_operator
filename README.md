# Read FCS operator

##### Description

`read_fcs` operator transforms FCS files to Tercen datasets.

##### Usage

Input projection|.
---|---
`documentId`        | is the documentId (document can be a single FCS file, or a zipped set of FCS files)

Input parameters|.
---|---
`which.lines`        | numeric, indicates the number of lines to be read. If NULL all the records are read, otherwise, a random sample of the size indicated by which.lines is read in
`use.builtin.compensation`        | A logical value indicating whether the built-in compensation matrix should be use (Set to false if you do not want the application of the built-in compensation or if you want to use an external matrix)
`use.custom.compensation`        | A logical value indicating whether the custom compensation matrix should be use (Set to false if you do not want the application of the compensation or if you want to use an internal matrix).
`use.custom.compensation`        | Type of the separator between values (if custom compensation matrix is provided)

Output relations|.
---|---
`filename`          | character, the name of the FCS file
`channels`          | numeric, one variable per channel in the FCS file

##### Details

The operator transforms FCS files into Tercen table. If the document is a ZIP file containing a set of FCS files, the operator extracts the FCS files and transforms them into Tercen table.

The Flow Cytometry Standard is a data file standard for the reading and writing of data from flow cytometry experiments.
