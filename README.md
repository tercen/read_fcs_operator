# Read FCS operator

##### Description

`read_fcs` operator transforms FCS files to Tercen datasets.

##### Usage

Data input|.
---|---
`documentId`        | is the documentId (document can be a single FCS file, or a zipped set of FCS files)

Operator settings|.
---|---
`which.lines`        | numeric, indicates the number of lines to be read. If NULL all the records are read, otherwise, a random sample of the size indicated by which.lines is read in

Data output|.
---|---
`filename`          | character, the name of the FCS file
`channels`          | numeric, one variable per channel in the FCS file
`Compensation`      | If built-in compensation matrices are found in the FCS files, they will be added to the "Compensation" folder within the project. 
`Annotation`        | The table containing channel names and descriptions per file is created in the "Annotation" folder within the project.

##### Details

The operator transforms FCS files into Tercen table. If the document is a ZIP file containing a set of FCS files, the operator extracts the FCS files and transforms them into Tercen table.

The Flow Cytometry Standard is a data file standard for the reading and writing of data from flow cytometry experiments.
