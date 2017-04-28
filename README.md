# gsnake
The name "gsnake" is an acronym for "gluttonous snake". It is a data processing
framework that helps you read and process logs from a variety of data format,
such as plain text file, gz text file, pcap data file.

# CHANGES
- support record last file read offset
- support dir pattern such as /tmp\*/dir
- create new dispatcher by conf
- auto select reader by file type
- fix some routine safe bugs