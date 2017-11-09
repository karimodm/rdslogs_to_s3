const AWS = require('aws-sdk')

const S3BCUKET=''
const S3PREFIX=''
const RDSINSANCE=[''] // This is a list
const LOGNAME=''
const LASTRECIEVED='lastWrittenMarker'
const REGION=''

let logFileData = ""
let S3BucketName = S3BCUKET
let S3BucketPrefix = S3PREFIX
let RDSInstanceNames = RDSINSANCE
let logNamePrefix = LOGNAME
let region = REGION

function downloadRDSLogFile(RDSclient, RDSInstanceName, LogFileName, Marker) {
  if (typeof(Marker) === 'undefined') Marker = '0'
  return RDSclient.downloadDBLogFilePortion({ DBInstanceIdentifier: RDSInstanceName, LogFileName, Marker }).promise()
    .then(logFile => {
      if (logFile.AdditionalDataPending) {
        console.log(`[${RDSInstanceName}] ${LogFileName} Partial data retrieved...`)
        return downloadRDSLogFile(RDSclient, RDSInstanceName, LogFileName, logFile.Marker)
          .then(_logFile => logFile.LogFileData + _logFile.logFileData)
      }
      console.log(`[${RDSInstanceName}] ${LogFileName} retrieved all data.`)
      return logFile.LogFileData
    })
}

exports.handler = function (event, context) {
  Promise.all(RDSInstanceNames.map(
    function (RDSInstanceName) {
      const RDSclient = new AWS.RDS({ region })
      const S3client  = new AWS.S3({ region })

      console.log(`Processing database ${RDSInstanceName}`)
      const lastRecievedFile = `${S3BucketPrefix}-${RDSInstanceName}-${LASTRECIEVED}`

      return RDSclient.describeDBLogFiles({ DBInstanceIdentifier: RDSInstanceName, FilenameContains: logNamePrefix}).promise()
        .then(dbLogs => {
          let lastWrittenTime = 0
          let lastWrittenThisRun = 0
          let firstRun

          return S3client.getObject({ Bucket: S3BucketName, Key: lastRecievedFile }).promise()
            .then(S3response => {
              firstRun = false
              return S3response
            })
            .catch(err => {
              if (err.statusCode === 404) {
                console.log(`[${RDSInstanceName}] It appears this is the first log import, all files will be retrieved from RDS`)
                firstRun = true
                return null
              } else
                throw err
            })
            .then(S3response => {
              if (firstRun === false)  {
                lastWrittenTime = parseInt(S3response.Body)
                console.log(`[${RDSInstanceName}] Found marker from last log download, retrieving log files with lastWritten time after ${lastWrittenTime}`)
              }
              return Promise.all(dbLogs.DescribeDBLogFiles.map(
                function (dbLog) {
                  if (dbLog.LastWritten > lastWrittenThisRun || firstRun) {
                    console.log(`[${RDSInstanceName}] Downloading log file: ${dbLog.LogFileName} found and with LastWritten value of: ${dbLog.LastWritten}`)
                    if (dbLog.LastWritten > lastWrittenThisRun)
                      lastWrittenThisRun = dbLog.LastWritten
                    return downloadRDSLogFile(RDSclient, RDSInstanceName, dbLog.LogFileName)
                      .then(logFileData => {
                        const objectName = `${S3BucketPrefix}-${RDSInstanceName}-${dbLog.LogFileName}-${(new Date).getTime()}`
                        return S3client.putObject({ Bucket: S3BucketName, Key: objectName, Body: logFileData }).promise()
                          .then(() => console.log(`[${RDSInstanceName}] Written log file ${objectName} to S3 bucket ${S3BucketName}`))
                      })
                  }
                }
              )).then(() =>
                S3client.putObject({ Bucket: S3BucketName, Key: lastRecievedFile, Body: lastWrittenThisRun.toString() }).promise()
                  .then(() => console.log(`[${RDSInstanceName}] Wrote new Last Written Market to ${lastRecievedFile}, in Bucket ${S3BucketName}`))
              )
            })
        })
    }
  )).then(() => console.log("Log file export complete"))
}
