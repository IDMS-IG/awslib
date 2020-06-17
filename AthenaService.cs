using System;
using System.Collections.Generic;
using System.Text;
using Amazon.Athena;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Amazon.S3.Util;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Amazon;
using Amazon.Athena.Model;

namespace awslib
{
    public class AthenaService
    {
        private static readonly RegionEndpoint bucketRegion = RegionEndpoint.USEast1;
        private static IAmazonS3 s3Client;
        public static int CLIENT_EXECUTION_TIMEOUT = 100000;
        public static string ATHENA_OUTPUT_BUCKET = "s3://my-athena-bucket";
        // This example demonstrates how to query a table created by the "Getting Started" tutorial in Athena
        public static long SLEEP_AMOUNT_IN_MS = 1000;
        public static string ATHENA_DEFAULT_DATABASE = "default";

        public void ExecuteQuery(string sql)
        {
            var awsCredentials = new Amazon.Runtime.BasicAWSCredentials("A", "B");
            var c = new AmazonAthenaClient(awsCredentials);

            QueryExecutionContext queryExecutionContext = new QueryExecutionContext();
            queryExecutionContext.Database = ATHENA_DEFAULT_DATABASE;

            // The result configuration specifies where the results of the query should go in S3 and encryption options
            ResultConfiguration resultConfiguration = new ResultConfiguration();
            // You can provide encryption options for the output that is written.
            // .withEncryptionConfiguration(encryptionConfiguration)
            resultConfiguration.OutputLocation = ATHENA_OUTPUT_BUCKET;

            // Create the StartQueryExecutionRequest to send to Athena which will start the query.
            StartQueryExecutionRequest startQueryExecutionRequest = new StartQueryExecutionRequest();
            startQueryExecutionRequest.QueryString = sql;
            startQueryExecutionRequest.QueryExecutionContext = queryExecutionContext;
            startQueryExecutionRequest.ResultConfiguration = resultConfiguration;

            var startQueryExecutionResponse = c.StartQueryExecutionAsync(startQueryExecutionRequest);
            //Console.WriteLine($"Query ID {startQueryExecutionResponse.QueryExecutionId}");
//            return startQueryExecutionResponse.QueryExecutionId();
        }
    }
}
