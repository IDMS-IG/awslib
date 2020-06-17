using System;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Amazon.S3.Util;
using System.IO;
using System.Threading.Tasks;
using Amazon;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;


namespace awslib
{
    public class S3Client
    {
        private BasicAWSCredentials AWSCredentials = null;
        public AmazonS3Client s3Client = null;
        public string BucketName = "";
        public RegionEndpoint BucketRegion = RegionEndpoint.USEast1;
        private IConfigurationRoot configuration;

        public S3Client(IConfigurationRoot config, string source)
        {
            configuration = config;
            LoadConfiguration(source);
        }

        private void LoadConfiguration(string source)
        {
            var section = configuration.GetSection("API-KEYS");
            var key = section.GetSection(source)["KEY"];
            var secret = section.GetSection(source)["SECRET"];
            BucketName = section.GetSection(source)["BUCKET"];
            BucketRegion = RegionEndpoint.GetBySystemName(section.GetSection(source)["REGION"]);
            var creds = new Amazon.Runtime.BasicAWSCredentials(key, secret);
            s3Client = new AmazonS3Client(creds, BucketRegion);
        }
    }
}
