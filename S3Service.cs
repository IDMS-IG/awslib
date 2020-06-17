using System;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Amazon.S3.Util;
using System.IO;
using System.Threading.Tasks;
using Amazon;
using System.Linq;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using System.Net;
using Amazon.Runtime.Internal.Util;
using NLog;

namespace awslib
{
    public class S3Service
    {
        private AmazonS3Client client;
        private string bucketName;
        private RegionEndpoint region;
        private string tempFolder = @"c:\temp\";
        private IConfigurationRoot configuration;
        private NLog.Logger logger;

        public S3Service(AmazonS3Client S3client, string S3bucketName, RegionEndpoint S3region, IConfigurationRoot config, NLog.Logger Logger)
        {
            this.client = S3client;
            this.bucketName = S3bucketName;
            this.region = S3region;
            configuration = config;
            tempFolder = configuration["TEMP-FOLDER"];
            logger = Logger;
        }

        public async Task DeleteObjectNonVersionedBucketAsync(string fileName)
        {
            var deleteObjectRequest = new DeleteObjectRequest
            {
                BucketName = bucketName,
                Key = fileName
            };
            await client.DeleteObjectAsync(deleteObjectRequest);
            logger.Info($"Object {fileName} deleted.");
        }

        public void CopyFilesBetweenFolders(string[] args)
        {
            var sourceFile = args[2];
            var targetFile = Path.Combine(args[3], Path.GetFileName(sourceFile));
            CopyingObjectAsync(sourceFile, targetFile).Wait();
            logger.Info($"Object {sourceFile} was copied to {targetFile}.");
        }

        public void TransferFilesBetweenS3(string[] args)
        {
            var source = args[1];
            var target = args[2];

            var targetClient = new S3Client(configuration, target);

            var sourceFiles = args[3];
            var targetFolderName = args[4];

            var files = ListFiles(sourceFiles);
            logger.Info($"Total Objects to tranfer {files.Count}.");

            foreach (var f in files)
            {
                var fileName = f.Key;
                var downloadedFile = DownloadFile(fileName, tempFolder);
                UploadFile(targetClient, downloadedFile, targetFolderName);
                File.Delete(downloadedFile);
                logger.Info($"Object {downloadedFile} was transferred to {targetFolderName}.");
            }
        }

        public void DeleteFiles(string[] args)
        {
            if (args.Length != 3)
            {
                logger.Error($"Invalid # of arguments.");
                return;
            }
            else if (args[2].Trim().Length == 0)
            {
                logger.Error($"Must specify file name/mask to delete.");
                return;
            }

            try
            {
                var files = ListFiles(args[2]);
                logger.Info($"Total Files to delete:{files.Count}");
                foreach (var f in files)
                {
                    DeleteObjectNonVersionedBucketAsync(f.Key).Wait();
                }
            }
            catch (Exception ex)
            {
                logger.Error($"Error Deleting Files {ex}");
                throw ex;
            }
        }

        public void UploadFile(S3Client targetClient, string fileName, string TargetFolder)
        {
            var fileTransferUtility =
                new TransferUtility(targetClient.s3Client);

            var fileTransferUtilityRequest = new TransferUtilityUploadRequest
            {
                BucketName = targetClient.BucketName,
                FilePath = fileName,
                StorageClass = S3StorageClass.Standard,
                PartSize = 6291456, // 6 MB.
                Key = $"{TargetFolder}/{Path.GetFileName(fileName)}"
            };

            fileTransferUtility.Upload(fileTransferUtilityRequest);
            logger.Info($"{fileName} to {TargetFolder}/{Path.GetFileName(fileName)} uploaded.");
        }

        public void UploadFiles(string[] args)
        {
            string source = args[2];
            string targetFolder = args.Length > 3 ? args[3] + "/" : "";

            string[] files;
            files = Directory.GetFiles(Path.GetDirectoryName(source), Path.GetFileName(source));
            logger.Info($"Total objects to upload : {files.Length}.");
            for (int i = 0; i < files.Length; i++)
            {
                var fileName = files[i];
                UploadFileAsync(fileName, targetFolder);
            }
        }

        public void DownloadFiles(string prefix, string downloadPath)
        {
            var files = ListFiles(prefix);
            int i = 0;
            Task[] tasks = new Task[files.Count];
            logger.Info($"Total objects to download : {files.Count}.");
            foreach (var f in files)
            {
                var fileName = f.Key;
                tasks[i] = Task.Run(() => DownloadFileAsync(fileName, downloadPath));
                i++;
            }
            Task.WaitAll(tasks);
        }

        public List<S3Object> ListFiles(string prefix)
        {
            List<S3Object> bucketFiles = new List<S3Object>();
            ListObjectsV2Response response;

            ListObjectsV2Request request = new ListObjectsV2Request
            {
                BucketName = bucketName,
                MaxKeys = 500,
                Prefix = prefix
            };

            while (true)
            {
                response = client.ListObjectsV2Async(request).Result;
                bucketFiles.AddRange(response.S3Objects);

                if (response.IsTruncated)
                    request.ContinuationToken = response.NextContinuationToken;
                else
                    break;
            }
            logger.Info($"List Files Count: {bucketFiles.Count}");

            return bucketFiles;
        }
        public void UploadFileAsync(string fileName, string targetFolder)
        {
            var fileTransferUtility =
                new TransferUtility(client);

            var fileTransferUtilityRequest = new TransferUtilityUploadRequest
            {
                BucketName = bucketName,
                FilePath = fileName,
                StorageClass = S3StorageClass.Standard,
                PartSize = 6291456, // 6 MB.
                Key = $"{targetFolder}{Path.GetFileName(fileName)}"
            };

            fileTransferUtility.Upload(fileTransferUtilityRequest);
            logger.Info($"Object {fileName} uploaded.");
        }
        public void DownloadFileAsync(string fileName, string targetPath)
        {
            var fileTransferUtility =
               new TransferUtility(client);

            var fileTransferUtilityRequest = new TransferUtilityDownloadRequest
            {
                BucketName = bucketName,
                FilePath = Path.Combine(targetPath, Path.GetFileName(fileName)),
                Key = $"{fileName}"
            };

            fileTransferUtility.Download(fileTransferUtilityRequest);
            logger.Info($"Object {fileName} downloaded.");

        }
        public string DownloadFile(string fileName, string targetPath)
        {
            var targetFileName = Path.Combine(targetPath, fileName.Replace(":", ""));

            var fileTransferUtility =
                new TransferUtility(client);

            var fileTransferUtilityRequest = new TransferUtilityDownloadRequest
            {
                BucketName = bucketName,
                FilePath = targetFileName,
                Key = $"{fileName}"
            };

            fileTransferUtility.Download(fileTransferUtilityRequest);
            logger.Info($"Object {fileName} downloaded.");

            return targetFileName;
        }
        public async Task CopyingObjectAsync(string objectKey, string destObjectKey)
        {
            CopyObjectRequest request = new CopyObjectRequest
            {
                SourceBucket = bucketName,
                SourceKey = objectKey,
                DestinationBucket = bucketName,
                DestinationKey = destObjectKey
            };
            CopyObjectResponse response = await client.CopyObjectAsync(request);
            logger.Info($"Object {objectKey} was copid to {destObjectKey}.");
        }
    }
}
