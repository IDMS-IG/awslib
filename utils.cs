using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;

namespace awslib
{
    public class Utilities
    {
        public void Decompress(string fileName, string targetFile)
        {
            var fileToDecompress = new FileInfo(fileName);
            string currentFileName = fileToDecompress.FullName;

            using FileStream originalFileStream = fileToDecompress.OpenRead();
            using FileStream decompressedFileStream = File.Create(targetFile);
            using GZipStream decompressionStream = new GZipStream(originalFileStream, CompressionMode.Decompress);

            decompressionStream.CopyTo(decompressedFileStream);
        }
    }
}
