using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;

namespace ETLBox.DataFlow
{
    public class StreamMetaData
    {
        public int ProgressCount { get; set; }
        public string UnparsedData { get; set; }
    }
}
