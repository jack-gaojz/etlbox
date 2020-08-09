using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public abstract class DataFlowStreamDestination<TInput> : DataFlowDestination<TInput>
    {
        #region Public properties
        /* Public properties */
        /// <summary>
        /// The Url of the webservice (e.g. https://test.com/foo) or the file name (relative or absolute)
        /// </summary>
        public string Uri { get; set; }

        /// <summary>
        /// Specifies the resourc type. ResourceType.
        /// Specify ResourceType.File if you want to write into a file.
        /// </summary>
        public ResourceType ResourceType { get; set; }

        public HttpClient HttpClient { get; set; } = new HttpClient();

        #endregion

        #region Implement abstract methods

        internal override void InitBufferObjects()
        {
            TargetAction = new ActionBlock<TInput>(WriteData, new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = 1,
                BoundedCapacity = MaxBufferSize,
            });
        }

        protected override void CleanUpOnSuccess()
        {
            CloseStream();
            StreamWriter?.Close();
            NLogFinishOnce();
        }

        protected override void CleanUpOnFaulted(Exception e)
        {
            CloseStream();
            StreamWriter?.Close();
        }

        #endregion

        #region Implementation template

        protected StreamWriter StreamWriter { get; set; }

        protected void WriteData(TInput data)
        {
            NLogStartOnce();
            if (StreamWriter == null)
            {
                CreateStreamWriterByResourceType();
                InitStream();
            }
            WriteIntoStream(data);
        }

        private void CreateStreamWriterByResourceType()
        {
            if (ResourceType == ResourceType.File)
                StreamWriter = new StreamWriter(Uri);
            else
                StreamWriter = new StreamWriter(HttpClient.GetStreamAsync(new Uri(Uri)).Result);
        }

        protected abstract void InitStream();
        protected abstract void WriteIntoStream(TInput data);
        protected abstract void CloseStream();

        #endregion
    }
}
