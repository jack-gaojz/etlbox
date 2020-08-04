using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;

namespace ETLBox.DataFlow
{
    public abstract class DataFlowStreamSource<TOutput> : DataFlowSource<TOutput>
    {
        #region Public properties

        /// <summary>
        /// The Url of the webservice (e.g. https://test.com/foo) or the file name (relative or absolute)
        /// </summary>
        public string Uri
        {
            get
            {
                return _uri;
            }
            set
            {
                _uri = value;
                GetNextUri = c => _uri;
                HasNextUri = c => false;
            }
        }
        private string _uri;

        public Func<StreamMetaData, string> GetNextUri { get; set; }
        public Func<StreamMetaData, bool> HasNextUri { get; set; }

        /// <summary>
        /// Specifies the resource type. By default requests are made with HttpClient.
        /// Specify ResourceType.File if you want to read from a json file.
        /// </summary>
        public ResourceType ResourceType { get; set; }

        public HttpClient HttpClient { get; set; } = new HttpClient();

        #endregion

        #region Internal properties

        protected string CurrentRequestUri { get; set; }
        protected StreamReader StreamReader { get; set; }
        protected StringBuilder UnparsedData { get; set; }

        #endregion

        #region Implement abstract methods

        protected override void OnExecutionDoSynchronousWork()
        {

        }

        protected override void OnExecutionDoAsyncWork()
        {
            NLogStartOnce();
            do
            {
                CurrentRequestUri = GetNextUri(CreateMetaDataObject);
                OpenStream(CurrentRequestUri);
                InitReader();
                WasStreamOpened = true;
                ReadAllRecords();
            } while (HasNextUri(CreateMetaDataObject));
            Buffer.Complete();
        }

        protected override void InitComponent() { }

        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
            CloseStreamsIfOpen();
        }

        protected override void CleanUpOnFaulted(Exception e) {
            CloseStreamsIfOpen();
        }

        #endregion

        #region Implementation

        private bool WasStreamOpened;

        private void CloseStreamsIfOpen()
        {
            if (WasStreamOpened)
            {
                CloseReader();
                CloseStream();
            }
        }

        private StreamMetaData CreateMetaDataObject =>
                new StreamMetaData()
                {
                    ProgressCount = ProgressCount,
                    UnparsedData = UnparsedData?.ToString()
                };


        private void OpenStream(string uri)
        {
            if (ResourceType == ResourceType.File)
                StreamReader = new StreamReader(uri);
            else
                StreamReader = new StreamReader(HttpClient.GetStreamAsync(new Uri(uri)).Result);
        }

        private void CloseStream()
        {
            HttpClient?.Dispose();
            StreamReader?.Dispose();
        }

        protected abstract void InitReader();
        protected abstract void ReadAllRecords();
        protected abstract void CloseReader();

        #endregion
    }
}
