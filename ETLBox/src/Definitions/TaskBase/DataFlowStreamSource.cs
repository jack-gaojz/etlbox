using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using ETLBox.Helper;

namespace ETLBox.DataFlow
{
    public abstract class DataFlowStreamSource<TOutput> : DataFlowExecutableSource<TOutput>, IDataFlowStreamSource<TOutput>
    {
        #region Public properties

        /// <summary>
        /// The Url of the webservice (e.g. https://test.com/foo) or the file name (relative or absolute).
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

        /// <summary>
        /// This func returns the next url that is used for reading data. It will be called until <see cref="HasNextUri"/> returns false.
        /// The incoming <see cref="StreamMetaData"/> holds information about the current progress and other meta data from the response, like unparsed
        /// json data that contains references to the next page of the response.
        /// This property can be used if you want to read multiple files or if you want to paginate through web responses.
        /// </summary>
        public Func<StreamMetaData, string> GetNextUri { get; set; }

        /// <summary>
        /// This func determines if another request is started to read additional data from the next uri.
        /// <see cref="StreamMetaData"/> has information about the current progress and other meta data from the response.
        /// </summary>
        public Func<StreamMetaData, bool> HasNextUri { get; set; }

        /// <summary>
        /// Specifies the resource type. By default requests are made with HttpClient.
        /// Specify ResourceType.File if you want to read from a json file.
        /// </summary>
        public ResourceType ResourceType { get; set; }

        /// <summary>
        /// The System.Net.Http.HttpClient uses for the request. Use this client if you want to
        /// add or change the http request data, e.g. you can add your authorization information here.
        /// </summary>
        public HttpClient HttpClient { get; set; } = new HttpClient();

        /// <summary>
        /// The System.Net.Http.HttpRequestMessage use for the request from the HttpClient. Add your request
        /// message here, e.g. your POST body.
        /// </summary>
        public HttpRequestMessage HttpRequestMessage { get; set; } = new HttpRequestMessage();

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
        }

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
            {
                var message = HttpRequestMessage.Clone();
                message.RequestUri =  new Uri(uri);
                var response = HttpClient.SendAsync(message, HttpCompletionOption.ResponseHeadersRead).Result;
                response.EnsureSuccessStatusCode();
                StreamReader = new StreamReader(response.Content.ReadAsStreamAsync().Result);
            }
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
