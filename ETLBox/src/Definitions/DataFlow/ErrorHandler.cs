using Newtonsoft.Json;
using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    /// <summary>
    /// Works as a source component for any errors. Another component
    /// can use this source to redirect errors into the error data flow.
    /// </summary>
    public class ErrorHandler
    {
        /// <summary>
        /// SourceBlock from the underlying TPL.Dataflow which is used as output buffer for the error messages.
        /// </summary>
        public ISourceBlock<ETLBoxError> ErrorSourceBlock => ErrorBuffer;
        internal BufferBlock<ETLBoxError> ErrorBuffer { get; set; }


        public void LinkErrorTo(IDataFlowDestination<ETLBoxError> target, Task completion)
        {
            ErrorBuffer = new BufferBlock<ETLBoxError>();
            ErrorSourceBlock.LinkTo(target.TargetBlock, new DataflowLinkOptions());
            //target.AddPredecessorCompletion(ErrorSourceBlock.Completion);
            completion.ContinueWith(t => ErrorBuffer.Complete());
        }

        public void Send(Exception e, string jsonRow)
        {
            ErrorBuffer.SendAsync(new ETLBoxError()
            {
                ExceptionType = e.GetType().ToString(),
                ErrorText = e.Message,
                ReportTime = DateTime.Now,
                RecordAsJson = jsonRow
            }).Wait();
        }

        public static string ConvertErrorData<T>(T row)
        {
            try
            {
                return JsonConvert.SerializeObject(row, new JsonSerializerSettings());
            }
            catch (Exception e)
            {
                return e.Message;
            }
        }
    }
}
