using Newtonsoft.Json;
using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public class ErrorSource : DataFlowSource<ETLBoxError>
    {
        //public ISourceBlock<ETLBoxError> SourceBlock => ErrorBuffer;
        //internal BufferBlock<ETLBoxError> ErrorBuffer { get; set; }
        //public bool HasErrorBuffer => ErrorBuffer != null;


        public ErrorSource()
        {

        }

        //protected override void InitBufferObjects()
        //{
        //    //ErrorBuffer = new BufferBlock<ETLBoxError>();
        //}


        protected override void LinkBuffers(DataFlowTask successor, Tuple<object, object> predicate)
        {
            var s = successor as IDataFlowLinkTarget<ETLBoxError>;
            this.SourceBlock.LinkTo<ETLBoxError>(s.TargetBlock);
        }


        //public void LinkErrorTo(IDataFlowLinkTarget<ETLBoxError> target, Task completion)
        //{
        ////    ErrorBuffer = new BufferBlock<ETLBoxError>();
        ////    ErrorSourceBlock.LinkTo(target.TargetBlock, new DataflowLinkOptions());
        ////    //target.AddPredecessorCompletion(ErrorSourceBlock.Completion);
        ////    completion.ContinueWith(t => ErrorBuffer.Complete());
        //}

        public void Send(Exception e, string jsonRow)
        {
            Buffer.SendAsync(new ETLBoxError()
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

        public override void Execute()
        {
            //Task t = new Task(ExecuteAsyncPart, TaskCreationOptions.LongRunning);
            //Completion = t;
            ExecuteSyncPart();
            Completion.RunSynchronously();
            //return Completion;
        }

        public override void ExecuteAsyncPart()
        {

        }

        public override void ExecuteSyncPart()
        {
            InitBufferRecursively();
            LinkBuffersRecursively();
            SetCompletionTaskRecursively();
        }
    }
}
